#!/usr/bin/env python3
"""Generate the MobilitySpark UDF surface from the MEOS-API catalog.

North Star (meos-api-codegen-regularity): the Spark UDF surface is GENERATED
from the single MEOS-API source of truth, never hand-written. This drives the
engine over the WHOLE catalog (every @sqlfn name + every contract operator bare
name from portableAliases) — no hardcoded scope, no skip-hacks. Functions whose
types are genuinely internal (Datum/SkipList/function-pointers/out-param arrays)
are the ONLY exclusions, and they are reported, never silently skipped.

Two emission modes:
  - SINGLE: a name with one backing -> a 1:1 UDF.
  - DISPATCH: a name/operator with N typed backings (Spark cannot overload by
    name) -> ONE UDF that classifies each String arg by its MEOS type and routes
    to the catalog-determined backing.

Wire format (matches the portable suite): temporals travel as hex-WKB; spans,
sets, boxes, cbuffer/npoint/pose and geometries travel as TEXT; scalars are typed
Spark columns (Integer/Double/Boolean/Long/Timestamp).

Usage: python3 tools/codegen_spark_udfs.py [--catalog PATH] [--out DIR] [--report]
"""
import argparse, json, os, sys, collections, glob, re


def norm(c):
    return c.replace("const ", "").replace("struct ", "").strip()


# ── Pointer-typed args: canonical base -> (GeneratedFunctions parser, KIND) ──
# parser takes a Java String, returns a jnr Pointer (freed after the call).
PARSE = {
    "Temporal":     ("GeneratedFunctions.temporal_from_hexwkb(%s)", "K_TEMPORAL"),
    "TInstant":     ("GeneratedFunctions.temporal_from_hexwkb(%s)", "K_TEMPORAL"),
    "TSequence":    ("GeneratedFunctions.temporal_from_hexwkb(%s)", "K_TEMPORAL"),
    "TSequenceSet": ("GeneratedFunctions.temporal_from_hexwkb(%s)", "K_TEMPORAL"),
    "GSERIALIZED":  ("GeneratedFunctions.geo_from_text(%s, 0)",     "K_GEO"),
    # span/set/spanset parse from hex-WKB: the bare *_in(text) needs a type-OID 2nd
    # arg (intspan vs floatspan can't be told from "[1,5)"); hex-WKB embeds the type.
    "Span":         ("GeneratedFunctions.span_from_hexwkb(%s)",     "K_SPAN"),
    "SpanSet":      ("GeneratedFunctions.spanset_from_hexwkb(%s)",  "K_SPANSET"),
    "Set":          ("GeneratedFunctions.set_from_hexwkb(%s)",      "K_SET"),
    "STBox":        ("GeneratedFunctions.stbox_in(%s)",             "K_STBOX"),
    "TBox":         ("GeneratedFunctions.tbox_in(%s)",              "K_TBOX"),
    "Cbuffer":      ("GeneratedFunctions.cbuffer_in(%s)",           "K_CBUFFER"),
    "Npoint":       ("GeneratedFunctions.npoint_in(%s)",            "K_NPOINT"),
    "Nsegment":     ("GeneratedFunctions.nsegment_in(%s)",          "K_NSEGMENT"),
    "Pose":         ("GeneratedFunctions.pose_in(%s)",              "K_POSE"),
    "Jsonb":        ("GeneratedFunctions.jsonb_in(%s)",             "K_JSONB"),
}
# ── Pointer-typed returns: canonical base -> GeneratedFunctions serializer ──
SERIAL = {
    "Temporal":     "GeneratedFunctions.temporal_as_hexwkb(%s, (byte) 4)",
    "TInstant":     "GeneratedFunctions.temporal_as_hexwkb(%s, (byte) 4)",
    "TSequence":    "GeneratedFunctions.temporal_as_hexwkb(%s, (byte) 4)",
    "TSequenceSet": "GeneratedFunctions.temporal_as_hexwkb(%s, (byte) 4)",
    "GSERIALIZED":  "GeneratedFunctions.geo_as_text(%s, 15)",
    "Span":         "GeneratedFunctions.span_as_hexwkb(%s, (byte) 4)",
    "SpanSet":      "GeneratedFunctions.spanset_as_hexwkb(%s, (byte) 4)",
    "Set":          "GeneratedFunctions.set_as_hexwkb(%s, (byte) 4)",
    "STBox":        "GeneratedFunctions.stbox_out(%s, 15)",
    "TBox":         "GeneratedFunctions.tbox_out(%s, 15)",
    "Cbuffer":      "GeneratedFunctions.cbuffer_out(%s, 15)",
    "Npoint":       "GeneratedFunctions.npoint_out(%s, 15)",
    "Nsegment":     "GeneratedFunctions.nsegment_out(%s, 15)",
    "Pose":         "GeneratedFunctions.pose_out(%s, 15)",
    "Jsonb":        "GeneratedFunctions.jsonb_out(%s)",
}
# ── Scalar args: canonical -> (Spark DataType, Java boxed type, "parse expr") ──
SCALAR_ARG = {
    "int":         ("IntegerType", "Integer", "%s"),
    "bool":        ("BooleanType", "Boolean", "%s"),
    "double":      ("DoubleType",  "Double",  "%s"),
    "int64_t":     ("LongType",    "Long",    "%s"),
    "uint64_t":    ("LongType",    "Long",    "%s"),   # 64-bit (H3Index/hash) <-> jnr long
    "DateADT":     ("IntegerType", "Integer", "%s"),   # JMEOS maps DateADT -> int
    "unsigned char": ("ByteType",  "Byte",    "%s"),   # the WKB `variant` flag <-> jnr byte
}
# ── Scalar returns: canonical -> (Spark DataType, Java box, "serialize expr") ──
SCALAR_RET = {
    "bool":     ("BooleanType", "%s"),
    "double":   ("DoubleType",  "%s"),
    "int64_t":  ("LongType",    "%s"),
    "uint64_t": ("LongType",    "%s"),
    "DateADT":  ("IntegerType", "%s"),
    "char *":   ("StringType",  "%s"),     # cstring already a Java String via jnr
    "text *":   ("StringType",  "GeneratedFunctions.text_out(%s)"),
}
# operators whose int (1/0/-1) result is a tri-state predicate -> BooleanType ==1
PRED_OPS = {"?=", "?<>", "?<", "?<=", "?>", "?>=",
            "%=", "%<>", "%<", "%<=", "%>", "%>=",
            "#=", "#<>", "#<", "#<=", "#>", "#>="}
# genuinely-internal / non-user-facing base types -> legitimately OUT OF SCOPE.
INTERNAL = {"Datum", "SkipList", "GBOX", "BOX3D", "void", "meosType", "MeosType",
            "uint8_t", "LWGEOM", "GEOSGeometry", "RTree", "interpType", "json_object",
            "size_t", "Match", "TimeSplit", "FloatSplit", "FloatTimeSplit",
            "IntSplit", "IntTimeSplit", "MvtGeom", "unsigned int",
            "uint32", "Interval", "text", "char"}
# (Interval/text/char START as harder marshalling — deferred to a follow-up pass;
#  counted as not-yet-emitted, NOT as a permanent exclusion. DateADT (->int) and
#  TimestampTz (->OffsetDateTime) are now handled.)


# JMEOS actual signatures (name -> (javaRet, nArgs)), parsed from the jar in main().
# The jar is the ground truth: it catches catalog/typerecover disagreements (uint64
# collapsed to int, collapsed-jsonb int*, opaque PJ pointers) before they miscompile.
JSIG = {}
JPRIM = {"long": "LongType", "int": "IntegerType", "double": "DoubleType",
         "boolean": "BooleanType", "float": "DoubleType"}


def base(canon):
    t = norm(canon)
    if "(*" in t or "()" in t or t.endswith("**"):
        return "__INTERNAL__"
    b = t.replace("*", "").strip()
    return b


# pointer-to-primitive out-params: JMEOS drops the param, allocs a buffer, and
# returns a Pointer to it (the bool/void return is discarded). canonical -> deref.
OUTPRIM = {
    "double *":   ("DoubleType",  "%s.getDouble(0)"),
    "int *":      ("IntegerType", "%s.getInt(0)"),
    "int64_t *":  ("LongType",    "%s.getLongLong(0)"),
    "bool *":     ("BooleanType", "(%s.getByte(0) != 0)"),
}


def arg_kind(canon):
    """('ptr', parse, KIND) | ('scalar', DataType, Box, expr) | ('ts',) | None."""
    nc = norm(canon)
    b = base(canon)
    if b == "TimestampTz" and nc == "TimestampTz":
        return ("ts",)
    if b in PARSE:
        return ("ptr",) + PARSE[b]
    # scalar ONLY when not a pointer: int* / DateADT* are arrays/out-params, not ints.
    if b in SCALAR_ARG and "*" not in nc:
        return ("scalar",) + SCALAR_ARG[b]
    # C string: a single `const char *` is a Java String (jnr marshals it), passed
    # straight to JMEOS — this is how the *_in(text) parsers take their WKT literal.
    # (char** / multi-pointer stay unmapped; the jar arity cross-check guards mismatches.)
    if b == "char" and nc.count("*") == 1:
        return ("scalar", "StringType", "String", "%s")
    return None


def classify(f):
    """Split params into (in_params, out): a single trailing NON-const writable
    pointer out-param on a bool/void function is dropped, and JMEOS returns a
    Pointer to it. out is (DataType, serialize/deref-expr) or None. The out-param
    may be a primitive (deref) or a struct in SERIAL (serialize). const pointers
    are inputs, never out-params."""
    params = f["params"]
    # A trailing non-const `size_t *` is the canonical buffer-length out-param of the
    # *_as_wkb / *_as_hexwkb / *_as_ewkb family: JMEOS swallows it and returns the
    # buffer (char* / byte[]) directly, so it is never a Java-visible parameter.
    if params and "const" not in params[-1]["canonical"] and norm(params[-1]["canonical"]) == "size_t *":
        params = params[:-1]
    rt = norm(f["returnType"]["canonical"])
    if rt in ("bool", "void") and params:
        lastc = params[-1]["canonical"]
        lastn = norm(lastc)
        writable = "const" not in lastc and lastn.endswith("*")
        # no other writable out-param may precede it (single-out-param only)
        others = [p for p in params[:-1]
                  if "const" not in p["canonical"]
                  and (norm(p["canonical"]) in OUTPRIM or base(p["canonical"]) in SERIAL)
                  and norm(p["canonical"]).endswith("*")]
        if writable and not others:
            if lastn in OUTPRIM:
                return params[:-1], OUTPRIM[lastn]
            if base(lastc) in SERIAL:
                return params[:-1], ("StringType", SERIAL[base(lastc)])
    return params, None


def ret_emit(canon, sqlop):
    """('ptr', DataType, serialize) | ('scalar', DataType, serialize) | None."""
    t = norm(canon)
    b = base(t)
    if b in SERIAL and t.endswith("*"):
        return ("ptr", "StringType", SERIAL[b])
    if b == "TimestampTz":            # JMEOS maps TimestampTz -> OffsetDateTime
        return ("dt", "StringType", "UdfMarshal.tsOut(%s)")
    if t == "int" or b == "int":
        if sqlop in PRED_OPS:
            return ("scalar", "BooleanType", "%s == 1")
        return ("scalar", "IntegerType", "%s")
    if t in SCALAR_RET:
        return ("scalar",) + SCALAR_RET[t]
    if b in SCALAR_RET:
        return ("scalar",) + SCALAR_RET[b]
    return None


def supported(f):
    """Reason string if NOT emittable, else None."""
    # meos_internal_* doxygen groups are MEOS-internal, not user-facing — excluded.
    if (f.get("group") or "").startswith("meos_internal"):
        return "internal"
    in_params, out = classify(f)
    if out is None:
        r = ret_emit(f["returnType"]["canonical"], f.get("sqlop"))
        if r is None:
            b = base(f["returnType"]["canonical"])
            return ("internal" if b in INTERNAL or b == "__INTERNAL__" else "ret:"+norm(f["returnType"]["canonical"]))
    for p in in_params:
        if arg_kind(p["canonical"]) is None:
            b = base(p["canonical"])
            return ("internal" if b in INTERNAL or b == "__INTERNAL__" else "arg:"+norm(p["canonical"]))
    # cross-check against the jar: my call arity must match JMEOS's. A mismatch means
    # the catalog type disagrees with JMEOS (collapsed-jsonb int*, opaque PJ pointer,
    # multi-out-param) — exclude rather than emit a call that won't bind.
    if JSIG and f["name"] in JSIG and JSIG[f["name"]][1] != len(in_params):
        return "arity:jmeos-mismatch"
    # return-kind cross-check: if JMEOS returns a Pointer but my catalog-inferred
    # return is a scalar (or vice versa), the catalog collapsed a type (LWGEOM/Jsonb
    # -> int) — exclude rather than miscompile.
    if out is None and JSIG and f["name"] in JSIG:
        r = ret_emit(f["returnType"]["canonical"], f.get("sqlop"))
        if (JSIG[f["name"]][0] == "jnr.ffi.Pointer") != (r[0] == "ptr"):
            return "ret:jmeos-kind-mismatch"
    return None


_JAVA_KEYWORDS = {
    "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char",
    "class", "const", "continue", "default", "do", "double", "else", "enum",
    "extends", "final", "finally", "float", "for", "goto", "if", "implements",
    "import", "instanceof", "int", "interface", "long", "native", "new", "package",
    "private", "protected", "public", "return", "short", "static", "strictfp",
    "super", "switch", "synchronized", "this", "throw", "throws", "transient",
    "try", "void", "volatile", "while", "true", "false", "null", "var",
}


def _javaid(n):
    """A MEOS param name may collide with a Java keyword (e.g. synchronized)."""
    return n + "_" if n in _JAVA_KEYWORDS else n


def class_for(group):
    """Java class name for a doxygen @ingroup group. The group string is kept
    literally (meos_ prefix stripped) so the same function lands in the same-named
    class across tools. Functions with no @ingroup go to GeneratedUdfs_ungrouped."""
    g = group or "ungrouped"
    if g.startswith("meos_"):
        g = g[len("meos_"):]
    return "GeneratedUdfs_" + g


def emit_single(name, f):
    params, out = classify(f)
    if out is None:
        r = ret_emit(f["returnType"]["canonical"], f.get("sqlop"))
        ret_dt, ret_ser, ret_ptr, ret_out = r[1], r[2], (r[0] == "ptr"), False
        # trust the jar for plain numeric returns: the catalog collapses uint64->int,
        # but JMEOS exposes the true width (e.g. *_hash_extended returns long).
        if r[0] == "scalar" and ret_ser == "%s" and name in JSIG and JSIG[name][0] in JPRIM:
            ret_dt = JPRIM[JSIG[name][0]]
    else:
        ret_dt, ret_ser, ret_ptr, ret_out = out[0], out[1], False, True
    box = {"IntegerType": "Integer", "DoubleType": "Double", "BooleanType": "Boolean",
           "LongType": "Long", "StringType": "String"}[ret_dt]
    argnames = [_javaid(p["name"] or ("a%d" % i)) for i, p in enumerate(params)]
    argboxes, kinds = [], []
    for p in params:
        k = arg_kind(p["canonical"])
        kinds.append(k)
        argboxes.append({"ptr": "String", "ts": "Object", "scalar": k[2] if k[0] == "scalar" else "String"}[k[0]])
    iface = "UDF%d<%s, %s>" % (len(params), ", ".join(argboxes), box) if params else "UDF0<%s>" % box
    # Emit as an inline lambda inside a register() call (NOT a static field): 2349
    # static-field initializers overrun the 64 KB <clinit> bytecode limit. Inline
    # lambdas compile to separate synthetic methods, keeping the chunk method small.
    L = [f'        spark.udf().register("{name}", ({iface}) (' + ", ".join(argnames) + ") -> {"
         if params else f'        spark.udf().register("{name}", ({iface}) () -> {{']
    if argnames:
        L.append("        if (" + " || ".join(f"{a} == null" for a in argnames) + ") return null;")
    L.append("        MeosThread.ensureReady();")
    callargs, frees = [], []
    for a, p, k in zip(argnames, params, kinds):
        if k[0] == "ptr":
            parse = k[1]
            L.append(f"        jnr.ffi.Pointer p_{a} = {parse % a};")
            L.append(f"        if (p_{a} == null) return null;")
            callargs.append(f"p_{a}"); frees.append(f"p_{a}")
        elif k[0] == "ts":
            L.append(f"        java.time.OffsetDateTime dt_{a} = UdfMarshal.tsOdt({a});")
            callargs.append(f"dt_{a}")
        else:
            callargs.append(a)
    call = f"GeneratedFunctions.{f['name']}(" + ", ".join(callargs) + ")"
    L.append("        try {")
    if ret_out:
        # JMEOS returns a jnr-allocated buffer (GC-managed) — deref, never free it.
        L.append(f"            jnr.ffi.Pointer _r = {call};")
        L.append("            if (_r == null) return null;")
        L.append(f"            return {ret_ser % '_r'};")
    elif ret_ptr:
        L.append(f"            jnr.ffi.Pointer _r = {call};")
        L.append("            if (_r == null) return null;")
        L.append(f"            try {{ return {ret_ser % '_r'}; }} finally {{ MeosMemory.free(_r); }}")
    else:
        L.append(f"            return {ret_ser % call};")
    L.append("        } finally {")
    for fr in frees:
        L.append(f"            MeosMemory.free({fr});")
    L.append("        }")
    L.append(f"        }}, DataTypes.{ret_dt});")
    return "\n".join(L)


_RETBOX = {"IntegerType": "Integer", "DoubleType": "Double", "BooleanType": "Boolean",
           "LongType": "Long", "StringType": "String", "ByteType": "Byte"}


def _sig(f):
    """The Spark-marshalled SIGNATURE of an emittable function: (arg-slot tuple,
    return-shape). Two functions with the SAME _sig present an identical Java UDF
    interface, so several C overloads of one @sqlfn name that share a _sig can be
    dispatched by ONE Spark UDF. Slot = the Java box for a scalar arg, "P" for any
    pointer arg (always a String), "T" for a timestamp. Returns None if unemittable."""
    params, out = classify(f)
    slots = []
    for p in params:
        k = arg_kind(p["canonical"])
        if k is None:
            return None
        slots.append("P" if k[0] == "ptr" else ("T" if k[0] == "ts" else k[2]))
    if out is not None:
        ret = ("out", out[0], out[1])
    else:
        r = ret_emit(f["returnType"]["canonical"], f.get("sqlop"))
        if r is None:
            return None
        ret = (("ptr" if r[0] == "ptr" else "scalar"), r[1], r[2])
    return (tuple(slots), ret)


def _safe_dispatch(f):
    """A function is safely arg-kind-dispatchable only if every pointer arg parses via a
    hex-WKB or WKT parser — those are mutually discriminating and reject foreign input by
    returning null. The text *_in parsers (stbox_in / tbox_in / cbuffer_in / npoint_in /
    pose_in / jsonb_in) are NOT: fed a hex-WKB or WKT string they may mis-parse, so an
    overload using one cannot be told apart at runtime and must not enter a dispatcher."""
    for p in classify(f)[0]:
        k = arg_kind(p["canonical"])
        if k and k[0] == "ptr" and "from_hexwkb" not in k[1] and "geo_from_text" not in k[1]:
            return False
    return True


def _parsetuple(f):
    """The arg KINDS that a runtime parse can actually DISTINGUISH: K_TEMPORAL / K_GEO /
    K_SPAN ... for pointer args, a constant marker for scalars/timestamps. Two overloads
    with the SAME _parsetuple differ only by temporal SUBTYPE (tdistance_tgeo_tgeo vs
    tdistance_tnpoint_tnpoint — both (Temporal,Temporal)) and so CANNOT be told apart by
    parsing the hex-WKB; only one of them may go into a parse-based dispatcher."""
    out = []
    for p in classify(f)[0]:
        k = arg_kind(p["canonical"])
        out.append(k[2] if k[0] == "ptr" else ("TS" if k[0] == "ts" else "S"))
    return tuple(out)


# Among parse-indistinguishable overloads, prefer the geometry family the canonical
# BerlinMOD suite (and most users) call: tgeo / geo. Lower rank = more preferred.
_FAMTOK = ["_tgeo_", "tgeo_", "_geo_", "_geo", "geo_", "temporal_", "_tspatial_", "tpoint", "tnumber"]
def _famrank(f):
    n = f["name"]
    return next((i for i, t in enumerate(_FAMTOK) if t in n), len(_FAMTOK))


def emit_dispatch(name, cands):
    """Emit ONE Spark UDF for an @sqlfn name backed by SEVERAL C overloads that share
    a _sig (e.g. eIntersects <- eintersects_tgeo_tgeo / _tgeo_geo / _geo_tgeo). Spark
    cannot overload a UDF name, so the single lambda parses each pointer arg with each
    candidate's parsers in turn: the FIRST candidate whose every pointer arg parses is
    the matching overload (the hex-WKB / WKT / span parsers are mutually discriminating,
    so exactly one matches). Parse-all-then-check keeps it leak-free on every path."""
    rep = cands[0]
    slots, ret = _sig(rep)
    n = len(slots)
    argnames = ["a%d" % i for i in range(n)]
    argboxes = [("String" if s == "P" else ("Object" if s == "T" else s)) for s in slots]
    ret_kind, ret_dt, ret_ser = ret
    box = _RETBOX[ret_dt]
    iface = "UDF%d<%s, %s>" % (n, ", ".join(argboxes), box) if n else "UDF0<%s>" % box
    L = ['        spark.udf().register("%s", (%s) (%s) -> {' % (name, iface, ", ".join(argnames))]
    if argnames:
        L.append("        if (" + " || ".join("%s == null" % a for a in argnames) + ") return null;")
    L.append("        MeosThread.ensureReady();")
    # order GEO/WKT-parsing candidates LAST so the strict hex parsers get first refusal
    def geocount(f):
        return sum(1 for p in classify(f)[0] if base(p["canonical"]) == "GSERIALIZED")
    for f in sorted(cands, key=geocount):
        cps = classify(f)[0]
        ks = [arg_kind(p["canonical"]) for p in cps]
        callargs, ptrs = [], []
        L.append("        {")
        for a, k in zip(argnames, ks):
            if k[0] == "ptr":
                pv = "P_%s" % a
                parse = k[1] % a
                # a hex-WKB parser CRASHES on non-hex input — gate it on isHex so a WKT
                # geometry literal falls through to the geo_from_text candidate instead.
                if "from_hexwkb" in k[1]:
                    parse = "UdfMarshal.isHex(%s) ? %s : null" % (a, parse)
                L.append("          jnr.ffi.Pointer %s = %s;" % (pv, parse))
                ptrs.append(pv); callargs.append(pv)
            elif k[0] == "ts":
                L.append("          java.time.OffsetDateTime D_%s = UdfMarshal.tsOdt(%s);" % (a, a))
                callargs.append("D_%s" % a)
            else:
                callargs.append(a)
        cond = " && ".join("%s != null" % p for p in ptrs) if ptrs else "true"
        free = " ".join("MeosMemory.free(%s);" % p for p in ptrs)
        call = "GeneratedFunctions.%s(%s)" % (f["name"], ", ".join(callargs))
        L.append("          if (%s) {" % cond)
        if ret_kind == "out":
            L.append("            jnr.ffi.Pointer _r = %s;" % call)
            L.append("            try { return _r == null ? null : %s; } finally { %s }" % (ret_ser % "_r", free))
        elif ret_kind == "ptr":
            L.append("            jnr.ffi.Pointer _r = %s;" % call)
            L.append("            try { return _r == null ? null : %s; } finally { MeosMemory.free(_r); %s }" % (ret_ser % "_r", free))
        else:
            L.append("            try { return %s; } finally { %s }" % (ret_ser % call, free))
        L.append("          }")
        for p in ptrs:
            L.append("          if (%s != null) MeosMemory.free(%s);" % (p, p))
        L.append("        }")
    L.append("        return null;")
    L.append("        }, DataTypes.%s);" % ret_dt)
    return "\n".join(L)


GEN_NOTE = "// GENERATED by tools/codegen_spark_udfs.py from the MEOS-API catalog. DO NOT EDIT.\n"
IMPORTS = """\
package org.mobilitydb.spark.generated;

import functions.GeneratedFunctions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
"""

# Shared marshalling helpers live in their own class: the 2300+ UDFs are partitioned
# across many Part classes (a single class overruns the 64 KB method / constant-pool
# limits — exactly why JMEOS splits GeneratedFunctions into MeosLibraryPartA/PartB).
MARSHAL = GEN_NOTE + """\
package org.mobilitydb.spark.generated;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;
import jnr.ffi.Pointer;
import functions.GeneratedFunctions;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

final class UdfMarshal {
    private UdfMarshal() {}
    private static final DateTimeFormatter PG_TZ = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXX");
    // JMEOS marshals TimestampTz as java.time.OffsetDateTime.
    static java.time.OffsetDateTime tsOdt(Object a) {
        if (a instanceof java.sql.Timestamp)
            return ((java.sql.Timestamp) a).toInstant().atOffset(ZoneOffset.UTC);
        if (a instanceof java.time.OffsetDateTime) return (java.time.OffsetDateTime) a;
        if (a instanceof java.time.Instant) return ((java.time.Instant) a).atOffset(ZoneOffset.UTC);
        return java.time.OffsetDateTime.parse(a.toString().trim().replace(' ', 'T'));
    }
    static String tsOut(java.time.OffsetDateTime t) {
        return t == null ? null : t.format(PG_TZ);
    }

    // A hex-WKB string is an even-length run of hex digits. The overload dispatchers
    // must check this BEFORE handing a String to a *_from_hexwkb parser: MEOS's hex
    // decoder crashes (not returns null) on non-hex bytes, so trying a temporal parser
    // on a WKT geometry literal would segfault. WKT fails isHex (it has letters like
    // 'L','I','N','(' that are not hex digits), so the dispatch falls through to the
    // geo_from_text branch instead.
    static boolean isHex(String s) {
        int n = s.length();
        if (n == 0 || (n & 1) != 0) return false;
        for (int i = 0; i < n; i++)
            if (Character.digit(s.charAt(i), 16) < 0) return false;
        return true;
    }

    // True iff a hex-WKB temporal pointer is a tnumber (tint/tfloat): only those have
    // a TBox value extent. Used SOLELY to select which existing backing to delegate to
    // for the axis-ambiguous space-X operators — never to compute a result.
    private static boolean isTnumber(Pointer p) {
        Pointer box = GeneratedFunctions.tnumber_to_tbox(p);
        if (box == null) return false;
        MeosMemory.free(box);
        return true;
    }

    // Space-X (<<, >>, &<, &>): the value-axis (tnumber) and the X-axis (tspatial) are
    // distinct C operators. Parse both args once, then dispatch to the tnumber backing
    // for a tnumber left arg, else the tspatial backing. Both delegates are the
    // operator's own existing MEOS symbols — no operator logic here.
    static Boolean axisBool(String s1, String s2,
            BiFunction<Pointer, Pointer, Boolean> tnumber,
            BiFunction<Pointer, Pointer, Boolean> tspatial) {
        if (s1 == null || s2 == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
        if (p1 == null) return null;
        Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
        if (p2 == null) { MeosMemory.free(p1); return null; }
        try {
            return isTnumber(p1) ? tnumber.apply(p1, p2) : tspatial.apply(p1, p2);
        } finally {
            MeosMemory.free(p1);
            MeosMemory.free(p2);
        }
    }
}
"""


def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument("--catalog", default=os.path.join(here, "..", "..", "MEOS-API", "output", "meos-idl.json"))
    # Default into the maven build dir (NOT a source root): build-time generation
    # owns target/; a bare run must never pollute src/ (which would double-compile).
    ap.add_argument("--out", default=os.path.join(
        here, "..", "target", "generated-sources", "spark",
        "org", "mobilitydb", "spark", "generated"))
    ap.add_argument("--jar", default=os.path.join(here, "..", "libs", "JMEOS-1.4.jar"),
                    help="JMEOS jar; only functions JMEOS actually exposes are emitted "
                         "(catalog is a superset incl. internal _addmat/above8D/GEOS macros)")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    cat = json.load(open(args.catalog))
    fns = cat["functions"]

    # The catalog is the raw extern parse of the MobilityDB headers; JMEOS curates
    # its public surface (e.g. drops matrix internals _addmat/_choldc1, RTree node
    # predicates above8D/adjacent2D, GEOS conv macros MEOS_GEOS2POSTGIS). Emit ONLY
    # what JMEOS exposes, so a generated UDF can never call an absent jar symbol.
    jar_syms = None
    if args.jar and os.path.exists(args.jar):
        import subprocess, re
        jv = subprocess.run(["javap", "-p", "-cp", args.jar, "functions.GeneratedFunctions"],
                            capture_output=True, text=True).stdout
        jar_syms = set(re.findall(r"\b([a-z][A-Za-z0-9_]+)\(", jv))
        # JSIG: name -> (javaReturnType, nArgs) — the jar's actual signatures.
        for m in re.finditer(r"public static (\S+) ([a-z][A-Za-z0-9_]+)\(([^)]*)\)", jv):
            jret, jname, jargs = m.group(1), m.group(2), m.group(3).strip()
            JSIG[jname] = (jret if "." in jret else jret.lower(),
                           0 if not jargs else len(jargs.split(",")))

    # GOAL: reach the WHOLE JMEOS surface. Every MEOS C function (unique by its C
    # name) becomes a 1:1 UDF named by that C symbol — that is how the ~2254
    # functions with no @sqlfn are reached. The portable dialect (@sqlfn / operator
    # bare names from the contract) is layered on top in a later dispatch pass.
    grouped, names, cov = {}, set(), 0
    skips = collections.Counter()
    internal = total = not_in_jar = 0

    for f in fns:
        name = f["name"]
        if jar_syms is not None and name not in jar_syms:
            not_in_jar += 1          # internal helper JMEOS doesn't expose — skip
            continue
        total += 1
        if name in names:
            continue
        why = supported(f)
        if why:
            if why == "internal":
                internal += 1
            else:
                skips[why] += 1
            continue
        grouped.setdefault(class_for(f.get("group")), []).append(emit_single(name, f))
        names.add(name)
        cov += 1

    # ── DISPATCH PASS: portable bare names from the contract families ──
    # Spark cannot overload by name, but MEOS exposes SUPERCLASS entrypoints that
    # dispatch every concrete temporal type internally from the type-erased hex-WKB
    # string. So a portable bare name (everEq, tempLt, alwaysGe — RFC #920 / contract
    # #19) is emitted ONCE, wrapping its superclass C symbol. No Java type-inspection.
    by_name = {f["name"]: f for f in fns}
    fams = (cat.get("portableAliases") or {}).get("families", {})
    SUF = {"Eq": "eq", "Ne": "ne", "Lt": "lt", "Le": "le", "Gt": "gt", "Ge": "ge"}
    DISPATCH = [("everComparison", "ever_%s_temporal_temporal"),
                ("alwaysComparison", "always_%s_temporal_temporal"),
                ("temporalComparison", "temporal_%s")]
    ndisp = 0
    for fam, pat in DISPATCH:
        for e in fams.get(fam, []):
            bare = e["bareName"]
            suf = next((v for k, v in SUF.items() if bare.endswith(k)), None)
            backing = by_name.get(pat % suf) if suf else None
            if backing and supported(backing) is None:
                grouped.setdefault("GeneratedUdfs_portable_comparison", []).append(
                    emit_single(bare, backing))
                ndisp += 1
                cov += 1
    print("  dispatch bare names (comparison)  : %d" % ndisp, file=sys.stderr)

    # ── bare-name-IS-prefix families: topology / same / time / space Y,Z ──
    # Each contract bareName IS the MEOS C operator prefix, and the superclass
    # entrypoint *_temporal_temporal (time/topology) or *_tspatial_tspatial (spatial
    # position) dispatches every concrete subtype internally from the type-erased
    # hex-WKB — so one emit covers all six type families. Same emit machinery as the
    # comparison dispatch; only the backing-name pattern differs.
    PREFIX = [("topology",     "%s_temporal_temporal"),
              ("same",         "%s_temporal_temporal"),
              ("timePosition", "%s_temporal_temporal"),
              ("spaceY",       "%s_tspatial_tspatial"),
              ("spaceZ",       "%s_tspatial_tspatial")]
    nbare = 0
    for fam, pat in PREFIX:
        for e in fams.get(fam, []):
            bare = e["bareName"]
            backing = by_name.get(pat % bare)
            if backing and supported(backing) is None:
                grouped.setdefault("GeneratedUdfs_portable_operator", []).append(
                    emit_single(bare, backing))
                nbare += 1
                cov += 1
    print("  dispatch bare names (operator)    : %d" % nbare, file=sys.stderr)

    # (distance — tdistance / nearestApproachDistance — is NOT registered here: those
    # carry @sqlfn tags (tDistance / nearestApproachDistance) with several typed C
    # overloads, so the @sqlfn pass below emits them with full arg-kind dispatch — a
    # single tgeo_geo backing here would wrongly null a trip-vs-trip call.)

    # ── space X (<<, >>, &<, &>): the ONE axis-ambiguous family ──
    # left/right/overleft/overright resolve to DIFFERENT C symbols by argument class
    # — the tnumber value-axis (left_tnumber_tnumber) vs. the tspatial X-axis
    # (left_tspatial_tspatial). A thin runtime classifier (UdfMarshal.axisBool, which
    # inspects whether arg1 is a tnumber) SELECTS between the two existing backings;
    # it contains no operator logic, so equivalence-by-construction holds. This is the
    # only family that needs a per-arg type inspection, exactly as the contract notes.
    naxis = 0
    for e in fams.get("spaceX", []):
        bare = e["bareName"]
        tnum, tspat = by_name.get("%s_tnumber_tnumber" % bare), by_name.get("%s_tspatial_tspatial" % bare)
        if tnum and tspat and supported(tnum) is None and supported(tspat) is None:
            grouped.setdefault("GeneratedUdfs_portable_operator", []).append(
                '        spark.udf().register("%s", (UDF2<String, String, Boolean>) (s1, s2) ->\n'
                '            UdfMarshal.axisBool(s1, s2, GeneratedFunctions::%s, GeneratedFunctions::%s),\n'
                '            DataTypes.BooleanType);' % (bare, tnum["name"], tspat["name"]))
            naxis += 1
            cov += 1
    print("  dispatch bare names (space-axis)  : %d" % naxis, file=sys.stderr)

    # ── @sqlfn CANONICAL-NAME pass: emit the MobilityDB SQL surface ──
    # Every catalog function carries the canonical MobilityDB SQL spelling in its
    # @sqlfn tag (numInstants, eIntersects, atTime, asHexWKB ...). That is the name a
    # user — and the portable BerlinMOD suite — actually calls, so emit each UDF under
    # its @sqlfn name with the C symbol as backing. One @sqlfn often maps SEVERAL C
    # overloads differing only by argument KIND (eIntersects <- eintersects_tgeo_tgeo /
    # _tgeo_geo / _geo_tgeo); since Spark cannot overload a UDF name, those that share a
    # marshalled _sig() are emitted as ONE arg-kind-dispatching UDF (emit_dispatch).
    # Registered AFTER the contract bare names (class name sorts later) so an @sqlfn
    # with full overload dispatch supersedes a single-backing portable registration.
    # skip @sqlfn names already owned by the contract bare-name passes (operator
    # superclass registrations). EXCLUDE the distance family: its names (tDistance /
    # nearestApproachDistance) are better served by the @sqlfn arg-kind dispatch.
    portable_names = {e["bareName"] for fn, fam in fams.items() if fn != "distance" for e in fam}
    sqlgroups = {}
    for f in fns:
        s = f.get("sqlfn")
        if not s or s in names or s in portable_names or supported(f) is not None:
            continue
        sqlgroups.setdefault(s, []).append(f)
    nsql = nsqldisp = ndropped = nskip = 0
    sqlfn_emitted = set()
    for sname in sorted(sqlgroups):
        # subgroup the overloads by marshalled signature; emit the largest consistent
        # group (a name whose overloads disagree on arity/scalar-shape can't be one
        # Spark UDF — take the dominant shape, the rest stay reachable via their C name).
        bysig = {}
        for f in sqlgroups[sname]:
            sig = _sig(f)
            if sig is not None:
                bysig.setdefault(sig, []).append(f)
        if not bysig:
            continue
        group = max(bysig.values(), key=len)
        # A multi-overload @sqlfn needs a runtime parse dispatcher, which is only sound
        # when every overload discriminates via hex-WKB / WKT — drop the text-*_in ones
        # (stbox/tbox/cbuffer/npoint/pose), so e.g. nearestApproachDistance keeps just its
        # tgeo_tgeo / tgeo_geo overloads. A name left with no safe overload is skipped
        # (still reachable under its C names), never emitted as a fragile guess.
        if len(group) > 1:
            group = [f for f in group if _safe_dispatch(f)]
        if not group:
            nskip += 1
            continue
        # Keep only parse-DISTINGUISHABLE overloads: one per _parsetuple, preferring the
        # tgeo/geo family. Overloads differing only by temporal subtype can't be routed
        # by parsing, so they're left to their C name (not silently mis-dispatched).
        best = {}
        for f in group:
            t = _parsetuple(f)
            if t not in best or _famrank(f) < _famrank(best[t]):
                best[t] = f
        disp = sorted(best.values(), key=lambda f: f["name"])
        ndropped += len(group) - len(disp)
        # ever/always boolean predicates (eIntersects, aDisjoint, eDwithin ...) follow
        # the MobilityDB @sqlfn convention <e|a><Verb> and return int in C (1/0, -1 on
        # error) but boolean in SQL. Tag them with a predicate sqlop so ret_emit yields
        # BooleanType (== 1). Guarded on an int C-return, so atTime/asHexWKB (also a*) —
        # which return a temporal / string — are untouched.
        if re.match(r"[ea][A-Z]", sname):
            disp = [dict(f, sqlop="?=") if norm(f["returnType"]["canonical"]) == "int" else f
                    for f in disp]
        code = emit_single(sname, disp[0]) if len(disp) == 1 else emit_dispatch(sname, disp)
        if len(disp) > 1:
            nsqldisp += 1
        grouped.setdefault("GeneratedUdfs_sqlfn", []).append(code)
        sqlfn_emitted.add(sname)
        nsql += 1
        cov += 1
    print("  @sqlfn canonical names      : %d  (%d arg-kind-dispatched, %d subtype-siblings + %d unsafe-overload names to C-name)" %
          (nsql, nsqldisp, ndropped, nskip), file=sys.stderr)

    # Organize by doxygen module group (@ingroup), one class per group — the SAME
    # structure as the MEOS reference manual / XML docs, so a function is found in the
    # same place across tools. This also keeps every class small, dodging the per-class
    # constant-pool / BootstrapMethods limits a single 2300-lambda class would hit.
    # Within a class, register() statements are chunked to stay under the 64 KB method
    # bytecode limit.
    os.makedirs(args.out, exist_ok=True)
    # Clean stale generated files first: this tool fully OWNS args.out, so a prior
    # run's classes (a function later excluded by the jar arity/kind cross-check, or a
    # now-empty/renamed group) must not linger — they would silently break the build.
    for _old in glob.glob(os.path.join(args.out, "*.java")):
        os.remove(_old)
    CHUNK = 40        # register() statements per method (64 KB method-bytecode safety)
    MAXCLASS = 120    # UDFs per class (constant-pool / BootstrapMethods safety)
    written = []
    for grp in sorted(grouped):
        part = grouped[grp]
        subs = [part[i:i + MAXCLASS] for i in range(0, len(part), MAXCLASS)]
        for si, sub in enumerate(subs):
            cls = grp if len(subs) == 1 else "%s_%d" % (grp, si)
            chunks = [sub[i:i + CHUNK] for i in range(0, len(sub), CHUNK)]
            body = GEN_NOTE + IMPORTS + "\nfinal class %s {\n    private %s() {}\n" % (cls, cls)
            for i, ch in enumerate(chunks):
                body += "\n    private static void reg%d(SparkSession spark) {\n" % i + "\n".join(ch) + "\n    }\n"
            body += "\n    static void register(SparkSession spark) {\n"
            body += "\n".join("        reg%d(spark);" % i for i in range(len(chunks)))
            body += "\n    }\n}\n"
            with open(os.path.join(args.out, cls + ".java"), "w") as fh:
                fh.write(body)
            written.append(cls)

    with open(os.path.join(args.out, "UdfMarshal.java"), "w") as fh:
        fh.write(MARSHAL)

    main_cls = GEN_NOTE + IMPORTS + "\npublic final class GeneratedSpatioTemporalUDFs {\n"
    main_cls += "    private GeneratedSpatioTemporalUDFs() {}\n"
    main_cls += "\n    public static void registerAll(SparkSession spark) {\n"
    main_cls += "\n".join("        %s.register(spark);" % c for c in written)
    main_cls += "\n    }\n}\n"
    with open(os.path.join(args.out, "GeneratedSpatioTemporalUDFs.java"), "w") as fh:
        fh.write(main_cls)

    print("wrote %d group classes + UdfMarshal + GeneratedSpatioTemporalUDFs in %s" % (len(written), args.out), file=sys.stderr)
    print("  JMEOS functions in catalog : %d" % total, file=sys.stderr)
    print("  1:1 UDFs emitted (reached)  : %d  (%.0f%%)" % (cov, 100.0*cov/total), file=sys.stderr)
    print("  internal (excluded)         : %d" % internal, file=sys.stderr)
    print("  deferred type gaps (top):", file=sys.stderr)
    for k, c in skips.most_common(18):
        print("     %4d  %s" % (c, k), file=sys.stderr)


if __name__ == "__main__":
    main()
