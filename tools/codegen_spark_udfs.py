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
    # Type-SAFE hex-WKB wrappers: a *_from_hexwkb parser CRASHES (SIGSEGV) on a valid-hex
    # but wrong-TYPE buffer (a span hex fed to temporal_from_hexwkb). UdfMarshal.*FromHex
    # reads the WKB type byte (byte1 = MeosType) and only calls the C parser when the type
    # matches, else returns null — so a foreign hex is rejected, never crashes, and the
    # arg-kind dispatch can safely try every candidate.
    "Temporal":     ("UdfMarshal.tFromHex(%s)",       "K_TEMPORAL"),
    "TInstant":     ("UdfMarshal.tFromHex(%s)",       "K_TEMPORAL"),
    "TSequence":    ("UdfMarshal.tFromHex(%s)",       "K_TEMPORAL"),
    "TSequenceSet": ("UdfMarshal.tFromHex(%s)",       "K_TEMPORAL"),
    "GSERIALIZED":  ("UdfMarshal.geoFromText(%s)",                  "K_GEO"),
    "Span":         ("UdfMarshal.spanFromHex(%s)",    "K_SPAN"),
    "SpanSet":      ("UdfMarshal.spansetFromHex(%s)", "K_SPANSET"),
    "Set":          ("UdfMarshal.setFromHex(%s)",     "K_SET"),
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


# Default literals for SQL-OPTIONAL trailing args (params in [sqlArity, sqlArityMax))
# that a generated UDF supplies so it can expose the SQL-required arity instead of the
# wider C one — e.g. asHexWKB(temporal) calls temporal_as_hexwkb(p, (byte) 4),
# trajectory(temporal) calls tpoint_trajectory(p, false). Only scalar flags are
# defaultable; if a hidden arg isn't here the UDF keeps the full C arity.
HIDE_DEFAULT = {"bool": "false", "unsigned char": "(byte) 4", "int": "0",
                "double": "0.0", "int64_t": "0L", "uint64_t": "0L"}


def emit_single(name, f, vis_arity=None):
    params, out = classify(f)
    # SQL-faithful arity: expose only the first vis_arity params, supplying HIDE_DEFAULT
    # literals for the optional trailing flags. Fall back to the full C arity if any
    # hidden arg has no known default.
    hidden = []
    if vis_arity is not None and 0 <= vis_arity < len(params):
        tail = params[vis_arity:]
        if all(base(p["canonical"]) in HIDE_DEFAULT and "*" not in norm(p["canonical"]) for p in tail):
            hidden = tail
            params = params[:vis_arity]
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
    # supply default literals for the SQL-hidden trailing flags (sqlArity..C-arity)
    for p in hidden:
        callargs.append(HIDE_DEFAULT[base(p["canonical"])])
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
    type-safe hex-WKB wrapper (UdfMarshal.*FromHex, which checks the WKB type byte and
    returns null on a foreign family) or the validating WKT/EWKT parser
    (UdfMarshal.geoFromText, which delegates to geo_from_text and returns null on a
    foreign string). The text *_in parsers (stbox_in / tbox_in / cbuffer_in / npoint_in /
    pose_in / jsonb_in) are NOT: fed a hex-WKB or WKT string they may mis-parse, so an
    overload using one cannot be told apart at runtime and must not enter a dispatcher."""
    for p in classify(f)[0]:
        k = arg_kind(p["canonical"])
        if k and k[0] == "ptr" and "FromHex" not in k[1] and "geoFromText" not in k[1]:
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


def emit_dispatch(name, cands, vis_arity=None):
    """Emit ONE Spark UDF for an @sqlfn name backed by SEVERAL C overloads that share
    a _sig (e.g. eIntersects <- eintersects_tgeo_tgeo / _tgeo_geo / _geo_tgeo). Spark
    cannot overload a UDF name, so the single lambda parses each pointer arg with each
    candidate's parsers in turn: the FIRST candidate whose every pointer arg parses is
    the matching overload (the hex-WKB / WKT / span parsers are mutually discriminating,
    so exactly one matches). Parse-all-then-check keeps it leak-free on every path.
    vis_arity (SQL-required arity) exposes only the first N args, supplying HIDE_DEFAULT
    literals for the optional trailing flags (shared across the overloads via _sig)."""
    rep = cands[0]
    slots, ret = _sig(rep)
    n = len(slots)
    vis = n
    rep_params = classify(rep)[0]
    if vis_arity is not None and 0 <= vis_arity < n:
        tail = rep_params[vis_arity:]
        if all(base(p["canonical"]) in HIDE_DEFAULT and "*" not in norm(p["canonical"]) for p in tail):
            vis = vis_arity
            slots = slots[:vis]
            n = vis
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
        # SQL-hidden trailing flags get default literals (zip above paired only the
        # first `vis` exposed args; the candidate's remaining params are the flags).
        for p in cps[vis:]:
            callargs.append(HIDE_DEFAULT[base(p["canonical"])])
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


def emit_timearg(name, op):
    """Emit the time-restrict polymorphic UDF (atTime / minusTime): the time arg is a
    String classified at runtime (timestamp / period / set / span set) and routed to the
    matching temporal_<op>_{timestamptz,tstzspan,tstzset,tstzspanset} backing — Spark
    cannot overload a UDF name by the time arg's type."""
    refs = ", ".join("GeneratedFunctions::temporal_%s_%s" % (op, k)
                     for k in ("timestamptz", "tstzspan", "tstzset", "tstzspanset"))
    return ('        spark.udf().register("%s", (UDF2<String, String, String>) (a, b) -> {\n'
            '        if (a == null || b == null) return null;\n'
            '        MeosThread.ensureReady();\n'
            '        jnr.ffi.Pointer t = UdfMarshal.tFromHex(a);\n'
            '        if (t == null) return null;\n'
            '        try { return UdfMarshal.restrictTime(t, b, %s); }\n'
            '        finally { MeosMemory.free(t); }\n'
            '        }, DataTypes.StringType);' % (name, refs))


def tgeoarr_shape(f):
    """Recognize a MEOS NxN array kernel — params are one or more (Temporal **, int)
    array pairs, an optional `double` (distance), and optional non-const out-params
    (int *count, SpanSet ***periods). Returns a shape dict or None.
    ret: 'double' (scalar, e.g. minDistance) | 'pairs' (int* of 2*count [i,j], e.g. the
    *Pairs functions). periods=True iff a SpanSet*** out-param is present (tDwithin)."""
    ps = f["params"]
    n = len(ps)
    i = arrays = 0
    dist = has_count = periods = False
    while i < n:
        c = norm(ps[i]["canonical"]); isconst = "const" in ps[i]["canonical"]
        bare = c.replace("*", "").strip()   # base() maps ** -> __INTERNAL__, so strip here
        if bare == "Temporal" and c.endswith("**"):
            if i + 1 < n and norm(ps[i + 1]["canonical"]) == "int":
                arrays += 1; i += 2; continue
            return None
        if c == "double":
            dist = True; i += 1; continue
        if c == "int *" and not isconst:
            has_count = True; i += 1; continue
        if bare == "SpanSet" and c.count("*") == 3 and not isconst:
            periods = True; i += 1; continue
        return None
    if arrays < 1:
        return None
    rt = norm(f["returnType"]["canonical"])
    ret = "double" if rt == "double" else ("pairs" if rt == "int *" else None)
    if ret is None or (ret == "pairs" and not has_count):
        return None
    return {"arrays": arrays, "dist": dist, "periods": periods, "ret": ret}


def emit_tgeoarr(name, f, shape):
    """Emit a Spark UDF for an NxN array kernel. Each (Temporal**, int) pair becomes one
    Spark array<string> arg (the count is the array length); an optional `double` distance
    is a Double arg; out-params (count / periods) are auto-allocated. Scalar-return kernels
    yield a Double UDF; pairs-return kernels yield array<struct<i,j[,periods]>> (consumed via
    LATERAL explode), with MEOS 1-based indices mapped to 0-based."""
    nA = shape["arrays"]
    argnames = ["a%d" % i for i in range(nA)] + (["dist"] if shape["dist"] else [])
    # dist arrives as Object: a bare SQL literal like 10.0 is decimal (BigDecimal), not
    # double, so take it as Object and coerce via Number rather than failing the cast.
    boxes = ["Object"] * nA + (["Object"] if shape["dist"] else [])
    if shape["ret"] == "double":
        retbox, ret_dt = "Double", "DataTypes.DoubleType"
    else:
        fields = ['DataTypes.createStructField("i", DataTypes.IntegerType, false)',
                  'DataTypes.createStructField("j", DataTypes.IntegerType, false)']
        if shape["periods"]:
            fields.append('DataTypes.createStructField("periods", DataTypes.StringType, true)')
        struct = "DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{%s})" % ", ".join(fields)
        retbox, ret_dt = "java.util.List<org.apache.spark.sql.Row>", "DataTypes.createArrayType(%s)" % struct
    iface = "UDF%d<%s, %s>" % (len(argnames), ", ".join(boxes), retbox)
    L = ['        spark.udf().register("%s", (%s) (%s) -> {' % (name, iface, ", ".join(argnames))]
    L.append("        if (" + " || ".join("a%d == null" % i for i in range(nA)) + ") return null;")
    L.append("        MeosThread.ensureReady();")
    for i in range(nA):
        L.append("        String[] s%d = UdfMarshal.asStrArray(a%d);" % (i, i))
    L.append("        if (" + " || ".join("s%d == null" % i for i in range(nA)) + ") return null;")
    L.append("        jnr.ffi.Runtime _rt = jnr.ffi.Runtime.getSystemRuntime();")
    for i in range(nA):
        L.append("        jnr.ffi.Pointer[] e%d = new jnr.ffi.Pointer[s%d.length];" % (i, i))
        L.append("        jnr.ffi.Pointer arr%d = UdfMarshal.tArrNative(s%d, e%d, _rt);" % (i, i, i))
    callargs = []
    for i in range(nA):
        callargs += ["arr%d" % i, "s%d.length" % i]
    if shape["dist"]:
        callargs.append("dist == null ? 0.0 : ((Number) dist).doubleValue()")
    if shape["ret"] == "pairs":
        L.append("        jnr.ffi.Pointer _cnt = jnr.ffi.Memory.allocateDirect(_rt, 4);")
        callargs.append("_cnt")
    if shape["periods"]:
        L.append("        jnr.ffi.Pointer _per = jnr.ffi.Memory.allocateDirect(_rt, 8);")
        callargs.append("_per")
    call = "GeneratedFunctions.%s(%s)" % (f["name"], ", ".join(callargs))
    L.append("        try {")
    if shape["ret"] == "double":
        L.append("            return %s;" % call)
    else:
        L.append("            jnr.ffi.Pointer _res = %s;" % call)
        L.append("            int _c = _cnt.getInt(0L);")
        if shape["periods"]:
            L.append("            return UdfMarshal.readPairsPeriods(_res, _c, _per.getPointer(0L));")
        else:
            L.append("            return UdfMarshal.readPairs(_res, _c);")
    L.append("        } finally {")
    for i in range(nA):
        L.append("            UdfMarshal.freeArr(e%d);" % i)
    L.append("        }")
    L.append("        }, %s);" % ret_dt)
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
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
        // A String timestamp: MEOS owns TZ resolution (ecosystem-uniform, like SRID via
        // geo_from_text) — parse via timestamptz_in, NEVER a Java/Spark offset fixup.
        return GeneratedFunctions.timestamptz_in(a.toString().trim(), -1);
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

    // Geometry/geography arg parse. The canonical wire form is EWKT
    // ("SRID=4326;POLYGON(...)") so the SRID survives to MEOS — but geo_from_text() is
    // WKT-only and returns null on the "SRID=" prefix, dropping the SRID (which H3 /
    // geo_to_h3index_set REQUIRE via ensure_srid_is_latlong). Split the prefix and pass
    // its value as geo_from_text's srid argument; plain WKT (no prefix) parses at SRID 0.
    // CRITICAL for overload dispatch: reject a pure-hex string up front. geo_from_text()
    // also accepts hex-(E)WKB, so a temporal/set hex-WKB arg (a tgeompoint trip) would be
    // MIS-READ as a geometry (garbage npoints / free() crash) when a (geo, tgeo) candidate
    // is tried before the (tgeo, geo) one. A real WKT/EWKT geometry ALWAYS contains
    // non-hex chars (P O L Y G N '(' '=' ';' ...), so isHex(s) is false for it; only a
    // foreign WKB hex is pure-hex — exactly what must fall through to the next candidate.
    static Pointer geoFromText(String s) {
        if (s == null || isHex(s)) return null;
        int srid = 0; String wkt = s;
        if (s.length() > 5 && s.regionMatches(true, 0, "SRID=", 0, 5)) {
            int semi = s.indexOf(';');
            if (semi > 5) {
                try { srid = Integer.parseInt(s.substring(5, semi).trim()); wkt = s.substring(semi + 1); }
                catch (NumberFormatException e) { srid = 0; wkt = s; }
            }
        }
        return GeneratedFunctions.geo_from_text(wkt, srid);
    }

    // ── NxN array (tgeoarr) marshalling ──────────────────────────────────────────
    // The MEOS *_tgeoarr_tgeoarr kernels take (Temporal **arr, int n) array pairs and
    // run the whole NxN inside C. Spark passes array<string> as a scala Seq / WrappedArray
    // (or a java List), so normalize to String[] first.
    static String[] asStrArray(Object o) {
        if (o == null) return null;
        if (o instanceof scala.collection.Seq) {
            scala.collection.Seq<?> q = (scala.collection.Seq<?>) o;
            String[] r = new String[q.length()];
            for (int i = 0; i < r.length; i++) { Object e = q.apply(i); r[i] = e == null ? null : e.toString(); }
            return r;
        }
        if (o instanceof java.util.List) {
            java.util.List<?> l = (java.util.List<?>) o;
            String[] r = new String[l.size()];
            for (int i = 0; i < r.length; i++) { Object e = l.get(i); r[i] = e == null ? null : e.toString(); }
            return r;
        }
        if (o instanceof Object[]) {
            Object[] a = (Object[]) o; String[] r = new String[a.length];
            for (int i = 0; i < r.length; i++) r[i] = a[i] == null ? null : a[i].toString();
            return r;
        }
        return null;
    }
    // Parse a hex-temporal array into a native Temporal** buffer; the parsed element
    // pointers are stored in `elems` (parallel) so the caller frees them after the call.
    static Pointer tArrNative(String[] hex, Pointer[] elems, Runtime rt) {
        Pointer buf = Memory.allocateDirect(rt, Math.max(1, hex.length) * 8);
        for (int i = 0; i < hex.length; i++) {
            Pointer p = (hex[i] != null && isHex(hex[i])) ? GeneratedFunctions.temporal_from_hexwkb(hex[i]) : null;
            elems[i] = p;
            buf.putPointer((long) i * 8L, p);
        }
        return buf;
    }
    static void freeArr(Pointer[] elems) { for (Pointer p : elems) MeosMemory.free(p); }
    // A *_tgeoarr_tgeoarr kernel returns a flat int* of 2*count [i,j] index pairs (caller
    // frees). The C kernel uses 0-based C indices (the PG SETOF wrapper is what makes them
    // 1-based) — and the Spark UDF calls the kernel directly via JMEOS, so the indices are
    // already 0-based array offsets; emit them as-is (matching MeosSetSetJoin).
    static java.util.List<Row> readPairs(Pointer res, int cnt) {
        java.util.ArrayList<Row> out = new java.util.ArrayList<>();
        if (res == null || cnt <= 0) return out;
        for (int k = 0; k < cnt; k++)
            out.add(RowFactory.create(res.getInt((long) (2 * k) * 4L),
                                      res.getInt((long) (2 * k + 1) * 4L)));
        MeosMemory.free(res);
        return out;
    }
    // tDwithin also returns a parallel SpanSet** of the per-pair intersection periods
    // (via a SpanSet ***out-param); render each as hex-WKB. Frees res, the periods array,
    // and each period span set.
    static java.util.List<Row> readPairsPeriods(Pointer res, int cnt, Pointer ssArr) {
        java.util.ArrayList<Row> out = new java.util.ArrayList<>();
        if (res == null || cnt <= 0) { MeosMemory.free(res); return out; }
        for (int k = 0; k < cnt; k++) {
            Pointer ss = ssArr == null ? null : ssArr.getPointer((long) k * 8L);
            String periods = ss == null ? null : GeneratedFunctions.spanset_as_hexwkb(ss, (byte) 0);
            out.add(RowFactory.create(res.getInt((long) (2 * k) * 4L),
                                      res.getInt((long) (2 * k + 1) * 4L), periods));
            MeosMemory.free(ss);
        }
        MeosMemory.free(ssArr);
        MeosMemory.free(res);
        return out;
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
        Pointer p1 = tFromHex(s1);
        if (p1 == null) return null;
        Pointer p2 = tFromHex(s2);
        if (p2 == null) { MeosMemory.free(p1); return null; }
        try {
            return isTnumber(p1) ? tnumber.apply(p1, p2) : tspatial.apply(p1, p2);
        } finally {
            MeosMemory.free(p1);
            MeosMemory.free(p2);
        }
    }

    // ── Type-SAFE hex-WKB parsing ────────────────────────────────────────────────
    // A *_from_hexwkb parser reads the MEOS WKB structure for ONE type family and
    // SEGV-crashes on a valid-hex buffer of a different family (a tstzspan hex fed to
    // temporal_from_hexwkb). The WKB type lives in byte 1 (= the MeosType enum value),
    // so read it first and only call the C parser when the family matches. These sets
    // are generated from the catalog's MeosType enum (data-driven, no hardcoding).
__WKB_KINDS__
    // The MeosType byte = the 2nd WKB byte (hex chars [2,4)). -1 if not a hex-WKB string.
    static int wkbType(String s) {
        if (s == null || s.length() < 4 || !isHex(s)) return -1;
        return Character.digit(s.charAt(2), 16) * 16 + Character.digit(s.charAt(3), 16);
    }
    static Pointer tFromHex(String s) {
        return TEMPORAL_WKB.contains(wkbType(s)) ? GeneratedFunctions.temporal_from_hexwkb(s) : null;
    }
    static Pointer spanFromHex(String s) {
        return SPAN_WKB.contains(wkbType(s)) ? GeneratedFunctions.span_from_hexwkb(s) : null;
    }
    static Pointer spansetFromHex(String s) {
        return SPANSET_WKB.contains(wkbType(s)) ? GeneratedFunctions.spanset_from_hexwkb(s) : null;
    }
    static Pointer setFromHex(String s) {
        return SET_WKB.contains(wkbType(s)) ? GeneratedFunctions.set_from_hexwkb(s) : null;
    }

    // Time-restrict polymorphism (atTime / minusTime): MobilityDB resolves the time arg
    // by type (timestamptz / tstzspan / tstzset / tstzspanset), but Spark cannot overload
    // a UDF name. The arg arrives as a String — classify it by its first char (a period
    // "[..]", a set "{..}", a span set "{[..],..}", else a bare timestamp) and route to
    // the matching MEOS overload, returning the restricted temporal as hex-WKB.
    static String restrictTime(Pointer t, String arg,
            BiFunction<Pointer, java.time.OffsetDateTime, Pointer> byTs,
            BiFunction<Pointer, Pointer, Pointer> bySpan,
            BiFunction<Pointer, Pointer, Pointer> bySet,
            BiFunction<Pointer, Pointer, Pointer> bySpanset) {
        String s = arg.trim();
        Pointer r;
        if (s.startsWith("{")) {
            boolean spanset = s.indexOf('[') >= 0 || s.indexOf('(') >= 0;
            Pointer p = spanset ? GeneratedFunctions.tstzspanset_in(s) : GeneratedFunctions.tstzset_in(s);
            if (p == null) return null;
            try { r = (spanset ? bySpanset : bySet).apply(t, p); } finally { MeosMemory.free(p); }
        } else if (s.startsWith("[") || s.startsWith("(")) {
            Pointer p = GeneratedFunctions.tstzspan_in(s);
            if (p == null) return null;
            try { r = bySpan.apply(t, p); } finally { MeosMemory.free(p); }
        } else {
            r = byTs.apply(t, tsOdt(s));
        }
        if (r == null) return null;
        try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 4); } finally { MeosMemory.free(r); }
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
    # string. So a portable bare name (eEq, tLt, aGe — RFC #920 / contract #19) wraps its
    # superclass *_temporal_temporal C symbol for ALL temporal subtypes. But some eEq/eNe
    # overloads have a NON-temporal first arg — eEq(geo, tgeo), and the th3 cell-set
    # prefilter eEq(h3indexset, th3index) whose first arg is a Set* — which the Temporal*
    # superclass cannot reach. Gather those (same 2-pointer-Boolean signature, distinct
    # parse-tuple, dispatch-safe) and emit ONE parse-dispatching UDF so they resolve too.
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
            if not (backing and supported(backing) is None):
                continue
            # Non-temporal overloads under the SAME @sqlfn bare name (geo/set first arg),
            # sharing the superclass's (P,P)->Boolean signature, each parse-distinct.
            rep_sig = _sig(backing)
            cands, seen = [backing], {_parsetuple(backing)}
            extras = [f for f in fns
                      if f.get("sqlfn") == bare and f["name"] != backing["name"]
                      and supported(f) is None and _safe_dispatch(f)
                      and _sig(f) == rep_sig]
            for f in sorted(extras, key=_famrank):
                pt = _parsetuple(f)
                if pt not in seen:
                    seen.add(pt)
                    cands.append(f)
            emit = emit_single(bare, backing) if len(cands) == 1 else emit_dispatch(bare, cands)
            grouped.setdefault("GeneratedUdfs_portable_comparison", []).append(emit)
            ndisp += 1
            cov += 1
    print("  dispatch bare names (comparison)  : %d" % ndisp, file=sys.stderr)

    # ── bare-name-IS-prefix families: topology / same / time / space Y,Z ──
    # Each contract bareName IS the MEOS C operator prefix, and the superclass
    # entrypoint *_temporal_temporal (time/topology) or *_tspatial_tspatial (spatial
    # position) dispatches every concrete subtype internally from the type-erased
    # hex-WKB — so one emit covers all six type families. Same emit machinery as the
    # comparison dispatch; only the backing-name pattern differs.
    PREFIX = [("same",         "%s_temporal_temporal"),
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
    # topology (&&, @>, <@, -|-) is polymorphic over SPANS and TEMPORALS: overlaps(
    # tstzspan, tstzspan) = overlaps_span_span, overlaps(tgeompoint, ...) =
    # overlaps_temporal_temporal. Dispatch across both — the type-safe WKB parsers route a
    # span (e.g. from timeSpan()) to the span backing instead of crashing the temporal one.
    for e in fams.get("topology", []):
        bare = e["bareName"]
        cands = [by_name.get(bare + "_span_span"), by_name.get(bare + "_temporal_temporal")]
        cands = [f for f in cands if f and supported(f) is None]
        if not cands:
            continue
        code = emit_single(bare, cands[0]) if len(cands) == 1 else emit_dispatch(bare, cands)
        grouped.setdefault("GeneratedUdfs_portable_operator", []).append(code)
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

    # ── NxN array (tgeoarr) pass: array-in + SETOF/array-of-struct UDFs ──
    # The *_tgeoarr_tgeoarr kernels take Temporal** array args, so the 1:1 and @sqlfn
    # passes exclude them (** -> internal). Emit each under its @sqlfn name via the array
    # template: minDistance (array,array -> double) and the *Pairs (array,array[,dist] ->
    # array<struct<i,j[,periods]>>, consumed by LATERAL explode; MEOS 1-based -> 0-based).
    narr = 0
    for f in fns:
        s = f.get("sqlfn")
        if not s or s in names:
            continue
        if jar_syms is not None and f["name"] not in jar_syms:
            continue
        shape = tgeoarr_shape(f)
        if shape is None:
            continue
        grouped.setdefault(class_for(f.get("group")), []).append(emit_tgeoarr(s, f, shape))
        names.add(s)
        cov += 1
        narr += 1
    print("  NxN array (tgeoarr) UDFs          : %d" % narr, file=sys.stderr)

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
        # time-restrict polymorphism (atTime / minusTime): the time arg type
        # (timestamptz / tstzspan / tstzset / spanset) can't be ONE Spark UDF signature,
        # so emit a String-classifying dispatch instead of the normal arg-kind one.
        gnames = {f["name"] for f in sqlgroups[sname]}
        trop = next((m for m in ("at", "minus")
                     if ("temporal_%s_timestamptz" % m) in gnames
                     and ("temporal_%s_tstzspan" % m) in gnames), None)
        if trop:
            grouped.setdefault("GeneratedUdfs_sqlfn", []).append(emit_timearg(sname, trop))
            sqlfn_emitted.add(sname)
            nsql += 1
            cov += 1
            continue
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
        # expose the SQL-required arity (sqlArity) — default the optional trailing flags
        # so e.g. asHexWKB(temporal) / trajectory(temporal) are 1-arg, matching SQL.
        va = disp[0].get("sqlArity")
        code = (emit_single(sname, disp[0], vis_arity=va) if len(disp) == 1
                else emit_dispatch(sname, disp, vis_arity=va))
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

    # Build the WKB-kind byte sets from the catalog's MeosType enum (the WKB type byte =
    # the enum value): categorise each T_* by name suffix so the safe hex parsers know
    # which MeosType bytes belong to temporals vs spans vs spansets vs sets.
    mt = next((e for e in cat.get("enums", []) if e["name"] in ("MeosType", "meosType")), None)
    kinds = {"TEMPORAL_WKB": [], "SPAN_WKB": [], "SPANSET_WKB": [], "SET_WKB": []}
    for v in (mt.get("values") or mt.get("members") or []) if mt else []:
        nm = v["name"] if isinstance(v, dict) else v
        val = v.get("value") if isinstance(v, dict) else None
        if val is None or not nm:
            continue
        if nm.endswith("SPANSET"):
            kinds["SPANSET_WKB"].append(val)
        elif nm.endswith("SPAN"):
            kinds["SPAN_WKB"].append(val)
        elif nm.endswith("SET"):
            kinds["SET_WKB"].append(val)
        elif nm.startswith("T_T") and "BOX" not in nm:
            kinds["TEMPORAL_WKB"].append(val)
    wkb_lines = "\n".join(
        "    private static final java.util.Set<Integer> %s = java.util.Set.of(%s);"
        % (k, ", ".join(str(x) for x in sorted(set(v)))) for k, v in kinds.items())
    with open(os.path.join(args.out, "UdfMarshal.java"), "w") as fh:
        fh.write(MARSHAL.replace("__WKB_KINDS__", wkb_lines))

    main_cls = GEN_NOTE + IMPORTS + "\npublic final class GeneratedSpatioTemporalUDFs {\n"
    main_cls += "    private GeneratedSpatioTemporalUDFs() {}\n"
    main_cls += "\n    public static void registerAll(SparkSession spark) {\n"
    main_cls += "\n".join("        %s.register(spark);" % c for c in written)
    main_cls += "\n    }\n}\n"
    with open(os.path.join(args.out, "GeneratedSpatioTemporalUDFs.java"), "w") as fh:
        fh.write(main_cls)

    # ── per-thread MEOS-init invariant (build-failing) ────────────────────────────
    # MEOS keeps locale/collation, session timezone, PROJ context and RNGs in THREAD-
    # LOCAL storage, so every thread (Spark executor threads run UDFs off the thread
    # that called meos_initialize()) must run the per-thread init guard before its first
    # MEOS call — exactly the gap that crashed MobilityDuck's table functions. Assert it
    # for EVERY emitted entry point so a future emit path can't silently drop the guard
    # as the bindings are regenerated (the codegen north-star). A register() lambda is
    # guarded iff ensureReady() / axisBool / restrictTime (the latter two call
    # ensureReady() first) appears before the first GeneratedFunctions call in its body.
    unguarded = []
    for fp in glob.glob(os.path.join(args.out, "*.java")):
        src = open(fp).read()
        for m in re.finditer(r'udf\(\)\.register\("([^"]+)"', src):
            body = src[m.start():m.start() + 1600]
            gf = body.find("GeneratedFunctions.")
            guard = min([x for x in (body.find("ensureReady"), body.find("axisBool"),
                                     body.find("restrictTime")) if x >= 0] or [1 << 30])
            if gf >= 0 and guard > gf:
                unguarded.append("%s (%s)" % (m.group(1), os.path.basename(fp)))
    if unguarded:
        print("FATAL: %d generated UDF entry point(s) reach MEOS without a per-thread "
              "init guard (MeosThread.ensureReady):" % len(unguarded), file=sys.stderr)
        for u in unguarded[:20]:
            print("   " + u, file=sys.stderr)
        sys.exit(1)

    print("wrote %d group classes + UdfMarshal + GeneratedSpatioTemporalUDFs in %s" % (len(written), args.out), file=sys.stderr)
    print("  per-thread MEOS-init guard  : every entry point verified", file=sys.stderr)
    print("  JMEOS functions in catalog : %d" % total, file=sys.stderr)
    print("  1:1 UDFs emitted (reached)  : %d  (%.0f%%)" % (cov, 100.0*cov/total), file=sys.stderr)
    print("  internal (excluded)         : %d" % internal, file=sys.stderr)
    print("  deferred type gaps (top):", file=sys.stderr)
    for k, c in skips.most_common(18):
        print("     %4d  %s" % (c, k), file=sys.stderr)


if __name__ == "__main__":
    main()
