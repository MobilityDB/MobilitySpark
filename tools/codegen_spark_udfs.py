#!/usr/bin/env python3
"""Generate MobilitySpark UDF-registration classes from the MEOS-API catalog.

North Star (meos-api-codegen-regularity): the Spark UDF surface is GENERATED
from the single MEOS-API source of truth, never hand-written.

Inputs:
  - MEOS-API catalog output/meos-idl.json (MEOS-C functions + C-type sigs + the
    @sqlfn / @sqlop map from MEOS-API #18).

Two emission modes:
  - SINGLE: a SQL name with one backing -> a 1:1 UDF.
  - DISPATCH: a SQL name / operator with N typed backings (Spark cannot overload
    by name) -> ONE UDF that classifies each String arg by its MEOS type and
    routes to the catalog-determined backing. The classification is parser-driven
    (MEOS decides the type), the routing table is the catalog's param types, and
    the emitted lambdas call only static GeneratedFunctions (no captured state ->
    serializable). Zero hand heuristics, zero new MEOS functions.

Usage: python3 tools/codegen_spark_udfs.py [--catalog PATH] [--out DIR]
"""
import argparse
import json
import os
import sys

ARG_RULES = {
    "Temporal":   ("GeneratedFunctions.temporal_from_hexwkb(%s)", True),
    "GSERIALIZED": ("GeneratedFunctions.geo_from_text(%s, 0)", True),
    "Span":       ("GeneratedFunctions.tstzspan_in(%s)", True),
    "STBox":      ("GeneratedFunctions.stbox_in(%s)", True),   # TEXT form, not hex
    "Timestamp":  ("parseTs(%s)", False),
}
RET_RULES = {
    "double":        ("DoubleType",  "%s", False),
    "bool":          ("BooleanType", "%s", False),
    "int":           ("BooleanType", "%s == 1", False),
    "Temporal *":    ("StringType",  "GeneratedFunctions.temporal_as_hexwkb(%s, (byte) 4)", True),
    "GSERIALIZED *": ("StringType",  "GeneratedFunctions.geo_as_text(%s, 15)", True),
    "Span *":        ("StringType",  "GeneratedFunctions.span_out(%s, 15)", True),
    "SpanSet *":     ("StringType",  "GeneratedFunctions.spanset_out(%s, 15)", True),
    "STBox *":       ("StringType",  "GeneratedFunctions.stbox_out(%s, 15)", True),   # TEXT form
}
# canonical C type -> dispatch KIND constant emitted in the class
KMAP = {"Temporal *": "K_TEMPORAL", "GSERIALIZED *": "K_GEO", "Span *": "K_SPAN",
        "STBox *": "K_STBOX", "TimestampTz": "K_TS"}
# KIND -> (Op-field for the parsed value used as a call arg)
KARG = {"K_TEMPORAL": "%s.ptr", "K_GEO": "%s.ptr", "K_SPAN": "%s.ptr",
        "K_STBOX": "%s.ptr", "K_TS": "%s.ts"}


def norm(canon):
    return canon.replace("const ", "").replace("struct ", "").strip()


def kind_of(canonical):
    c = norm(canonical)
    if c.startswith("Temporal"):    return "Temporal"
    if c.startswith("GSERIALIZED"): return "GSERIALIZED"
    if c.startswith("Span") and "Set" not in c: return "Span"
    if c.startswith("STBox"):       return "STBox"
    if c == "TimestampTz":          return "Timestamp"
    return None


def ret_key(canonical):
    c = norm(canonical)
    if c in RET_RULES:
        return c
    return None


# Single-signature SQL names; overloaded ones go through DISPATCH below.
SPEC = ["whenTrue"]
# Dispatch groups: (udf name, how to collect backings from the catalog).
#   {"name","by":"sqlop"|"sqlfn","key":..., "arity":N}
# timeSpan dispatches on the arg's MEOS type (temporal/stbox) and returns the
# span's TEXT form (span_out) — matching the wire format the suite passes between
# functions (the hand AccessorUDFs.timespan leaked the parsed trip AND emitted hex).
DISPATCH = [
    {"name": "overlaps", "by": "sqlop", "key": "&&", "arity": 2},
    {"name": "stbox",    "by": "sqlfn", "key": "stbox", "arity": 2},
    {"name": "timeSpan", "by": "sqlfn", "key": "timeSpan", "arity": 1},
]


def emit_udf(name, f):
    params = f["params"]
    rk = ret_key(f["returnType"]["canonical"])
    if rk is None:
        raise SystemExit("unsupported return for %s: %s" % (name, f["returnType"]))
    ret_dt, ret_ser, ret_free = RET_RULES[rk]
    box = {"DoubleType": "Double", "BooleanType": "Boolean", "StringType": "String"}[ret_dt]
    nargs = len(params)
    udf_iface = "UDF%d<%s, %s>" % (nargs, ", ".join(["String"] * nargs), box)
    argnames = [p["name"] or ("a%d" % i) for i, p in enumerate(params)]
    L = [f"    public static final {udf_iface} {name} = (" + ", ".join(argnames) + ") -> {"]
    L.append("        if (" + " || ".join(f"{a} == null" for a in argnames) + ") return null;")
    L.append("        MeosThread.ensureReady();")
    ptrs = []
    for a, p in zip(argnames, params):
        k = kind_of(p["canonical"])
        if k is None:
            raise SystemExit("unsupported arg for %s: %s" % (name, p["canonical"]))
        parse, free = ARG_RULES[k]
        L.append(f"        Pointer p_{a} = {parse % a};")
        L.append(f"        if (p_{a} == null) return null;")
        if free:
            ptrs.append(f"p_{a}")
    call = f"GeneratedFunctions.{f['name']}(" + ", ".join(f"p_{a}" for a in argnames) + ")"
    L.append("        try {")
    if ret_free:
        L.append(f"            Pointer _r = {call};")
        L.append("            if (_r == null) return null;")
        L.append(f"            try {{ return {ret_ser % '_r'}; }} finally {{ MeosMemory.free(_r); }}")
    else:
        L.append(f"            return {ret_ser % call};")
    L.append("        } finally {")
    for ptr in ptrs:
        L.append(f"            MeosMemory.free({ptr});")
    L.append("        }")
    L.append("    };")
    reg = f'        spark.udf().register("{name}", {name}, DataTypes.{ret_dt});'
    return "\n".join(L), reg


def emit_dispatch(name, backings):
    """One UDF that classifies each arg's MEOS type and routes to a backing."""
    arity = len(backings[0]["params"])
    # keep backings of this arity whose every param is a classifiable KIND;
    # types the classifier does not cover yet (Cbuffer/Npoint/Set/SpanSet/TBox)
    # are simply not routed — they are added when their parser/KIND lands.
    backings = [b for b in backings if len(b["params"]) == arity
                and all(KMAP.get(norm(p["canonical"])) for p in b["params"])]
    if not backings:
        return None, None
    pos_kinds = [set() for _ in range(arity)]
    for b in backings:
        for i, p in enumerate(b["params"]):
            pos_kinds[i].add(KMAP[norm(p["canonical"])])
    # uniform return type
    rks = {norm(b["returnType"]["canonical"]) for b in backings}
    if len(rks) != 1 or ret_key(rks.pop()) is None:
        return None, None
    rk = norm(backings[0]["returnType"]["canonical"])
    ret_dt, ret_ser, ret_free = RET_RULES[rk]
    box = {"DoubleType": "Double", "BooleanType": "Boolean", "StringType": "String"}[ret_dt]
    # a position is Object (may carry a java.sql.Timestamp) iff K_TS is possible
    argtypes = ["Object" if "K_TS" in pos_kinds[i] else "String" for i in range(arity)]
    argnames = ["x%d" % i for i in range(arity)]
    iface = "UDF%d<%s, %s>" % (arity, ", ".join(argtypes), box)

    L = [f"    public static final {iface} {name} = (" + ", ".join(argnames) + ") -> {"]
    L.append("        if (" + " || ".join(f"{a} == null" for a in argnames) + ") return null;")
    L.append("        MeosThread.ensureReady();")
    for a, at in zip(argnames, argtypes):
        cls = "classifyObj" if at == "Object" else "classify"
        L.append(f"        Op o_{a} = {cls}({a});")
    L.append("        try {")
    # one branch per backing, keyed on its kind tuple (only unambiguous tuples)
    tuple_count = {}
    for b in backings:
        t = tuple(KMAP[norm(p["canonical"])] for p in b["params"])
        tuple_count[t] = tuple_count.get(t, 0) + 1
    emitted = 0
    for b in backings:
        t = tuple(KMAP[norm(p["canonical"])] for p in b["params"])
        if tuple_count[t] > 1:
            continue  # ambiguous by KIND (subtype-only difference) -> skip
        cond = " && ".join(f"o_{argnames[i]}.kind == {t[i]}" for i in range(arity)) \
            + " && " + " && ".join(
                (f"o_{argnames[i]}.ptr != null" if t[i] != "K_TS" else f"o_{argnames[i]}.ts != null")
                for i in range(arity))
        callargs = ", ".join(KARG[t[i]] % f"o_{argnames[i]}" for i in range(arity))
        call = f"GeneratedFunctions.{b['name']}({callargs})"
        if ret_free:
            L.append(f"            if ({cond}) {{ Pointer _r = {call}; "
                     f"try {{ return _r==null?null:{ret_ser % '_r'}; }} finally {{ MeosMemory.free(_r); }} }}")
        else:
            L.append(f"            if ({cond}) return {ret_ser % call};")
        emitted += 1
    if emitted == 0:
        return None, None
    L.append("            return null;")
    L.append("        } finally {")
    for a in argnames:
        L.append(f"            freeOp(o_{a});")
    L.append("        }")
    L.append("    };")
    reg = f'        spark.udf().register("{name}", {name}, DataTypes.{ret_dt});'
    return "\n".join(L), reg


HEADER = """\
// GENERATED by tools/codegen_spark_udfs.py from the MEOS-API catalog. DO NOT EDIT.
package org.mobilitydb.spark.generated;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

public final class GeneratedSpatioTemporalUDFs {
    private GeneratedSpatioTemporalUDFs() {}

    private static final DateTimeFormatter PG_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static OffsetDateTime parseTs(Object arg) {
        if (arg instanceof java.sql.Timestamp)
            return GeneratedFunctions.pg_timestamptz_in(
                ((java.sql.Timestamp) arg).toInstant().atOffset(ZoneOffset.UTC).format(PG_FMT), -1);
        return GeneratedFunctions.pg_timestamptz_in(arg.toString().trim(), -1);
    }

    private static Pointer szbuf() {
        return Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
    }

    // ── runtime MEOS-type classification for generated dispatch ──
    // The value's type is decided by MEOS, not by semantic guessing: a leading
    // '[' / '(' is a span; a date-like string is a timestamp; a hex-WKB body is
    // parsed as a temporal and falls back to an stbox; anything else is geometry.
    static final int K_NONE = -1, K_SPAN = 0, K_TEMPORAL = 1, K_STBOX = 2, K_GEO = 3, K_TS = 4;

    static final class Op { int kind = K_NONE; Pointer ptr; OffsetDateTime ts; }

    private static boolean isHex(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f'))) return false;
        }
        return s.length() > 0;
    }

    static Op classify(String s) {
        Op o = new Op();
        if (s == null || s.isEmpty()) return o;
        char c = s.charAt(0);
        // Unambiguous by leading token: spans/stboxes/geometries travel as TEXT,
        // ONLY temporals travel as hex-WKB — so temporal_from_hexwkb is never fed a
        // non-temporal (which could misread a size field and over-allocate).
        if (c == '[' || c == '(') { o.kind = K_SPAN; o.ptr = GeneratedFunctions.tstzspan_in(s); return o; }
        if (s.regionMatches(true, 0, "STBOX", 0, 5)) { o.kind = K_STBOX; o.ptr = GeneratedFunctions.stbox_in(s); return o; }
        if (c >= '0' && c <= '9' && s.indexOf('-') > 0) { o.kind = K_TS; o.ts = parseTs(s); return o; }
        if (isHex(s)) { o.kind = K_TEMPORAL; o.ptr = GeneratedFunctions.temporal_from_hexwkb(s); return o; }
        o.kind = K_GEO; o.ptr = GeneratedFunctions.geo_from_text(s, 0);
        return o;
    }

    static Op classifyObj(Object a) {
        if (a instanceof java.sql.Timestamp) { Op o = new Op(); o.kind = K_TS; o.ts = parseTs(a); return o; }
        return classify(a == null ? null : a.toString());
    }

    private static void freeOp(Op o) { if (o != null && o.ptr != null) MeosMemory.free(o.ptr); }

"""


def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument("--catalog", default=os.path.join(here, "..", "..", "MEOS-API", "output", "meos-idl.json"))
    ap.add_argument("--out", default=os.path.join(here, "..", "src", "main", "java", "org", "mobilitydb", "spark", "generated"))
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    cat = json.load(open(args.catalog))
    fns = cat["functions"]
    by_sqlfn = {}
    for f in fns:
        s = f.get("sqlfn")
        if s:
            by_sqlfn.setdefault(s, []).append(f)

    bodies, regs = [], []
    for name in SPEC:
        cands = by_sqlfn.get(name, [])
        if len(cands) == 1:
            b, r = emit_udf(name, cands[0])
            bodies.append(b); regs.append(r)
        else:
            print("  SKIP single %s: %d candidates" % (name, len(cands)), file=sys.stderr)

    for d in DISPATCH:
        if d["by"] == "sqlop":
            cands = [f for f in fns if f.get("sqlop") == d["key"] and len(f["params"]) == d["arity"]]
        else:
            cands = [f for f in by_sqlfn.get(d["key"], []) if len(f["params"]) == d["arity"]]
        if not cands:
            print("  SKIP dispatch %s: no backings" % d["name"], file=sys.stderr); continue
        b, r = emit_dispatch(d["name"], cands)
        if b is None:
            print("  SKIP dispatch %s: no emittable branches" % d["name"], file=sys.stderr); continue
        bodies.append(b); regs.append(r)
        print("  dispatch %s: %d backings" % (d["name"], len(cands)), file=sys.stderr)

    out = HEADER + "\n".join(bodies)
    out += "\n\n    public static void registerAll(SparkSession spark) {\n" + "\n".join(regs) + "\n    }\n}\n"
    if args.check:
        print(out); return
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, "GeneratedSpatioTemporalUDFs.java")
    with open(path, "w") as fh:
        fh.write(out)
    print("wrote %s (%d UDFs)" % (path, len(bodies)))


if __name__ == "__main__":
    main()
