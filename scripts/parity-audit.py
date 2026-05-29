#!/usr/bin/env python3
"""Audit the MobilityDB SQL surface against the registered MobilitySpark UDFs.

Scope and method:

1. **Aggregates are in scope.** MobilityDB declares its user-facing aggregates
   with ``CREATE AGGREGATE`` (tcount, tsum, tavg, extent, merge, setUnion,
   the windowed w* family, ...). They are parsed and counted, and checked
   against the registered Spark UDAFs.

2. **Comparison / ordering operators are in scope.** ``_eq/_ne/_lt/_le/_gt/
   _ge/_cmp/_hash/_hash_extended`` are the btree/hash opclass support functions
   and back the ``= <> < <= > >=`` operators -- user-facing, not PG-only.

3. **One tier per name.** Each MobilityDB name is classified into exactly one
   tier, reported separately, so a single bare-name UDF (or a substring match
   against an unrelated UDF) is never silently counted as full coverage:

     EXACT      camel/snake name registered verbatim (the honest floor)
     PREFIX     type-dispatch overload-split (abs <-> tnumberAbs)
     SUFFIX     overload-split (always_eq <-> alwaysEqTintInt)
     BARENAME   covered only by an exact RFC#920 bare-name UDF
                (temporal_overlaps <-> overlaps) -- the per-type behaviour is
                NOT verified by this name audit; flagged, not assumed
     WRAPPER    a verb that is only a SUBSTRING of some unrelated UDF -- almost
                certainly NOT real coverage (reported as a hallucination)
     MISSING    no registered UDF

Genuinely PG-only helpers stay out of scope: GiST/SPGiST/GIN index opclasses,
``_in/_out/_recv/_send`` text/binary I/O, ``_transfn/_combinefn/_finalfn/
_serialize/_deserialize`` aggregate plumbing, ``_sel/_joinsel/_supportfn/
_analyze`` planner hooks, typmod helpers, the oid cache, and PG geometric
constructors.

Usage:
    python3 scripts/parity-audit.py --mdb ../MobilityDB --mspark . \\
        --out docs/parity-status.md
"""

import argparse
import collections
import glob
import os
import re
import sys
from datetime import date


# Whole SQL sections that are PG-only (no Spark equivalent exists).
OUT_OF_SCOPE_SECTIONS = {
    "temporal/011_span_indexes.in.sql", "temporal/012_spanset_indexes.in.sql",
    "temporal/013_set_indexes.in.sql", "temporal/019_geo_constructors.in.sql",
    "temporal/043_temporal_gist.in.sql", "temporal/044_temporal_spgist.in.sql",
    "temporal/999_oid_cache.in.sql", "geo/073_tgeo_gist.in.sql",
    "geo/073_tpoint_gist.in.sql", "geo/074_tgeo_spgist.in.sql",
    "geo/074_tpoint_spgist.in.sql", "cbuffer/166_tcbuffer_indexes.in.sql",
    "npoint/092_tnpoint_gin.in.sql", "npoint/098_tnpoint_indexes.in.sql",
    "pose/114_tpose_indexes.in.sql", "rgeo/134_trgeo_indexes.in.sql",
}

# PG-only helper suffixes (text/binary I/O, aggregate plumbing, planner hooks).
# NOTE: the comparison/ordering suffixes are NOT here -- they are in scope.
OOS_SUFFIXES = (
    "_transfn", "_combinefn", "_finalfn", "_serialize", "_deserialize",
    "_sel", "_joinsel", "_supportfn", "_analyze", "_typmod_in", "_typmod_out",
    "_in", "_out", "_recv", "_send",
)
# Comparison / ordering operator-class support functions -- IN scope.
CMP_SUFFIXES = ("_cmp", "_eq", "_ne", "_lt", "_le", "_gt", "_ge",
                "_hash", "_hash_extended")

# PG built-ins / platform bridges with no portable equivalent.
OUT_OF_SCOPE_NAMES = frozenset({"range", "multirange", "create_trip",
                                "transform_gk"})


def is_oos_name(fname):
    low = fname.lower()
    if low in OUT_OF_SCOPE_NAMES:
        return True
    return any(low.endswith(s) and len(low) > len(s) for s in OOS_SUFFIXES)


def is_cmp_name(fname):
    low = fname.lower()
    return any(low.endswith(s) and len(low) > len(s) for s in CMP_SUFFIXES)


def snake_to_camel(name):
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:]) if parts else name


CREATE_FUNC_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\(", re.IGNORECASE | re.DOTALL)
CREATE_AGG_RE = re.compile(r"CREATE\s+AGGREGATE\s+(\w+)\s*\(", re.IGNORECASE | re.DOTALL)
CREATE_OP_RE = re.compile(r"CREATE\s+OPERATOR\s+(\S+)\s*\(", re.IGNORECASE)
REGISTER_RE = re.compile(r'spark\.udf\(\)\.register\s*\(\s*"([^"]+)"')

TYPE_PREFIXES = (
    "tnumber", "tpoint", "tgeompoint", "tgeogpoint", "tgeo", "tgeometry",
    "tgeography", "tfloat", "tint", "tbool", "ttext", "tbox", "stbox", "span",
    "spanset", "set", "tcbuffer", "tnpoint", "tpose", "trgeo", "temporal",
    "geo", "geom", "geog",
)
TYPE_SUFFIXES = (
    "tintint", "tfloatfloat", "tboolbool", "ttexttext", "tnumbertnumber",
    "ttempt", "temporaltemporal", "tgeotgeo", "tgeogeo", "tgeotgeompoint",
    "tpointtpoint", "tpointgeo", "tpointpoint", "tcbuffertcbuffer",
    "tcbuffergeo", "tnpointtnpoint", "tnpointgeo", "tposetpose", "tposegeo",
    "trgeotrgeo", "trgeogeo", "intint", "floatfloat", "textstring", "boolbool",
    "tboxtbox", "stboxstbox", "tinttbox", "tboxtint", "tfloattbox",
    "tboxtfloat", "tnumbertbox", "tboxtnumber", "tspatialstbox",
    "stboxtspatial", "spanspan", "spansetspanset", "spansetspan",
    "spanspanset", "setset", "tint", "tfloat", "tbool", "ttext", "ttempt",
    "temporal", "tgeo", "tpoint", "tcbuffer", "tnpoint", "tpose", "trgeo",
    "tbox", "stbox", "span", "spanset", "set", "geo", "geom", "geog", "point",
    "int", "float", "text", "bool",
)
WRAPPER_PREFIXES = ("temporal_", "tnumber_", "tspatial_", "tgeo_", "tpoint_")


def collect_mdb(mdb_root):
    sql_root = os.path.join(mdb_root, "mobilitydb", "sql")
    if not os.path.isdir(sql_root):
        sys.exit(f"MobilityDB SQL dir not found: {sql_root}")
    sec_funcs = collections.OrderedDict()
    sec_aggs = collections.OrderedDict()
    sec_ops = collections.OrderedDict()
    for section in sorted(os.listdir(sql_root)):
        full = os.path.join(sql_root, section)
        if not os.path.isdir(full):
            continue
        for sql in sorted(glob.glob(f"{full}/*.in.sql")):
            rel = os.path.relpath(sql, sql_root)
            text = open(sql).read()
            sec_funcs[rel] = sorted(set(m.group(1) for m in CREATE_FUNC_RE.finditer(text)))
            sec_aggs[rel] = sorted(set(m.group(1) for m in CREATE_AGG_RE.finditer(text)))
            sec_ops[rel] = len(CREATE_OP_RE.findall(text))
    return sec_funcs, sec_aggs, sec_ops


def collect_mspark(mspark_root):
    src_root = os.path.join(mspark_root, "src", "main", "java")
    if not os.path.isdir(src_root):
        sys.exit(f"MobilitySpark src dir not found: {src_root}")
    funcs = set()
    for java in glob.glob(f"{src_root}/**/*.java", recursive=True):
        text = open(java, errors="replace").read()
        for m in REGISTER_RE.finditer(text):
            funcs.add(m.group(1))
    return funcs


def build_classifier(ms_funcs):
    ms_lower = {k.lower() for k in ms_funcs}
    ms_list = sorted(ms_lower, key=len, reverse=True)
    loose = set(ms_lower)
    for k in ms_lower:
        for pfx in TYPE_PREFIXES:
            if k.startswith(pfx) and len(k) > len(pfx):
                loose.add(k[len(pfx):])

    def classify(mdb):
        camel = snake_to_camel(mdb).lower()
        bare = mdb.lower()
        if bare in ms_lower or camel in ms_lower:
            return "EXACT"
        if bare in loose or camel in loose:
            return "PREFIX"
        for udf in ms_list:
            if udf.startswith(camel) and len(udf) > len(camel) \
                    and udf[len(camel):] in TYPE_SUFFIXES:
                return "SUFFIX"
        for w in WRAPPER_PREFIXES:
            if bare.startswith(w) and len(bare) > len(w):
                verb = bare[len(w):]
                if len(verb) >= 3:
                    if verb in ms_lower:
                        return "BARENAME"
                    if any(verb in udf for udf in ms_list):
                        return "WRAPPER"
        return "MISSING"

    return classify


def pct(n, d):
    return f"{100 * n / d:.1f}%" if d else "-"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mdb", default="../MobilityDB")
    ap.add_argument("--mspark", default=".")
    ap.add_argument("--out", default="docs/parity-status.md")
    args = ap.parse_args()

    sec_funcs, sec_aggs, sec_ops = collect_mdb(args.mdb)
    ms_funcs = collect_mspark(args.mspark)
    classify = build_classifier(ms_funcs)

    rows = []
    tot = collections.Counter()
    addressable_total = 0
    missing_all = []
    wrapper_all = []
    for sec, funcs in sec_funcs.items():
        if sec in OUT_OF_SCOPE_SECTIONS:
            continue
        tiers = collections.Counter()
        oos = 0
        for f in funcs:
            if is_oos_name(f) and not is_cmp_name(f):
                oos += 1
                continue
            t = classify(f)
            tiers[t] += 1
            tot[t] += 1
            addressable_total += 1
            if t == "MISSING":
                missing_all.append((sec, f))
            if t == "WRAPPER":
                wrapper_all.append((sec, f))
        rows.append((sec, sum(tiers.values()), tiers, sec_ops[sec], oos))

    agg_names = sorted({a for v in sec_aggs.values() for a in v})
    agg_tiers = collections.Counter()
    agg_missing = []
    for a in agg_names:
        t = classify(a)
        agg_tiers[t] += 1
        if t in ("WRAPPER", "MISSING"):
            agg_missing.append(a)

    exact = tot["EXACT"]
    pref = tot["PREFIX"] + tot["SUFFIX"]
    bn = tot["BARENAME"]
    wrap = tot["WRAPPER"]
    miss = tot["MISSING"]
    name_backed = exact + pref
    with_bn = name_backed + bn

    L = []
    L.append("# MobilitySpark parity status - surface audit")
    L.append("")
    L.append(f"Generated {date.today().isoformat()} against the MobilityDB SQL "
             "surface (master + the portable-aliases / RFC #920 work).")
    L.append("")
    L.append("## Headline (CREATE FUNCTION surface, comparison operators included)")
    L.append("")
    L.append(f"- Addressable function names: **{addressable_total}**")
    L.append(f"- **Exact name match: {exact} ({pct(exact, addressable_total)})** - the exact-match floor")
    L.append(f"- + type-dispatch / overload-split heuristics: {name_backed} "
             f"({pct(name_backed, addressable_total)}) - name-backed, no type-agnostic assumption")
    L.append(f"- + RFC #920 bare-name dispatch (per-type behaviour NOT verified by this audit): "
             f"{with_bn} ({pct(with_bn, addressable_total)})")
    L.append(f"- Hallucinated (verb only a substring of an unrelated UDF): {wrap}")
    L.append(f"- Genuinely missing: **{miss} ({pct(miss, addressable_total)})**")
    L.append("")
    L.append("## Aggregates (CREATE AGGREGATE - invisible to a CREATE FUNCTION-only audit)")
    L.append("")
    L.append(f"- Aggregate names: **{len(agg_names)}**; backed (exact/heuristic): "
             f"{agg_tiers['EXACT'] + agg_tiers['PREFIX'] + agg_tiers['SUFFIX']}; "
             f"not backed: {len(agg_missing)}")
    if agg_missing:
        L.append(f"- Not backed: {', '.join(agg_missing)} "
                 "(some are per-type-split or windowed; see the source for which are real gaps)")
    L.append("")
    L.append("## Methodology")
    L.append("")
    L.append("Parsed `CREATE FUNCTION` + `CREATE AGGREGATE` from "
             "`mobilitydb/sql/**/*.in.sql` and `spark.udf().register(\"name\", ...)` "
             "(scalar + UDAF) from `MobilitySpark/src/main/java/**/*.java`. Each MobilityDB "
             "name is placed in exactly one tier (EXACT / PREFIX / SUFFIX / BARENAME / WRAPPER "
             "/ MISSING - see the header of `scripts/parity-audit.py`). A name audit does NOT "
             "prove per-overload signature parity, and the BARENAME tier (covered only by a "
             "type-agnostic RFC #920 bare-name UDF) is flagged, not assumed correct per type. "
             "Comparison/ordering operators are counted; PG-only plumbing (index opclasses, "
             "`_in/_out/_recv/_send`, `_transfn/...`, planner hooks) is excluded.")
    L.append("")
    L.append("## Per-section coverage")
    L.append("")
    L.append("| Section | Addr | Exact | Heur | BareName? | Halluc | Missing | OOS | MDB ops |")
    L.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for sec, addr, tiers, ops, oos in rows:
        if addr == 0 and oos == 0:
            continue
        L.append(f"| `{sec}` | {addr} | {tiers['EXACT']} | "
                 f"{tiers['PREFIX'] + tiers['SUFFIX']} | {tiers['BARENAME']} | "
                 f"{tiers['WRAPPER']} | {tiers['MISSING']} | {oos} | {ops} |")
    L.append(f"| **TOTAL** | **{addressable_total}** | **{exact}** | **{pref}** | "
             f"**{bn}** | **{wrap}** | **{miss}** | | |")
    L.append("")
    L.append("## Genuinely missing function names")
    L.append("")
    by_fam = collections.defaultdict(list)
    for sec, f in missing_all:
        by_fam[sec.split('/', 1)[0]].append(f)
    for fam in sorted(by_fam):
        names = sorted(set(by_fam[fam]))
        L.append(f"- **{fam}** ({len(names)}): {', '.join(names)}")
    L.append("")
    if wrapper_all:
        L.append("## Hallucinated matches (substring only - treat as NOT covered)")
        L.append("")
        for sec, f in sorted(set(wrapper_all)):
            L.append(f"- `{f}` [{sec}]")
        L.append("")

    with open(args.out, "w") as fh:
        fh.write("\n".join(L) + "\n")
    print(f"Wrote {args.out}")
    print(f"Exact {exact}/{addressable_total} = {pct(exact, addressable_total)}; "
          f"name-backed {pct(name_backed, addressable_total)}; "
          f"with-bare-name {pct(with_bn, addressable_total)}; "
          f"missing {miss}; hallucinated {wrap}; aggregates {len(agg_names)} "
          f"({len(agg_missing)} not backed)")


if __name__ == "__main__":
    main()
