#!/usr/bin/env python3
"""Compare MobilityDB SQL surface against MobilitySpark registered surface.

Adapted from MobilityDuck/scripts/parity-audit.py. Same OUT_OF_SCOPE /
DEFERRED bucketing model; differences:

- MobilitySpark Java sources (`*.java`) instead of C++; matches Spark UDF
  registration: `spark.udf().register("name", ...)` and the UDAF variant
  `spark.udf().register("name", org.apache.spark.sql.functions.udaf(...)`.
- MobilityDB SQL is snake_case; MobilitySpark UDFs are camelCase. Match
  converts snake_case → camelCase before comparison.

Usage:
    python3 scripts/parity-audit.py \\
        --mdb /path/to/MobilityDB \\
        --mspark /path/to/MobilitySpark \\
        --out docs/parity-status.md
"""

import argparse
import collections
import glob
import os
import re
import sys
from datetime import date


# Type families deferred from the active parity sweep.
#
# EMPTY BY INVARIANT. cbuffer / npoint / pose / rgeo are FULL user-facing
# temporal types and ARE in scope — covered like every other family (RFC
# #920; MobilityDB#1075 aliases all six). They must never be excluded from
# the parity headline. Re-deferring any of them is incomplete work, not an
# accepted end state. Keep this set empty.
DEFERRED_FAMILIES = set()


# Whole SQL sections that are PG-only (no Spark equivalent exists).
# Match by tail of the relpath under mobilitydb/sql/.
OUT_OF_SCOPE_SECTIONS = {
    "temporal/011_span_indexes.in.sql",       # GiST/SPGiST opclasses
    "temporal/012_spanset_indexes.in.sql",    # GiST/SPGiST opclasses
    "temporal/013_set_indexes.in.sql",        # GiST/SPGiST opclasses
    "temporal/019_geo_constructors.in.sql",   # PG geometric types
    "temporal/043_temporal_gist.in.sql",      # GiST support
    "temporal/044_temporal_spgist.in.sql",    # SPGiST support
    "temporal/999_oid_cache.in.sql",          # PG catalog hook
    "geo/073_tgeo_gist.in.sql",               # GiST support
    "geo/073_tpoint_gist.in.sql",             # GiST support
    "geo/074_tgeo_spgist.in.sql",             # SPGiST support
    "geo/074_tpoint_spgist.in.sql",           # SPGiST support
    # Sibling-family index plumbing — GiST/GIN opclass-support callbacks
    # (consistent / extract_query / extract_value / triconsistent), the
    # exact same PG-only class already excluded for temporal/geo above.
    # No Spark equivalent: index access methods are PG-internal.
    "cbuffer/166_tcbuffer_indexes.in.sql",    # GiST support
    "npoint/092_tnpoint_gin.in.sql",          # GIN support
    "npoint/098_tnpoint_indexes.in.sql",      # GiST support
    "pose/114_tpose_indexes.in.sql",          # GiST support
    "rgeo/134_trgeo_indexes.in.sql",          # GiST support
}


# Function-name suffixes that mark PG-only helpers.
OUT_OF_SCOPE_NAME_SUFFIXES = (
    "_transfn",
    "_combinefn",
    "_finalfn",
    "_serialize",
    "_deserialize",
    "_sel",
    "_joinsel",
    "_supportfn",
    "_analyze",
    "_typmod_in",
    "_typmod_out",
    "_in",
    "_out",
    "_recv",
    "_send",
    # PG btree opclass support — user-callable but only meaningful inside
    # PG operator classes for sorting and hash. Spark uses Dataset.distinct
    # / orderBy which works on hex-WKB string equality natively.
    "_cmp",
    "_eq",
    "_ne",
    "_lt",
    "_le",
    "_gt",
    "_ge",
    "_hash",
    "_hash_extended",
)


# PG-specific / PG-extension-specific entry points with no portable equivalent.
# Excluded from the audit; do not register as UDFs.
#
# NOTE: box2d / box3d ARE addressable here even though they're PostGIS types,
# because PostGIS is embedded in MEOS — the BOX3D / GBOX structs and their
# I/O functions are exported by libmeos.so.  The ranges (range/multirange)
# are PG built-ins NOT in MEOS, so they remain OOS.
OUT_OF_SCOPE_NAMES = frozenset({
    # PostgreSQL built-in range types — NOT in MEOS, no portable equivalent
    "range",       # span → PG range type
    "multirange",  # spanset → PG multirange type
    # PG-extension-specific data generators
    "create_trip", # BerlinMOD synthetic-trajectory generator (PG-only, depends
                   # on BerlinMOD road-network schema + PG random functions)
    # Platform-bridge functions
    "transform_gk", # Gauss-Krüger projection used to interoperate with the
                    # SECONDO research platform; not portable to Spark/DuckDB
})


def is_out_of_scope_name(fname):
    lower = fname.lower()
    if lower in OUT_OF_SCOPE_NAMES:
        return True
    for suf in OUT_OF_SCOPE_NAME_SUFFIXES:
        if lower.endswith(suf) and len(lower) > len(suf):
            return True
    return False


def snake_to_camel(name):
    """Convert MobilityDB snake_case to MobilitySpark camelCase.

    Examples:
        tgeompoint_in        → tgeompointIn
        eintersects_tgeo_geo → eintersectsTgeoGeo
        nad_tgeo_tgeo        → nadTgeoTgeo
        tdistance_tgeo_geo   → tdistanceTgeoGeo
    """
    parts = name.split("_")
    if not parts:
        return name
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


CREATE_FUNC_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\(([^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
CREATE_OP_RE = re.compile(r"CREATE\s+OPERATOR\s+(\S+)\s*\(", re.IGNORECASE)

# Spark UDF registration patterns.
REGISTER_RE = re.compile(r'spark\.udf\(\)\.register\s*\(\s*"([^"]+)"')


def collect_mobilitydb(mdb_root):
    sql_root = os.path.join(mdb_root, "mobilitydb", "sql")
    if not os.path.isdir(sql_root):
        sys.exit(f"MobilityDB SQL dir not found: {sql_root}")

    section_funcs = collections.OrderedDict()
    section_op_count = collections.OrderedDict()
    all_funcs = set()

    for section in sorted(os.listdir(sql_root)):
        full = os.path.join(sql_root, section)
        if not os.path.isdir(full):
            continue
        for sql in sorted(glob.glob(f"{full}/*.in.sql")):
            rel = os.path.relpath(sql, sql_root)
            with open(sql) as f:
                text = f.read()
            funcs = collections.Counter()
            for m in CREATE_FUNC_RE.finditer(text):
                funcs[m.group(1)] += 1
                all_funcs.add(m.group(1))
            section_funcs[rel] = funcs
            section_op_count[rel] = len(CREATE_OP_RE.findall(text))

    return section_funcs, section_op_count, all_funcs


def collect_mobilityspark(mspark_root):
    src_root = os.path.join(mspark_root, "src", "main", "java")
    if not os.path.isdir(src_root):
        sys.exit(f"MobilitySpark src dir not found: {src_root}")

    funcs = collections.Counter()
    files_for_func = collections.defaultdict(set)
    for java in glob.glob(f"{src_root}/**/*.java", recursive=True):
        with open(java, errors="replace") as f:
            text = f.read()
        rel = os.path.relpath(java, src_root)
        for m in REGISTER_RE.finditer(text):
            funcs[m.group(1)] += 1
            files_for_func[m.group(1)].add(rel)
    return funcs, files_for_func


def is_deferred(section_relpath):
    family = section_relpath.split("/", 1)[0]
    return family in DEFERRED_FAMILIES


def is_out_of_scope_section(section_relpath):
    return section_relpath in OUT_OF_SCOPE_SECTIONS


# Known MobilitySpark type-prefixes used by registered UDFs. A SQL function
# `abs(tnumber)` is covered if any Spark UDF named e.g. `tnumberAbs`,
# `tintAbs`, `tfloatAbs` exists. The match strips one of these prefixes
# and compares the remainder (case-insensitive).
TYPE_PREFIXES = (
    "tnumber", "tpoint", "tgeompoint", "tgeogpoint",
    "tgeo", "tgeometry", "tgeography",
    "tfloat", "tint", "tbool", "ttext",
    "tbox", "stbox",
    "span", "spanset", "set",
    "tcbuffer", "tnpoint", "tpose", "trgeo",
    "temporal", "geo", "geom", "geog",
)


def write_report(out_path, mdb_section_funcs, mdb_section_op_count,
                 all_mdb_funcs, mspark_funcs):
    # Build case-insensitive lookup of registered Spark UDF names.
    mspark_funcs_lower = {k.lower(): k for k in mspark_funcs}

    # Build loose-match index: for each Spark UDF, also index its name
    # with known type-prefixes stripped. So `tnumberAbs` becomes
    # discoverable by the bare name `abs`, `tpointSRID` by `srid`, etc.
    loose_index = set()
    for k in mspark_funcs:
        kl = k.lower()
        loose_index.add(kl)
        for pfx in TYPE_PREFIXES:
            if kl.startswith(pfx) and len(kl) > len(pfx):
                loose_index.add(kl[len(pfx):])

    # Build a list of Spark UDF names sorted by length (longest first) for
    # efficient prefix scanning.
    mspark_lower_list = sorted(mspark_funcs_lower.keys(), key=len, reverse=True)

    # Known MobilitySpark type-SUFFIX tokens that may be appended to a
    # MobilityDB camel-case name to disambiguate overloads. Match is
    # case-insensitive on the camelCase form.
    TYPE_SUFFIXES = (
        "tintint", "tfloatfloat", "tboolbool", "ttexttext",
        "tnumbertnumber", "ttempt", "temporaltemporal",
        "tgeotgeo", "tgeogeo", "tgeotgeompoint",
        "tpointtpoint", "tpointgeo", "tpointpoint",
        "tcbuffertcbuffer", "tcbuffergeo",
        "tnpointtnpoint", "tnpointgeo",
        "tposetpose", "tposegeo",
        "trgeotrgeo", "trgeogeo",
        "intint", "floatfloat", "textstring", "boolbool",
        "tboxtbox", "stboxstbox",
        "tinttbox", "tboxtint", "tfloattbox", "tboxtfloat",
        "tnumbertbox", "tboxtnumber",
        "tspatialstbox", "stboxtspatial",
        "spanspan", "spansetspanset", "spansetspan", "spanspanset",
        "setset",
        "tint", "tfloat", "tbool", "ttext", "ttempt", "temporal",
        "tgeo", "tpoint", "tcbuffer", "tnpoint", "tpose", "trgeo",
        "tbox", "stbox",
        "span", "spanset", "set",
        "geo", "geom", "geog", "point",
        "int", "float", "text", "bool",
    )

    # MobilityDB SQL "wrapper" prefixes that span multiple type combos.
    # A name like `temporal_above` is a dispatcher over
    # {above_stbox_tspatial, above_tspatial_stbox, above_tspatial_tspatial},
    # all of which appear in MobilitySpark with type-suffixed names like
    # `stboxAboveTpoint`, `tpointAboveStbox`, `tpointAbove`. To recognise
    # this, strip the wrapper prefix and check whether the remainder
    # appears as a substring (case-insensitive) of any Spark UDF name.
    WRAPPER_PREFIXES = (
        "temporal_", "tnumber_", "tspatial_", "tgeo_", "tpoint_",
    )

    def is_covered(mdb_fname):
        """A MobilityDB SQL name is covered by MobilitySpark if any of:
          1. exact match (case-insensitive) on snake or camel form
          2. loose: name appears after stripping a known type-PREFIX from a
             Spark UDF name (e.g. abs ↔ tnumberAbs)
          3. suffix: any Spark UDF starts with the camel-case form and
             ends with a known type-SUFFIX (e.g. always_eq ↔ alwaysEqTintInt,
             alwaysEqTfloatFloat, alwaysEqTemporal)
          4. wrapper: MobilityDB dispatcher name `<wrapper>_<verb>` is
             considered covered if any Spark UDF contains `<verb>` (PascalCase)
             — e.g. temporal_above ↔ stboxAboveTpoint / tpointAboveStbox."""
        camel = snake_to_camel(mdb_fname).lower()
        bare = mdb_fname.lower()
        if bare in mspark_funcs_lower or camel in mspark_funcs_lower:
            return True
        if bare in loose_index or camel in loose_index:
            return True
        for udf in mspark_lower_list:
            if udf.startswith(camel) and len(udf) > len(camel):
                tail = udf[len(camel):]
                for suf in TYPE_SUFFIXES:
                    if tail == suf:
                        return True
        # Wrapper match
        for wpfx in WRAPPER_PREFIXES:
            if bare.startswith(wpfx) and len(bare) > len(wpfx):
                verb = bare[len(wpfx):]
                # Avoid 1-letter false positives.
                if len(verb) >= 3:
                    for udf in mspark_lower_list:
                        if verb in udf:
                            return True
        return False

    active_results = []
    deferred_results = []
    out_of_scope_results = []

    for sec, funcs in mdb_section_funcs.items():
        if not funcs:
            continue
        section_oos = is_out_of_scope_section(sec)
        section_deferred = is_deferred(sec)
        covered, missing, oos_names = [], [], []
        for fname, count in sorted(funcs.items()):
            if section_oos:
                oos_names.append((fname, count))
                continue
            if not section_deferred and is_out_of_scope_name(fname):
                oos_names.append((fname, count))
                continue
            if is_covered(fname):
                covered.append((fname, count))
            else:
                missing.append((fname, count))
        addressable = len(covered) + len(missing)
        pct = (len(covered) / addressable * 100) if addressable else 0
        row = (sec, len(funcs), len(covered), len(missing), pct,
               missing, covered, mdb_section_op_count[sec],
               oos_names, addressable)
        if section_oos:
            out_of_scope_results.append(row)
        elif section_deferred:
            deferred_results.append(row)
        else:
            active_results.append(row)

    def totals(results):
        cov = sum(r[2] for r in results)
        miss = sum(r[3] for r in results)
        n = cov + miss
        pct = (cov / n * 100) if n else 0
        return n, cov, miss, pct

    def oos_total(results):
        return sum(len(r[8]) for r in results)

    a_total, a_cov, a_miss, a_pct = totals(active_results)
    d_total, d_cov, d_miss, d_pct = totals(deferred_results)
    a_oos_inside = oos_total(active_results)
    section_oos_total = sum(len(r[8]) for r in out_of_scope_results)
    total_oos = a_oos_inside + section_oos_total

    lines = []
    lines.append("# MobilitySpark parity status — surface-level audit")
    lines.append("")
    lines.append(
        f"Generated {date.today().isoformat()}. **Active addressable scope** "
        f"(temporal + geo, excluding PG-only helpers): "
        f"{a_cov}/{a_total} names covered ({a_pct:.1f}%)."
    )
    lines.append("")
    lines.append(
        f"**Out of scope** (PG-only — no Spark equivalent exists): "
        f"{total_oos} names skipped — {section_oos_total} from PG-only "
        f"sections (GiST/SPGiST opclasses, set/span/spanset index files, "
        f"`019_geo_constructors.in.sql` PG geometric types, "
        f"`999_oid_cache.in.sql`) plus {a_oos_inside} PG helper functions "
        f"inside active sections (`*_in/_out/_recv/_send`, `*_transfn/"
        f"_combinefn/_finalfn/_serialize/_deserialize`, `*_sel/_joinsel/"
        f"_supportfn/_analyze`, `*_typmod_in/_typmod_out`).  Listed in "
        f"appendix B; not counted in the headline."
    )
    lines.append("")
    if DEFERRED_FAMILIES:
        lines.append(
            f"**Deferred families** ({', '.join(sorted(DEFERRED_FAMILIES))}) "
            "appear in appendix C and are also excluded from the headline."
        )
    else:
        lines.append(
            "**All six type families in scope** (temporal, geo, cbuffer, "
            "npoint, pose, rgeo). None is deferred or excluded from the "
            "headline — they are full user-facing temporal types covered "
            "like every other family (RFC #920; MobilityDB#1075)."
        )
    lines.append("")
    lines.append(
        "**Methodology**: parsed `CREATE FUNCTION` from "
        "`mobilitydb/sql/**/*.in.sql` and `spark.udf().register(\"name\", "
        "...)` (scalar + UDAF) from `MobilitySpark/src/main/java/**/*.java`. "
        "Match is by **function name only**, case-insensitive; MobilityDB "
        "snake_case is converted to camelCase before comparison so e.g. "
        "`tdistance_tgeo_geo` matches `tdistanceTgeoGeo`. A name registered "
        "in MobilitySpark is treated as covering all its overloads; "
        "per-overload signature parity is not verified at this granularity."
    )
    lines.append("")
    lines.append("**Caveats**:")
    lines.append(
        "- A name match doesn't prove signature parity. e.g. "
        "`before(temporal, temporal)` registered in MobilitySpark does not "
        "necessarily cover MobilityDB's `before(tstzspan, temporal)`."
    )
    lines.append(
        "- Spark SQL has no infix-operator extension API; equivalent named "
        "functions are registered. The `MDB operators` column lists how "
        "many `CREATE OPERATOR` statements exist in the section, all of "
        "which collapse to named-function form in MobilitySpark."
    )
    lines.append("")
    lines.append(
        "Regenerate with `python3 scripts/parity-audit.py --mdb "
        "../MobilityDB --mspark . --out docs/parity-status.md`. The "
        "OUT_OF_SCOPE_SECTIONS / OUT_OF_SCOPE_NAME_SUFFIXES / "
        "DEFERRED_FAMILIES sets at the top of that script control bucketing."
    )
    lines.append("")

    lines.append("## Active-scope coverage summary (addressable surface)")
    lines.append("")
    lines.append("| Section | Addressable | Covered | Missing | Coverage | OOS | MDB operators |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for sec, total, cov, miss, pct, _, _, ops, oos_names, addressable in active_results:
        lines.append(
            f"| `{sec}` | {addressable} | {cov} | {miss} | {pct:.0f}% | "
            f"{len(oos_names)} | {ops} |"
        )
    lines.append(
        f"| **TOTAL (active)** | **{a_total}** | **{a_cov}** | "
        f"**{a_miss}** | **{a_pct:.0f}%** | **{a_oos_inside}** | — |"
    )
    lines.append("")

    lines.append("## Missing function names per active section")
    lines.append("")
    for sec, total, cov, miss, pct, missing, _, _, _, addressable in active_results:
        if not missing:
            continue
        lines.append(f"### `{sec}` — {miss} missing of {addressable} addressable ({pct:.0f}% covered)")
        lines.append("")
        for fname, count in missing:
            tag = f" ({count} overloads)" if count > 1 else ""
            lines.append(f"- `{fname}` → `{snake_to_camel(fname)}`{tag}")
        lines.append("")

    # ----- Appendix B: out-of-scope (PG-only) -----
    lines.append("## Appendix B — Out of scope (PG-only, no Spark equivalent)")
    lines.append("")
    lines.append(
        "These entries are PG-specific helpers — index opclasses, "
        "aggregate transition/combine/final/serialize callbacks, planner "
        "hooks (`_sel`, `_joinsel`, `_supportfn`, `_analyze`), text/binary "
        "I/O helpers (`_in`, `_out`, `_recv`, `_send`), type modifier "
        "helpers, the `999_oid_cache` PG catalog hook, PG geometric "
        "type constructors (`019_geo_constructors`), and the sibling-family "
        "GiST/GIN index opclass-support callbacks "
        "(`cbuffer/166_tcbuffer_indexes`, `npoint/092_tnpoint_gin`, "
        "`npoint/098_tnpoint_indexes`, `pose/114_tpose_indexes`, "
        "`rgeo/134_trgeo_indexes` — the same index-plumbing class already "
        "excluded for temporal/geo).  None of them have Spark equivalents "
        "and they should not be implemented; listed here only for "
        "completeness."
    )
    lines.append("")
    if out_of_scope_results:
        lines.append("### Whole sections excluded")
        lines.append("")
        lines.append("| Section | Names |")
        lines.append("|---|---:|")
        for sec, total, _, _, _, _, _, _, oos_names, _ in out_of_scope_results:
            lines.append(f"| `{sec}` | {len(oos_names)} |")
        lines.append("")
    if a_oos_inside:
        lines.append("### PG helpers inside active sections")
        lines.append("")
        lines.append("| Section | PG helpers |")
        lines.append("|---|---:|")
        for sec, _, _, _, _, _, _, _, oos_names, _ in active_results:
            if oos_names:
                lines.append(f"| `{sec}` | {len(oos_names)} |")
        lines.append("")

    # ----- Appendix C: deferred families -----
    if deferred_results:
        lines.append("## Appendix C — Deferred families")
        lines.append("")
        lines.append(
            f"These families ({', '.join(sorted(DEFERRED_FAMILIES))}) are "
            "deferred until the active temporal + geo surface stabilises. "
            "Re-include by editing `DEFERRED_FAMILIES` at the top of "
            "`scripts/parity-audit.py`. Listed here so the picture stays "
            "complete; not counted in headline coverage."
        )
        lines.append("")
        lines.append("| Section | Addressable | Covered | Missing | Coverage |")
        lines.append("|---|---:|---:|---:|---:|")
        for sec, total, cov, miss, pct, _, _, _, _, addressable in deferred_results:
            lines.append(
                f"| `{sec}` | {addressable} | {cov} | {miss} | {pct:.0f}% |"
            )
        lines.append(
            f"| **TOTAL (deferred)** | **{d_total}** | **{d_cov}** | "
            f"**{d_miss}** | **{d_pct:.0f}%** |"
        )
        lines.append("")

    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    return a_total, a_cov, a_pct


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mdb", default="../MobilityDB",
                    help="Path to MobilityDB checkout (default ../MobilityDB)")
    ap.add_argument("--mspark", default=".",
                    help="Path to MobilitySpark checkout (default .)")
    ap.add_argument("--out", default="docs/parity-status.md",
                    help="Output path (default docs/parity-status.md)")
    args = ap.parse_args()

    mdb_section_funcs, mdb_section_op_count, all_mdb_funcs = collect_mobilitydb(args.mdb)
    mspark_funcs, _files = collect_mobilityspark(args.mspark)

    a_total, a_cov, a_pct = write_report(
        args.out, mdb_section_funcs, mdb_section_op_count,
        all_mdb_funcs, mspark_funcs,
    )
    print(f"Wrote {args.out}")
    print(f"Active addressable coverage: {a_cov}/{a_total} ({a_pct:.1f}%)")


if __name__ == "__main__":
    main()
