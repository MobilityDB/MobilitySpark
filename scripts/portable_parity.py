#!/usr/bin/env python3
"""Portable bare-name parity gate for MobilitySpark.

The MobilitySpark analogue of MobilityDB/MEOS-API `portable_parity.py`
(and of MobilityDB `tools/portable_aliases/generate.py --check`). Same
prefix logic, applied to the set of Spark-SQL UDF names MobilitySpark
registers.

Single source of truth: the 29 operator -> bareName pairs in
`meta/portable-aliases.json` (vendored read-only from
MobilityDB/MEOS-API PR #8 / RFC #920). For every canonical bare name a
registered UDF *backs* it iff some registered name equals the bare name
or starts with `bareName + "_"`; failing that, the verified
`explicitBacking` C-family prefixes are tried (e.g.
`nearestApproachDistance` -> `nad`). A bare name with no match is
`needs-explicit-backing` (unbacked) — the precise worklist, never a
fabricated verdict.

Done = 0 unbacked = 29/29 = 100%, across all six type families
(temporal, geo, cbuffer, npoint, pose, rgeo) — none excluded.

    python3 scripts/portable_parity.py            # gate (exit 1 if unbacked)
    python3 scripts/portable_parity.py --out FILE  # also write JSON report

Usage:
    python3 scripts/portable_parity.py \\
        --mspark . --contract meta/portable-aliases.json
"""

import argparse
import glob
import json
import os
import re
import sys


REGISTER_RE = re.compile(r'\.udf\(\)\.register\(\s*"([A-Za-z0-9_]+)"')


def registered_udf_names(mspark_root):
    """Every name passed to spark.udf().register("name", ...) in src/main."""
    names = set()
    pat = os.path.join(mspark_root, "src", "main", "java", "**", "*.java")
    for path in glob.glob(pat, recursive=True):
        with open(path, encoding="utf-8") as fh:
            names.update(REGISTER_RE.findall(fh.read()))
    return names


def build_parity(contract, names):
    fam_of = {p["bareName"]: (fam, p["operator"])
              for fam, lst in contract["families"].items() for p in lst}
    explicit = contract.get("explicitBacking", {})

    def matches(prefix):
        return sorted(n for n in names
                      if n == prefix or n.startswith(prefix + "_"))

    by_bare = {}
    for bare, (fam, op) in sorted(fam_of.items()):
        hits, via = matches(bare), "prefix"
        if not hits:
            for pref in explicit.get(bare, []):
                hits += matches(pref)
            via = "explicit" if hits else None
        by_bare[bare] = {
            "operator": op, "family": fam, "via": via,
            "backedBy": len(hits), "sample": hits[:3],
            "status": "backed" if hits else "needs-explicit-backing",
        }
    backed = [b for b, v in by_bare.items() if v["status"] == "backed"]
    unbacked = sorted(b for b, v in by_bare.items()
                      if v["status"] == "needs-explicit-backing")
    total = len(by_bare)
    return {
        "total": total,
        "backed": len(backed),
        "needsExplicitBacking": len(unbacked),
        "parityPct": round(len(backed) * 100 / total, 1) if total else 0,
        "unbacked": unbacked,
        "byBareName": by_bare,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mspark", default=".",
                    help="MobilitySpark repo root (default: .)")
    ap.add_argument("--contract", default="meta/portable-aliases.json",
                    help="path to vendored portable-aliases.json")
    ap.add_argument("--out", default=None,
                    help="optional JSON report path")
    args = ap.parse_args()

    contract_path = (args.contract if os.path.isabs(args.contract)
                     else os.path.join(args.mspark, args.contract))
    with open(contract_path, encoding="utf-8") as fh:
        contract = json.load(fh)

    names = registered_udf_names(args.mspark)
    rep = build_parity(contract, names)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=2)

    print(f"[portable-parity] {rep['backed']}/{rep['total']} canonical bare "
          f"names backed by a registered UDF ({rep['parityPct']}%); "
          f"{rep['needsExplicitBacking']} unbacked", file=sys.stderr)
    for b in rep["unbacked"]:
        v = rep["byBareName"][b]
        print(f"  UNBACKED: {b!r}  ({v['operator']}, {v['family']})",
              file=sys.stderr)

    if rep["needsExplicitBacking"]:
        print("CHECK: FAIL — portable bare-name parity < 100%",
              file=sys.stderr)
        return 1
    print(f"CHECK: PASS — {rep['total']}/{rep['total']} bare names backed, "
          "0 unbacked, all six families", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
