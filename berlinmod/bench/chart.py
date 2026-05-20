#!/usr/bin/env python3
"""
BerlinMOD cross-platform / cross-tier bar chart generator.

Reads the same JSON result files as report.py and emits a grouped bar
chart (queries × (platform, tier) configurations) as a PNG image.  The
output is publishable as-is in Discussions, README, or papers.

Tier-1 Spark results are shown separately from Tier 3 (PG / Duck only) so
the reader can see the contribution of platform-native indexing.

Usage:
  chart.py --results DIR [--output FILE.png] [--log] [--width N] [--height N]

Reads: DIR/mbdb.tier{1,2,3}.json, DIR/mduck.tier{1,2,3}.json,
       DIR/mspark.tier1.json  (any subset).  Falls back to legacy
       DIR/mbdb.json / mduck.json / mspark.json (treated as Tier 3 / Tier 1).
Writes: FILE.png  (default: DIR/chart.png).  Requires matplotlib.

Companion: report.py emits the same data as a markdown table.  Run both
to publish the full benchmark artefact.
"""

import argparse
import json
import statistics
import sys
from pathlib import Path


# Canonical query order — keep in sync with report.py
QUERY_ORDER = [
    "q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "qrt",
    "q09", "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q17",
]

# Per-platform colour family.  Tier shading inside each family
# (Tier 1 = lightest, Tier 3 = darkest) so the eye groups by platform
# first, tier second.
PLATFORM_COLORS = {
    "mobilitydb":    {1: "#9ec3ff", 2: "#4d8ff5", 3: "#0f4ec9"},
    "mobilityduck":  {1: "#ffd28a", 2: "#f7a942", 3: "#c47000"},
    "mobilityspark": {1: "#a3d9a5", 2: "#5fb764", 3: "#1f7a23"},
}

PLATFORM_DISPLAY = {
    "mobilitydb":    "MobilityDB",
    "mobilityduck":  "MobilityDuck",
    "mobilityspark": "MobilitySpark",
}


def load_results(results_dir: Path) -> dict:
    """Same loader contract as report.load_results — duplicated here so
    chart.py has no Python import of report.py (keeps the two tools
    independent)."""
    platforms = {}
    for prefix, key in [("mbdb", "mobilitydb"),
                        ("mduck", "mobilityduck"),
                        ("mspark", "mobilityspark")]:
        tier_files = sorted(results_dir.glob(f"{prefix}.tier*.json"))
        if tier_files:
            tiered = {}
            for path in tier_files:
                with open(path) as f:
                    data = json.load(f)
                tier = int(data.get("tier", 3))
                tiered[tier] = data
            platforms[key] = tiered
            continue
        path = results_dir / f"{prefix}.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
            tier = int(data.get("tier", 3))
            platforms[key] = {tier: data}
    return platforms


def median_ms(times: list[int]) -> float:
    return statistics.median(times)


def build_chart(platforms: dict, output_path: Path,
                width: float, height: float, log: bool) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: chart.py requires matplotlib.  Install with: "
              "pip install matplotlib", file=sys.stderr)
        sys.exit(2)
    import numpy as np

    # Build the list of (platform, tier) columns in stable order so the
    # bar groups read MobilityDB → MobilityDuck → MobilitySpark, and within
    # each platform Tier 1 → Tier 2 → Tier 3.
    columns = []
    for key in ["mobilitydb", "mobilityduck", "mobilityspark"]:
        if key not in platforms:
            continue
        for tier in sorted(platforms[key].keys()):
            columns.append((key, tier))

    if not columns:
        print("ERROR: no result data found — nothing to chart.", file=sys.stderr)
        sys.exit(1)

    # Per-query median timing for each column
    # Only include queries that have at least one column with data
    queries_with_data = []
    series = {col: [] for col in columns}
    for q in QUERY_ORDER:
        any_data = False
        for col in columns:
            key, tier = col
            times = platforms[key][tier].get("queries", {}).get(q)
            if times:
                series[col].append(median_ms(times))
                any_data = True
            else:
                series[col].append(float("nan"))
        if any_data:
            queries_with_data.append(q)
        else:
            for col in columns:
                series[col].pop()

    n_queries = len(queries_with_data)
    n_columns = len(columns)
    bar_w = 0.8 / max(n_columns, 1)
    x = np.arange(n_queries)

    fig, ax = plt.subplots(figsize=(width, height))

    for i, col in enumerate(columns):
        key, tier = col
        offsets = x - 0.4 + (i + 0.5) * bar_w
        color = PLATFORM_COLORS[key][tier]
        label = f"{PLATFORM_DISPLAY[key]} T{tier}"
        # Only show platform name once per platform if all tiers present
        ax.bar(offsets, series[col], bar_w, label=label, color=color,
               edgecolor="white", linewidth=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in queries_with_data], rotation=45,
                       ha="right")
    ax.set_ylabel("Median wall-clock (ms)"
                  + (" — log scale" if log else ""))
    ax.set_title("BerlinMOD Portable SQL — Cross-Platform Benchmark")
    ax.set_axisbelow(True)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    if log:
        ax.set_yscale("log")
    ax.legend(loc="best", ncol=min(n_columns, 3), fontsize="small",
              framealpha=0.85)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    print(f"Chart written to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="BerlinMOD benchmark bar chart generator")
    parser.add_argument("--results", default="results",
                        help="Directory containing mbdb/mduck/mspark JSON files")
    parser.add_argument("--output", default=None,
                        help="Output PNG path (default: <results>/chart.png)")
    parser.add_argument("--log", action="store_true",
                        help="Use logarithmic y-axis "
                             "(useful when Spark NxN queries dominate the scale)")
    parser.add_argument("--width", type=float, default=14.0,
                        help="Figure width in inches (default: 14)")
    parser.add_argument("--height", type=float, default=6.0,
                        help="Figure height in inches (default: 6)")
    args = parser.parse_args()

    results_dir = Path(args.results)
    output_path = Path(args.output) if args.output else results_dir / "chart.png"

    platforms = load_results(results_dir)
    build_chart(platforms, output_path, args.width, args.height, args.log)


if __name__ == "__main__":
    main()
