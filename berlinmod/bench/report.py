#!/usr/bin/env python3
"""
BerlinMOD cross-platform benchmark report generator.

Reads JSON result files produced by bench_mbdb.sh, bench_mduck.sh, and
bench_mspark.sh, collects machine specifications, and writes a self-contained
markdown table.

Usage:
  report.py --results DIR [--output FILE]

Reads: DIR/mbdb.json, DIR/mduck.json, DIR/mspark.json  (any subset is fine)
Writes: FILE  (default: DIR/report.md)

To share your results with the MobilityDB community, paste the generated
markdown table as a comment on:
  https://github.com/MobilityDB/MobilityDB/discussions/913
"""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone


# Canonical query order and short descriptions
QUERY_LABELS = {
    "q01":  "Q1  — vehicle models (relational join)",
    "q02":  "Q2  — ever entered region (eIntersects)",
    "q03":  "Q3  — position at instant (atTime)",
    "q04":  "Q4  — ever passed point (eIntersects)",
    "q05":  "Q5  — min approach distance (nearestApproachDistance)",
    "q06":  "Q6  — truck pairs within 10 m (eDwithin)",
    "q07":  "Q7  — trip during period (atTime)",
    "q08":  "Q8  — trajectory geometry (trajectory)",
    "qrt":  "QRT — binary round-trip (asHexWKB)",
    "q09":  "Q9  — licence + region ever-intersect",
    "q10":  "Q10 — licence + point ever-intersect",
    "q11":  "Q11 — licence + period overlap",
    "q12":  "Q12 — vehicles ever in multiple regions",
    "q13":  "Q13 — pairs ever within 10 m",
    "q14":  "Q14 — vehicles with max speed > threshold",
    "q15":  "Q15 — distance travelled per vehicle",
    "q16":  "Q16 — vehicles present during each period",
    "q17":  "Q17 — aggregate: trips per vehicle type",
}


def median_ms(times: list[int]) -> float:
    return statistics.median(times)


def fmt_ms(ms: float) -> str:
    if ms < 1000:
        return f"{ms:.0f} ms"
    return f"{ms / 1000:.2f} s"


def collect_machine_spec() -> dict:
    spec: dict = {}

    # CPU model
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    spec["cpu"] = line.split(":")[1].strip()
                    break
    except OSError:
        spec["cpu"] = platform.processor() or "unknown"

    # Core / thread count
    try:
        cores = int(subprocess.check_output(
            ["nproc", "--all"], text=True).strip())
        physical = int(subprocess.check_output(
            "grep -c '^processor' /proc/cpuinfo", shell=True, text=True).strip())
        spec["threads"] = physical
        spec["cores"]   = cores // 2 if cores != physical else cores
    except Exception:
        spec["threads"] = os.cpu_count() or 1
        spec["cores"]   = spec["threads"]

    # RAM
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if "MemTotal" in line:
                    kb = int(line.split()[1])
                    spec["ram_gb"] = round(kb / 1024 / 1024, 1)
                    break
    except OSError:
        spec["ram_gb"] = "unknown"

    # OS
    try:
        uname = platform.uname()
        spec["os"] = f"{uname.system} {uname.release}"
        if "microsoft" in uname.release.lower():
            spec["os"] += " (WSL2)"
    except Exception:
        spec["os"] = platform.platform()

    # Python
    spec["python"] = platform.python_version()

    return spec


def load_results(results_dir: Path) -> dict[str, dict]:
    platforms = {}
    for fname, key in [("mbdb.json", "mobilitydb"),
                       ("mduck.json", "mobilityduck"),
                       ("mspark.json", "mobilityspark")]:
        path = results_dir / fname
        if path.exists():
            with open(path) as f:
                data = json.load(f)
            platforms[key] = data
    return platforms


def build_report(platforms: dict[str, dict], machine: dict) -> str:
    lines = []

    # Header
    lines.append("## BerlinMOD Portable SQL — Cross-Platform Benchmark")
    lines.append("")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"*Generated {now}*")
    lines.append("")

    # Machine specs
    lines.append("### Machine")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|---|---|")
    lines.append(f"| CPU | {machine.get('cpu', 'unknown')} |")
    cores   = machine.get('cores', '?')
    threads = machine.get('threads', '?')
    lines.append(f"| Cores / threads | {cores} cores / {threads} threads |")
    lines.append(f"| RAM | {machine.get('ram_gb', '?')} GB |")
    lines.append(f"| OS | {machine.get('os', 'unknown')} |")
    lines.append("")

    if not platforms:
        lines.append("*No result files found in the results directory.*")
        return "\n".join(lines)

    # Platform versions + data size
    lines.append("### Platforms")
    lines.append("")
    lines.append("| Platform | Version | Vehicles | Trips | Runs |")
    lines.append("|---|---|---|---|---|")
    DISPLAY = {
        "mobilitydb":    "MobilityDB",
        "mobilityduck":  "MobilityDuck",
        "mobilityspark": "MobilitySpark",
    }
    for key in ["mobilitydb", "mobilityduck", "mobilityspark"]:
        if key not in platforms:
            continue
        d = platforms[key]
        lines.append(
            f"| {DISPLAY[key]} | {d.get('version','?')} "
            f"| {d.get('data_vehicles','?')} "
            f"| {d.get('data_trips','?')} "
            f"| {d.get('runs','?')} |"
        )
    lines.append("")

    # Timing table
    present = [k for k in ["mobilitydb", "mobilityduck", "mobilityspark"]
               if k in platforms]
    header = "| Query | Description |" + "".join(
        f" {DISPLAY[k]} |" for k in present)
    lines.append("### Query Timings (median wall-clock)")
    lines.append("")
    lines.append(header)
    lines.append("|---|---|" + "---|" * len(present))

    all_queries = list(QUERY_LABELS.keys())
    for q in all_queries:
        # include row only if at least one platform has data for this query
        cells = {}
        for k in present:
            times = platforms[k].get("queries", {}).get(q)
            cells[k] = fmt_ms(median_ms(times)) if times else "—"
        if all(v == "—" for v in cells.values()):
            continue
        label = QUERY_LABELS.get(q, q)
        row = f"| `{q}` | {label} |"
        for k in present:
            row += f" {cells[k]} |"
        lines.append(row)

    lines.append("")

    # Notes
    lines.append("### Notes")
    lines.append("")
    lines.append("- All three platforms use **identical SQL** (no operator symbols — "
                 "named functions only per the portable dialect in Discussion #861).")
    lines.append("- Timings are wall-clock (client-side `date +%s%3N`), "
                 "median of N runs. Data loading is excluded.")
    lines.append("- MobilitySpark timings include Spark query planning overhead "
                 "but **not** JVM startup (all queries run in a single Spark session).")
    lines.append("- Queries marked `—` are not yet implemented on that platform.")
    lines.append("")
    lines.append("To share your results, paste this table as a comment on "
                 "https://github.com/MobilityDB/MobilityDB/discussions/913")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="BerlinMOD benchmark report generator")
    parser.add_argument("--results", default="results",
                        help="Directory containing mbdb.json / mduck.json / mspark.json")
    parser.add_argument("--output",  default=None,
                        help="Output markdown file (default: RESULTS/report.md)")
    args = parser.parse_args()

    results_dir = Path(args.results)
    output_path = Path(args.output) if args.output else results_dir / "report.md"

    platforms = load_results(results_dir)
    if not platforms:
        print(f"No result files found in {results_dir}/", file=sys.stderr)
        print("Run bench.sh (or individual bench_*.sh scripts) first.", file=sys.stderr)
        sys.exit(1)

    machine = collect_machine_spec()
    report  = build_report(platforms, machine)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report)
    print(f"Report written to {output_path}")


if __name__ == "__main__":
    main()
