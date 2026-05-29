<!--
Copyright(c) MobilityDB Contributors

This documentation is licensed under a
Creative Commons Attribution-Share Alike 3.0 License
https://creativecommons.org/licenses/by-sa/3.0/
-->

# MobilitySpark PR Reviewer Guide

Quick reference for anyone reviewing open pull requests in **MobilitySpark** and its JMEOS dependency.
Updated in the same commit as any PR that changes PR state or adds new branches.
**Last updated: 2026-05-11 — PR #9 extended to 86 UDFs (100% `meos_h3.h` parity) and the portable q02.sql polygon-side prefilter via MobilityDB PR #938 (`geo_to_h3index_set`). All three coordinated PRs source-complete (MobilityDB #938, MobilityDB-BerlinMOD #24, this PR #9); awaiting upstream th3index merge + JMEOS regen.**

---

## How to find this guide

- **In the repo:** `doc/contributing/reviewer-guide.md`
- **Rule:** every commit that opens, closes, or restructures a PR must update this file in the same commit. A one-liner status change is enough; a fuller rewrite is needed when the dependency graph changes.

---

## CI legend

| Symbol | Meaning |
|--------|---------|
| ✅ | All checks green |
| ❌ | Real failure — needs investigation before review |
| ⏳ | CI running |
| ❓ | No CI result yet (doc-only, draft, or external PR) |
| ⚠️ | Non-blocking failure (e.g. macOS/Windows `continue-on-error`, Codacy ACTION_REQUIRED — maintainer overrides in UI) |

---

## Dependency chains — land in this order

```
MobilityDB/MobilityDB
  PR #807 / #866 / #893  th3index temporal H3-cell index type
    └─► JMEOS regen against MEOS 1.4 master                       (parallel-session work)
          └─► MobilityDB/MobilitySpark
                PR #9   perf/spark-mt-and-binary  (th3index spatial prefilter — Stage-2 of perf)

MobilityDB/JMEOS
  PR #9  JashanReel:fix-tests-using-docker    (multi-module Maven layout; needs cleanup review)
    └─► PR #12  estebanzimanyi:fix/multimodule-with-split-interface  (split JNR-FFI → ARM64/macOS fix)
          └─► MobilityDB/MobilitySpark
                PR #7   fix/license-main-java  (multi-platform CI bootstrap — stacks on JMEOS above)
                  └─► PR #5  feat/jmeos-1.3-berlinmod-poc  (JMEOS 1.4 + BerlinMOD + 99.3% parity)
                  └─► PR #8  doc/reviewer-guide              (this guide — independent, mergeable any time)
```

**PR #11** (`estebanzimanyi:fix/split-meos-library-interface`) was superseded by
`fix/multimodule-with-split-interface` (#12) and **closed 2026-05-10** as part of the
MobilitySpark consolidation pass.

**PR #8** (`SachaDelsaux:JMEOS_v1.3`) is subsumed by PR #9 — external author; recommended for
closure but left in place pending author signal.

---

## Tier 1 — Merge immediately (visibility / trivially reviewable)

| Repo | PR | Branch | Description | CI |
|------|----|--------|-------------|----|
| MobilitySpark | #8 | `doc/reviewer-guide` | **This guide** — visibility wiring (PR template + README banner) — single commit | ✅ |
| JMEOS | #8 | `SachaDelsaux:JMEOS_v1.3` | Subsumed by #9 — recommended for closure (external author; comment posted, awaiting signal) | ❓ |

---

## Tier 0 — Performance (cross-platform th3index unification)

Cross-platform unification: all three benchmarked platforms (MobilityDB, MobilityDuck, MobilitySpark) execute identical portable SQL with the th3index spatial prefilter. PostgreSQL/MobilityDB additionally gets GiST + SP-GiST indexes — the native machinery that the columnar prefilter simulates on the other two platforms. **Land MobilityDB #807 / #866 / #893 / #938 first (foundation + polygon-side public API); then MobilityDB-BerlinMOD #24 (data side); then this PR #9 (consumer side).**

| Repo | PR | Branch | Description | CI | Notes |
|------|----|--------|-------------|----|-------|
| MobilityDB | **#938** | `estebanzimanyi:feat/h3-static-geo-coverage` | Static-geometry → H3 cell set public API: `h3_gs_point_to_cell` (promoted to public) + `geo_to_h3index_set` (POINT/LINESTRING/POLYGON/MULTI*/GeometryCollection in one walker) + `ever_eq_anyof_h3indexset_th3index`. PG SQL wrappers. pg_regress fixtures verified locally on PG 17 build (15/19 pass; 2 intentional ERROR; 2 wait on a pre-existing th3index branch SRID-resolver gap). | ❓ | Stacks on MobilityDB #866. Unblocks the polygon-side BerlinMOD prefilter consumed by this PR's q02.sql. |
| MobilityDB-BerlinMOD | **#24** | `estebanzimanyi:feat/portability-export-th3index` | Extend `berlinmod_portability_export()` to write a 4-column `trips.csv` including `trip_h3` (th3index hex-WKB at H3 resolution 7). Single SQL function change. | ❓ | Data-side, depends on `tgeompoint_to_th3index` from MobilityDB #807 / #866. |
| MobilitySpark | **#9** | `estebanzimanyi:perf/spark-mt-and-binary` | Cross-platform th3index unification: 86 UDFs (Th3IndexUDFs at 100% parity to `meos_h3.h`); load-time `trip_h3` materialisation; portable BerlinMOD SQL (q02/q04/q05/q06/q10) adopts the prefilter directly — including the polygon-side Q2 path via MobilityDB #938's `geoToH3IndexSet` + `everIntersectsH3IndexSet_Th3Index`; load_mbdb.sql adds GiST + SP-GiST indexes; load_mduck.sql + setup/generate_data.sh updated. `preprocessForSpark`'s th3index injection rules removed (now redundant with portable SQL). | ❌ | Source-complete; CI builds will fail until MobilityDB #807 / #866 / #893 / #938 land + JMEOS regen exposes the new symbols (parallel-session `feat/regen-against-meos-1.4`) + MobilityDuck th3index port lands. |

---

## Tier 2 — Cross-repo chain (stack in order: JMEOS → MobilitySpark)

| Repo | PR | Branch | Description | CI | Notes |
|------|----|--------|-------------|----|-------|
| JMEOS | #9 | `JashanReel:fix-tests-using-docker` | Multi-module Maven layout (`jmeos-core/`); MEOS 1.3 API; test fixes | ❓ | **Land first** — needs cleanup (IDE files, binary blobs, *.class, squash 78 commits) |
| JMEOS | #12 | `estebanzimanyi:fix/multimodule-with-split-interface` | Split `MeosLibrary` 1685-method interface into 4 `public static` sub-interfaces for JNR-FFI; removes binary blobs + `.class` + debug files; updates `.gitignore` | ❓ | Stacks on #9 |
| MobilitySpark | #7 | `estebanzimanyi:fix/license-main-java` | Multi-platform CI bootstrap: license check, compile, 51 unit tests on Linux; macOS/Windows non-blocking | ✅⚠️ | Stacks on JMEOS #12 |
| MobilitySpark | #5 | `estebanzimanyi:feat/jmeos-1.3-berlinmod-poc` | JMEOS 1.3 + BerlinMOD Q1–Q17 + TemporalParquet edge-to-cloud pipeline; 37/37 tests | ✅ | Stacks on #7 |

---

## Tier 3 — Fork staging (awaiting upstream merge)

These PRs exist on the fork and are awaiting upstream review after the Tier 2 chain merges.

| Repo | PR | Branch | Description | CI | Notes |
|------|----|--------|-------------|----|-------|
| MobilitySpark fork (`estebanzimanyi/MobilitySpark`) | #1 | `feat/udf-parity-phase2` | Expand UDF surface: 141 new UDFs in 7 classes + JMEOS-1.5 sub-interface fix; 203/203 tests | ✅ | Depends on JMEOS #11 being merged upstream |

### UDF breakdown for fork PR #1

| Class | UDFs | Summary |
|-------|------|---------|
| `ConstructorUDFs` | 18 | Text-literal constructors for temporal/span/set/box types |
| `AccessorUDFs` | 25 | start/end/min/max value, shift, scale, atSpan, atSpanset |
| `SpanAlgebraUDFs` | 23 | Span+set topology predicates + algebraic operations |
| `AnalyticsUDFs` | 12 | tfloat math, tnumber integral/twavg, tpoint length/speed/azimuth/direction |
| `PredicateUDFs` | 36 | Temporal order comparisons + ever/always lifting |
| `OutputUDFs` | 15 | Text serialisation, metadata, instant/sequence navigation |
| `TemporalLiftingUDFs` | 30 | Lifted comparisons (tbool), arithmetic, deltaValue, tprecision, tsample |

Total: 40 pre-existing + 141 new = **181 UDFs**.

---

## Review checklist

For every MobilitySpark / JMEOS PR, verify:

- [ ] PostgreSQL License header on every `.java` file
- [ ] `meos_initialize()` in `@BeforeAll`; **no** `meos_finalize()` in `@AfterAll`
- [ ] Surefire `forkCount=1 reuseForks=false` preserved in `pom.xml`
- [ ] New UDF: `null` input → `null` output (STRICT semantics)
- [ ] New UDF: `DBL_MAX` MEOS return → `null` Java return (NAD sentinel)
- [ ] No large binary data files (> 10 MB) in the commit
- [ ] CI green on Linux before requesting merge
