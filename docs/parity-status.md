# MobilitySpark parity status — surface-level audit

Generated 2026-05-10. **Active addressable scope** (temporal + geo, excluding PG-only helpers): 858/858 names covered (100.0%).

**Out of scope** (PG-only — no Spark equivalent exists): 405 names skipped — 84 from PG-only sections (GiST/SPGiST opclasses, set/span/spanset index files, `019_geo_constructors.in.sql` PG geometric types, `999_oid_cache.in.sql`) plus 321 PG helper functions inside active sections (`*_in/_out/_recv/_send`, `*_transfn/_combinefn/_finalfn/_serialize/_deserialize`, `*_sel/_joinsel/_supportfn/_analyze`, `*_typmod_in/_typmod_out`).  Listed in appendix B; not counted in the headline.

**Deferred families** (cbuffer, npoint, pose, rgeo) appear in appendix C and are also excluded from the headline.

**Methodology**: parsed `CREATE FUNCTION` from `mobilitydb/sql/**/*.in.sql` and `spark.udf().register("name", ...)` (scalar + UDAF) from `MobilitySpark/src/main/java/**/*.java`. Match is by **function name only**, case-insensitive; MobilityDB snake_case is converted to camelCase before comparison so e.g. `tdistance_tgeo_geo` matches `tdistanceTgeoGeo`. A name registered in MobilitySpark is treated as covering all its overloads; per-overload signature parity is not verified at this granularity.

**Caveats**:
- A name match doesn't prove signature parity. e.g. `before(temporal, temporal)` registered in MobilitySpark does not necessarily cover MobilityDB's `before(tstzspan, temporal)`.
- Spark SQL has no infix-operator extension API; equivalent named functions are registered. The `MDB operators` column lists how many `CREATE OPERATOR` statements exist in the section, all of which collapse to named-function form in MobilitySpark.

Regenerate with `python3 scripts/parity-audit.py --mdb ../MobilityDB --mspark . --out docs/parity-status.md`. The OUT_OF_SCOPE_SECTIONS / OUT_OF_SCOPE_NAME_SUFFIXES / DEFERRED_FAMILIES sets at the top of that script control bucketing.

## Active-scope coverage summary (addressable surface)

| Section | Addressable | Covered | Missing | Coverage | OOS | MDB operators |
|---|---:|---:|---:|---:|---:|---:|
| `geo/050_geoset.in.sql` | 34 | 34 | 0 | 100% | 22 | 46 |
| `geo/051_stbox.in.sql` | 66 | 66 | 0 | 100% | 17 | 29 |
| `geo/052_tgeo.in.sql` | 62 | 62 | 0 | 100% | 18 | 12 |
| `geo/052_tpoint.in.sql` | 62 | 62 | 0 | 100% | 16 | 12 |
| `geo/053_tgeo_inout.in.sql` | 18 | 18 | 0 | 100% | 0 | 0 |
| `geo/053_tpoint_inout.in.sql` | 18 | 18 | 0 | 100% | 0 | 0 |
| `geo/054_tgeo_compops.in.sql` | 2 | 2 | 0 | 100% | 5 | 36 |
| `geo/054_tpoint_compops.in.sql` | 2 | 2 | 0 | 100% | 4 | 36 |
| `geo/056_tgeo_spatialfuncs.in.sql` | 17 | 17 | 0 | 100% | 0 | 0 |
| `geo/056_tpoint_spatialfuncs.in.sql` | 29 | 29 | 0 | 100% | 1 | 0 |
| `geo/058_tgeo_tile.in.sql` | 5 | 5 | 0 | 100% | 0 | 0 |
| `geo/058_tpoint_tile.in.sql` | 11 | 11 | 0 | 100% | 0 | 0 |
| `geo/060_tgeo_boxops.in.sql` | 13 | 13 | 0 | 100% | 0 | 50 |
| `geo/060_tpoint_boxops.in.sql` | 13 | 13 | 0 | 100% | 0 | 50 |
| `geo/062_tgeo_posops.in.sql` | 16 | 16 | 0 | 100% | 0 | 76 |
| `geo/062_tpoint_posops.in.sql` | 16 | 16 | 0 | 100% | 0 | 76 |
| `geo/064_tgeo_distance.in.sql` | 4 | 4 | 0 | 100% | 0 | 16 |
| `geo/064_tpoint_distance.in.sql` | 4 | 4 | 0 | 100% | 0 | 21 |
| `geo/066_tpoint_similarity.in.sql` | 5 | 5 | 0 | 100% | 0 | 0 |
| `geo/068_tgeo_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 9 | 0 |
| `geo/068_tpoint_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 12 | 0 |
| `geo/070_tgeo_spatialrels.in.sql` | 14 | 14 | 0 | 100% | 0 | 0 |
| `geo/070_tpoint_spatialrels.in.sql` | 12 | 12 | 0 | 100% | 0 | 0 |
| `geo/072_tgeo_tempspatialrels.in.sql` | 6 | 6 | 0 | 100% | 0 | 0 |
| `geo/072_tpoint_tempspatialrels.in.sql` | 5 | 5 | 0 | 100% | 0 | 0 |
| `geo/076_tgeo_analytics.in.sql` | 13 | 13 | 0 | 100% | 0 | 0 |
| `geo/076_tpoint_analytics.in.sql` | 18 | 18 | 0 | 100% | 0 | 0 |
| `geo/078_tpoint_datagen.in.sql` | 0 | 0 | 0 | 0% | 1 | 0 |
| `temporal/001_set.in.sql` | 39 | 39 | 0 | 100% | 43 | 38 |
| `temporal/002_set_ops.in.sql` | 11 | 11 | 0 | 100% | 0 | 176 |
| `temporal/003_span.in.sql` | 36 | 36 | 0 | 100% | 32 | 30 |
| `temporal/005_span_ops.in.sql` | 12 | 12 | 0 | 100% | 0 | 160 |
| `temporal/007_spanset.in.sql` | 51 | 51 | 0 | 100% | 30 | 30 |
| `temporal/009_spanset_ops.in.sql` | 14 | 14 | 0 | 100% | 0 | 280 |
| `temporal/015_span_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 10 | 0 |
| `temporal/021_tbox.in.sql` | 43 | 43 | 0 | 100% | 17 | 21 |
| `temporal/022_temporal.in.sql` | 94 | 94 | 0 | 100% | 23 | 24 |
| `temporal/023_temporal_inout.in.sql` | 16 | 16 | 0 | 100% | 0 | 0 |
| `temporal/025_temporal_tile.in.sql` | 16 | 16 | 0 | 100% | 0 | 0 |
| `temporal/026_tnumber_mathfuncs.in.sql` | 17 | 17 | 0 | 100% | 0 | 24 |
| `temporal/028_tbool_boolops.in.sql` | 4 | 4 | 0 | 100% | 0 | 7 |
| `temporal/029_ttext_textfuncs.in.sql` | 4 | 4 | 0 | 100% | 0 | 3 |
| `temporal/030_temporal_compops.in.sql` | 6 | 6 | 0 | 100% | 13 | 180 |
| `temporal/032_temporal_boxops.in.sql` | 11 | 11 | 0 | 100% | 0 | 100 |
| `temporal/034_temporal_posops.in.sql` | 8 | 8 | 0 | 100% | 0 | 112 |
| `temporal/036_tnumber_distance.in.sql` | 2 | 2 | 0 | 100% | 0 | 17 |
| `temporal/038_temporal_similarity.in.sql` | 5 | 5 | 0 | 100% | 0 | 0 |
| `temporal/040_temporal_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 40 | 0 |
| `temporal/042_temporal_waggfuncs.in.sql` | 0 | 0 | 0 | 0% | 8 | 0 |
| `temporal/046_temporal_analytics.in.sql` | 4 | 4 | 0 | 100% | 0 | 0 |
| **TOTAL (active)** | **858** | **858** | **0** | **100%** | **321** | — |

## Missing function names per active section

## Appendix B — Out of scope (PG-only, no Spark equivalent)

These entries are PG-specific helpers — index opclasses, aggregate transition/combine/final/serialize callbacks, planner hooks (`_sel`, `_joinsel`, `_supportfn`, `_analyze`), text/binary I/O helpers (`_in`, `_out`, `_recv`, `_send`), type modifier helpers, the `999_oid_cache` PG catalog hook, and PG geometric type constructors (`019_geo_constructors`).  None of them have Spark equivalents and they should not be implemented; listed here only for completeness.

### Whole sections excluded

| Section | Names |
|---|---:|
| `geo/073_tgeo_gist.in.sql` | 8 |
| `geo/073_tpoint_gist.in.sql` | 3 |
| `geo/074_tgeo_spgist.in.sql` | 9 |
| `temporal/011_span_indexes.in.sql` | 19 |
| `temporal/012_spanset_indexes.in.sql` | 3 |
| `temporal/013_set_indexes.in.sql` | 10 |
| `temporal/019_geo_constructors.in.sql` | 7 |
| `temporal/043_temporal_gist.in.sql` | 14 |
| `temporal/044_temporal_spgist.in.sql` | 10 |
| `temporal/999_oid_cache.in.sql` | 1 |

### PG helpers inside active sections

| Section | PG helpers |
|---|---:|
| `geo/050_geoset.in.sql` | 22 |
| `geo/051_stbox.in.sql` | 17 |
| `geo/052_tgeo.in.sql` | 18 |
| `geo/052_tpoint.in.sql` | 16 |
| `geo/054_tgeo_compops.in.sql` | 5 |
| `geo/054_tpoint_compops.in.sql` | 4 |
| `geo/056_tpoint_spatialfuncs.in.sql` | 1 |
| `geo/068_tgeo_aggfuncs.in.sql` | 9 |
| `geo/068_tpoint_aggfuncs.in.sql` | 12 |
| `geo/078_tpoint_datagen.in.sql` | 1 |
| `temporal/001_set.in.sql` | 43 |
| `temporal/003_span.in.sql` | 32 |
| `temporal/007_spanset.in.sql` | 30 |
| `temporal/015_span_aggfuncs.in.sql` | 10 |
| `temporal/021_tbox.in.sql` | 17 |
| `temporal/022_temporal.in.sql` | 23 |
| `temporal/030_temporal_compops.in.sql` | 13 |
| `temporal/040_temporal_aggfuncs.in.sql` | 40 |
| `temporal/042_temporal_waggfuncs.in.sql` | 8 |

## Appendix C — Deferred families

These families (cbuffer, npoint, pose, rgeo) are deferred until the active temporal + geo surface stabilises. Re-include by editing `DEFERRED_FAMILIES` at the top of `scripts/parity-audit.py`. Listed here so the picture stays complete; not counted in headline coverage.

| Section | Addressable | Covered | Missing | Coverage |
|---|---:|---:|---:|---:|
| `cbuffer/150_cbuffer.in.sql` | 31 | 8 | 23 | 26% |
| `cbuffer/151_cbufferset.in.sql` | 42 | 25 | 17 | 60% |
| `cbuffer/152_tcbuffer.in.sql` | 84 | 66 | 18 | 79% |
| `cbuffer/154_tcbuffer_compops.in.sql` | 6 | 6 | 0 | 100% |
| `cbuffer/155_tcbuffer_spatialfuncs.in.sql` | 11 | 11 | 0 | 100% |
| `cbuffer/158_tcbuffer_topops.in.sql` | 7 | 7 | 0 | 100% |
| `cbuffer/159_tcbuffer_posops.in.sql` | 12 | 12 | 0 | 100% |
| `cbuffer/160_tcbuffer_distance.in.sql` | 5 | 5 | 0 | 100% |
| `cbuffer/161_tcbuffer_aggfuncs.in.sql` | 7 | 0 | 7 | 0% |
| `cbuffer/162_tcbuffer_spatialrels.in.sql` | 13 | 13 | 0 | 100% |
| `cbuffer/164_tcbuffer_tempspatialrels.in.sql` | 6 | 6 | 0 | 100% |
| `cbuffer/166_tcbuffer_indexes.in.sql` | 1 | 0 | 1 | 0% |
| `npoint/081_npoint.in.sql` | 41 | 8 | 33 | 20% |
| `npoint/082_npointset.in.sql` | 43 | 22 | 21 | 51% |
| `npoint/083_tnpoint.in.sql` | 77 | 62 | 15 | 81% |
| `npoint/085_tnpoint_compops.in.sql` | 6 | 6 | 0 | 100% |
| `npoint/087_tnpoint_spatialfuncs.in.sql` | 12 | 12 | 0 | 100% |
| `npoint/089_tnpoint_topops.in.sql` | 7 | 7 | 0 | 100% |
| `npoint/090_tnpoint_posops.in.sql` | 12 | 12 | 0 | 100% |
| `npoint/091_tnpoint_routeops.in.sql` | 4 | 0 | 4 | 0% |
| `npoint/092_tnpoint_gin.in.sql` | 3 | 0 | 3 | 0% |
| `npoint/093_tnpoint_distance.in.sql` | 4 | 4 | 0 | 100% |
| `npoint/095_tnpoint_aggfuncs.in.sql` | 8 | 0 | 8 | 0% |
| `npoint/098_tnpoint_indexes.in.sql` | 1 | 0 | 1 | 0% |
| `pose/100_pose.in.sql` | 34 | 11 | 23 | 32% |
| `pose/101_poseset.in.sql` | 46 | 26 | 20 | 57% |
| `pose/102_tpose.in.sql` | 85 | 64 | 21 | 75% |
| `pose/104_tpose_compops.in.sql` | 6 | 6 | 0 | 100% |
| `pose/105_tpose_spatialfuncs.in.sql` | 8 | 8 | 0 | 100% |
| `pose/108_tpose_topops.in.sql` | 7 | 7 | 0 | 100% |
| `pose/109_tpose_posops.in.sql` | 16 | 16 | 0 | 100% |
| `pose/111_tpose_aggfuncs.in.sql` | 7 | 0 | 7 | 0% |
| `pose/113_tpose_distance.in.sql` | 4 | 4 | 0 | 100% |
| `pose/114_tpose_indexes.in.sql` | 1 | 0 | 1 | 0% |
| `rgeo/122_trgeo.in.sql` | 87 | 68 | 19 | 78% |
| `rgeo/124_trgeo_compops.in.sql` | 6 | 6 | 0 | 100% |
| `rgeo/125_trgeo_spatialfuncs.in.sql` | 8 | 8 | 0 | 100% |
| `rgeo/128_trgeo_topops.in.sql` | 5 | 5 | 0 | 100% |
| `rgeo/129_trgeo_posops.in.sql` | 12 | 12 | 0 | 100% |
| `rgeo/131_trgeo_aggfuncs.in.sql` | 7 | 0 | 7 | 0% |
| `rgeo/133_trgeo_distance.in.sql` | 4 | 4 | 0 | 100% |
| `rgeo/133_trgeo_vclip.in.sql` | 6 | 0 | 6 | 0% |
| `rgeo/134_trgeo_indexes.in.sql` | 1 | 0 | 1 | 0% |
| **TOTAL (deferred)** | **793** | **537** | **256** | **68%** |

