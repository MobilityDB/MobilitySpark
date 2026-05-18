# MobilitySpark parity status — surface-level audit

Generated 2026-05-18. **Active addressable scope** (temporal + geo, excluding PG-only helpers): 1353/1462 names covered (92.5%).

**Out of scope** (PG-only — no Spark equivalent exists): 594 names skipped — 84 from PG-only sections (GiST/SPGiST opclasses, set/span/spanset index files, `019_geo_constructors.in.sql` PG geometric types, `999_oid_cache.in.sql`) plus 510 PG helper functions inside active sections (`*_in/_out/_recv/_send`, `*_transfn/_combinefn/_finalfn/_serialize/_deserialize`, `*_sel/_joinsel/_supportfn/_analyze`, `*_typmod_in/_typmod_out`).  Listed in appendix B; not counted in the headline.

**All six type families in scope** (temporal, geo, cbuffer, npoint, pose, rgeo). None is deferred or excluded from the headline — they are full user-facing temporal types covered like every other family (RFC #920; MobilityDB#1075).

**Methodology**: parsed `CREATE FUNCTION` from `mobilitydb/sql/**/*.in.sql` and `spark.udf().register("name", ...)` (scalar + UDAF) from `MobilitySpark/src/main/java/**/*.java`. Match is by **function name only**, case-insensitive; MobilityDB snake_case is converted to camelCase before comparison so e.g. `tdistance_tgeo_geo` matches `tdistanceTgeoGeo`. A name registered in MobilitySpark is treated as covering all its overloads; per-overload signature parity is not verified at this granularity.

**Caveats**:
- A name match doesn't prove signature parity. e.g. `before(temporal, temporal)` registered in MobilitySpark does not necessarily cover MobilityDB's `before(tstzspan, temporal)`.
- Spark SQL has no infix-operator extension API; equivalent named functions are registered. The `MDB operators` column lists how many `CREATE OPERATOR` statements exist in the section, all of which collapse to named-function form in MobilitySpark.

Regenerate with `python3 scripts/parity-audit.py --mdb ../MobilityDB --mspark . --out docs/parity-status.md`. The OUT_OF_SCOPE_SECTIONS / OUT_OF_SCOPE_NAME_SUFFIXES / DEFERRED_FAMILIES sets at the top of that script control bucketing.

## Active-scope coverage summary (addressable surface)

| Section | Addressable | Covered | Missing | Coverage | OOS | MDB operators |
|---|---:|---:|---:|---:|---:|---:|
| `cbuffer/150_cbuffer.in.sql` | 18 | 8 | 10 | 44% | 13 | 7 |
| `cbuffer/151_cbufferset.in.sql` | 27 | 25 | 2 | 93% | 15 | 23 |
| `cbuffer/152_tcbuffer.in.sql` | 71 | 59 | 12 | 83% | 13 | 6 |
| `cbuffer/154_tcbuffer_compops.in.sql` | 2 | 2 | 0 | 100% | 4 | 18 |
| `cbuffer/155_tcbuffer_spatialfuncs.in.sql` | 11 | 11 | 0 | 100% | 0 | 0 |
| `cbuffer/158_tcbuffer_topops.in.sql` | 7 | 7 | 0 | 100% | 0 | 25 |
| `cbuffer/159_tcbuffer_posops.in.sql` | 12 | 12 | 0 | 100% | 0 | 44 |
| `cbuffer/160_tcbuffer_distance.in.sql` | 5 | 5 | 0 | 100% | 0 | 17 |
| `cbuffer/161_tcbuffer_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 7 | 0 |
| `cbuffer/162_tcbuffer_spatialrels.in.sql` | 13 | 13 | 0 | 100% | 0 | 0 |
| `cbuffer/164_tcbuffer_tempspatialrels.in.sql` | 6 | 6 | 0 | 100% | 0 | 0 |
| `cbuffer/166_tcbuffer_indexes.in.sql` | 1 | 0 | 1 | 0% | 0 | 0 |
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
| `npoint/081_npoint.in.sql` | 19 | 8 | 11 | 42% | 22 | 12 |
| `npoint/082_npointset.in.sql` | 28 | 22 | 6 | 79% | 15 | 23 |
| `npoint/083_tnpoint.in.sql` | 65 | 55 | 10 | 85% | 12 | 6 |
| `npoint/085_tnpoint_compops.in.sql` | 2 | 2 | 0 | 100% | 4 | 18 |
| `npoint/087_tnpoint_spatialfuncs.in.sql` | 12 | 12 | 0 | 100% | 0 | 0 |
| `npoint/089_tnpoint_topops.in.sql` | 7 | 7 | 0 | 100% | 0 | 25 |
| `npoint/090_tnpoint_posops.in.sql` | 12 | 12 | 0 | 100% | 0 | 44 |
| `npoint/091_tnpoint_routeops.in.sql` | 4 | 0 | 4 | 0% | 0 | 20 |
| `npoint/092_tnpoint_gin.in.sql` | 3 | 0 | 3 | 0% | 0 | 0 |
| `npoint/093_tnpoint_distance.in.sql` | 4 | 4 | 0 | 100% | 0 | 12 |
| `npoint/095_tnpoint_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 8 | 0 |
| `npoint/098_tnpoint_indexes.in.sql` | 1 | 0 | 1 | 0% | 0 | 0 |
| `pose/100_pose.in.sql` | 21 | 11 | 10 | 52% | 13 | 7 |
| `pose/101_poseset.in.sql` | 31 | 26 | 5 | 84% | 15 | 23 |
| `pose/102_tpose.in.sql` | 72 | 58 | 14 | 81% | 13 | 6 |
| `pose/104_tpose_compops.in.sql` | 2 | 2 | 0 | 100% | 4 | 18 |
| `pose/105_tpose_spatialfuncs.in.sql` | 8 | 8 | 0 | 100% | 0 | 0 |
| `pose/108_tpose_topops.in.sql` | 7 | 7 | 0 | 100% | 0 | 25 |
| `pose/109_tpose_posops.in.sql` | 16 | 16 | 0 | 100% | 0 | 56 |
| `pose/111_tpose_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 7 | 0 |
| `pose/113_tpose_distance.in.sql` | 4 | 4 | 0 | 100% | 0 | 12 |
| `pose/114_tpose_indexes.in.sql` | 1 | 0 | 1 | 0% | 0 | 0 |
| `rgeo/122_trgeo.in.sql` | 74 | 62 | 12 | 84% | 13 | 6 |
| `rgeo/124_trgeo_compops.in.sql` | 2 | 2 | 0 | 100% | 4 | 18 |
| `rgeo/125_trgeo_spatialfuncs.in.sql` | 8 | 8 | 0 | 100% | 0 | 0 |
| `rgeo/128_trgeo_topops.in.sql` | 5 | 5 | 0 | 100% | 0 | 25 |
| `rgeo/129_trgeo_posops.in.sql` | 12 | 12 | 0 | 100% | 0 | 44 |
| `rgeo/131_trgeo_aggfuncs.in.sql` | 0 | 0 | 0 | 0% | 7 | 0 |
| `rgeo/133_trgeo_distance.in.sql` | 4 | 4 | 0 | 100% | 0 | 12 |
| `rgeo/133_trgeo_vclip.in.sql` | 6 | 0 | 6 | 0% | 0 | 0 |
| `rgeo/134_trgeo_indexes.in.sql` | 1 | 0 | 1 | 0% | 0 | 0 |
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
| **TOTAL (active)** | **1462** | **1353** | **109** | **93%** | **510** | — |

## Missing function names per active section

### `cbuffer/150_cbuffer.in.sql` — 10 missing of 18 addressable (44% covered)

- `cbuffer` → `cbuffer` (2 overloads)
- `cbuffer_contains` → `cbufferContains`
- `cbuffer_covers` → `cbufferCovers`
- `cbuffer_disjoint` → `cbufferDisjoint`
- `cbuffer_dwithin` → `cbufferDwithin`
- `cbuffer_intersects` → `cbufferIntersects`
- `cbuffer_same` → `cbufferSame`
- `cbuffer_touches` → `cbufferTouches`
- `point` → `point`
- `radius` → `radius`

### `cbuffer/151_cbufferset.in.sql` — 2 missing of 27 addressable (93% covered)

- `cbuffersetFromBinary` → `cbuffersetFromBinary`
- `cbuffersetFromHexWKB` → `cbuffersetFromHexWKB`

### `cbuffer/152_tcbuffer.in.sql` — 12 missing of 71 addressable (83% covered)

- `points` → `points`
- `radius` → `radius`
- `tcbuffer` → `tcbuffer` (8 overloads)
- `tcbufferFromBinary` → `tcbufferFromBinary`
- `tcbufferFromEWKB` → `tcbufferFromEWKB`
- `tcbufferFromEWKT` → `tcbufferFromEWKT`
- `tcbufferFromHexEWKB` → `tcbufferFromHexEWKB`
- `tcbufferFromText` → `tcbufferFromText`
- `tcbufferInst` → `tcbufferInst`
- `tcbufferSeq` → `tcbufferSeq` (2 overloads)
- `tcbufferSeqSet` → `tcbufferSeqSet` (2 overloads)
- `tcbufferSeqSetGaps` → `tcbufferSeqSetGaps`

### `cbuffer/166_tcbuffer_indexes.in.sql` — 1 missing of 1 addressable (0% covered)

- `tcbuffer_gist_consistent` → `tcbufferGistConsistent`

### `npoint/081_npoint.in.sql` — 11 missing of 19 addressable (42% covered)

- `endPosition` → `endPosition`
- `getPosition` → `getPosition`
- `npoint` → `npoint` (2 overloads)
- `npointFromBinary` → `npointFromBinary`
- `npointFromEWKB` → `npointFromEWKB`
- `npointFromEWKT` → `npointFromEWKT`
- `npointFromHexEWKB` → `npointFromHexEWKB`
- `npointFromText` → `npointFromText`
- `nsegment` → `nsegment` (3 overloads)
- `route` → `route` (2 overloads)
- `startPosition` → `startPosition`

### `npoint/082_npointset.in.sql` — 6 missing of 28 addressable (79% covered)

- `npointsetFromBinary` → `npointsetFromBinary`
- `npointsetFromEWKB` → `npointsetFromEWKB`
- `npointsetFromEWKT` → `npointsetFromEWKT`
- `npointsetFromHexWKB` → `npointsetFromHexWKB`
- `npointsetFromText` → `npointsetFromText`
- `routes` → `routes`

### `npoint/083_tnpoint.in.sql` — 10 missing of 65 addressable (85% covered)

- `positions` → `positions`
- `route` → `route`
- `routes` → `routes`
- `tnpoint` → `tnpoint` (6 overloads)
- `tnpointFromBinary` → `tnpointFromBinary`
- `tnpointFromHexWKB` → `tnpointFromHexWKB`
- `tnpointInst` → `tnpointInst`
- `tnpointSeq` → `tnpointSeq` (3 overloads)
- `tnpointSeqSet` → `tnpointSeqSet` (3 overloads)
- `tnpointSeqSetGaps` → `tnpointSeqSetGaps`

### `npoint/091_tnpoint_routeops.in.sql` — 4 missing of 4 addressable (0% covered)

- `contained_rid` → `containedRid` (5 overloads)
- `contains_rid` → `containsRid` (5 overloads)
- `overlaps_rid` → `overlapsRid` (3 overloads)
- `same_rid` → `sameRid` (7 overloads)

### `npoint/092_tnpoint_gin.in.sql` — 3 missing of 3 addressable (0% covered)

- `tnpoint_gin_extract_query` → `tnpointGinExtractQuery`
- `tnpoint_gin_extract_value` → `tnpointGinExtractValue`
- `tnpoint_gin_triconsistent` → `tnpointGinTriconsistent`

### `npoint/098_tnpoint_indexes.in.sql` — 1 missing of 1 addressable (0% covered)

- `tnpoint_gist_consistent` → `tnpointGistConsistent`

### `pose/100_pose.in.sql` — 10 missing of 21 addressable (52% covered)

- `orientation` → `orientation`
- `point` → `point`
- `pose` → `pose` (4 overloads)
- `poseFromBinary` → `poseFromBinary`
- `poseFromEWKB` → `poseFromEWKB`
- `poseFromEWKT` → `poseFromEWKT`
- `poseFromHexEWKB` → `poseFromHexEWKB`
- `poseFromText` → `poseFromText`
- `pose_same` → `poseSame`
- `rotation` → `rotation`

### `pose/101_poseset.in.sql` — 5 missing of 31 addressable (84% covered)

- `posesetFromBinary` → `posesetFromBinary`
- `posesetFromEWKB` → `posesetFromEWKB`
- `posesetFromEWKT` → `posesetFromEWKT`
- `posesetFromHexWKB` → `posesetFromHexWKB`
- `posesetFromText` → `posesetFromText`

### `pose/102_tpose.in.sql` — 14 missing of 72 addressable (81% covered)

- `orientation` → `orientation`
- `points` → `points`
- `rotation` → `rotation`
- `tpose` → `tpose` (5 overloads)
- `tposeFromBinary` → `tposeFromBinary`
- `tposeFromEWKB` → `tposeFromEWKB`
- `tposeFromEWKT` → `tposeFromEWKT`
- `tposeFromHexEWKB` → `tposeFromHexEWKB`
- `tposeFromMFJSON` → `tposeFromMFJSON`
- `tposeFromText` → `tposeFromText`
- `tposeInst` → `tposeInst`
- `tposeSeq` → `tposeSeq` (2 overloads)
- `tposeSeqSet` → `tposeSeqSet` (2 overloads)
- `tposeSeqSetGaps` → `tposeSeqSetGaps`

### `pose/114_tpose_indexes.in.sql` — 1 missing of 1 addressable (0% covered)

- `tpose_gist_consistent` → `tposeGistConsistent`

### `rgeo/122_trgeo.in.sql` — 12 missing of 74 addressable (84% covered)

- `tpose` → `tpose`
- `trgeometry` → `trgeometry` (3 overloads)
- `trgeometryFromBinary` → `trgeometryFromBinary`
- `trgeometryFromEWKB` → `trgeometryFromEWKB`
- `trgeometryFromEWKT` → `trgeometryFromEWKT`
- `trgeometryFromHexEWKB` → `trgeometryFromHexEWKB`
- `trgeometryFromMFJSON` → `trgeometryFromMFJSON`
- `trgeometryFromText` → `trgeometryFromText`
- `trgeometryInst` → `trgeometryInst`
- `trgeometrySeq` → `trgeometrySeq` (2 overloads)
- `trgeometrySeqSet` → `trgeometrySeqSet` (2 overloads)
- `trgeometrySeqSetGaps` → `trgeometrySeqSetGaps`

### `rgeo/133_trgeo_vclip.in.sql` — 6 missing of 6 addressable (0% covered)

- `v_clip_poly_point` → `vClipPolyPoint`
- `v_clip_poly_poly` → `vClipPolyPoly`
- `v_clip_tpoly_point` → `vClipTpolyPoint`
- `v_clip_tpoly_poly` → `vClipTpolyPoly`
- `v_clip_tpoly_tpoint` → `vClipTpolyTpoint`
- `v_clip_tpoly_tpoly` → `vClipTpolyTpoly`

### `rgeo/134_trgeo_indexes.in.sql` — 1 missing of 1 addressable (0% covered)

- `trgeometry_gist_consistent` → `trgeometryGistConsistent`

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
| `cbuffer/150_cbuffer.in.sql` | 13 |
| `cbuffer/151_cbufferset.in.sql` | 15 |
| `cbuffer/152_tcbuffer.in.sql` | 13 |
| `cbuffer/154_tcbuffer_compops.in.sql` | 4 |
| `cbuffer/161_tcbuffer_aggfuncs.in.sql` | 7 |
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
| `npoint/081_npoint.in.sql` | 22 |
| `npoint/082_npointset.in.sql` | 15 |
| `npoint/083_tnpoint.in.sql` | 12 |
| `npoint/085_tnpoint_compops.in.sql` | 4 |
| `npoint/095_tnpoint_aggfuncs.in.sql` | 8 |
| `pose/100_pose.in.sql` | 13 |
| `pose/101_poseset.in.sql` | 15 |
| `pose/102_tpose.in.sql` | 13 |
| `pose/104_tpose_compops.in.sql` | 4 |
| `pose/111_tpose_aggfuncs.in.sql` | 7 |
| `rgeo/122_trgeo.in.sql` | 13 |
| `rgeo/124_trgeo_compops.in.sql` | 4 |
| `rgeo/131_trgeo_aggfuncs.in.sql` | 7 |
| `temporal/001_set.in.sql` | 43 |
| `temporal/003_span.in.sql` | 32 |
| `temporal/007_spanset.in.sql` | 30 |
| `temporal/015_span_aggfuncs.in.sql` | 10 |
| `temporal/021_tbox.in.sql` | 17 |
| `temporal/022_temporal.in.sql` | 23 |
| `temporal/030_temporal_compops.in.sql` | 13 |
| `temporal/040_temporal_aggfuncs.in.sql` | 40 |
| `temporal/042_temporal_waggfuncs.in.sql` | 8 |

