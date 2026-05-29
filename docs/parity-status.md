# MobilitySpark parity status - surface audit

Generated 2026-05-28 against the MobilityDB SQL surface (master + the portable-aliases / RFC #920 work).

## Headline (CREATE FUNCTION surface, comparison operators included)

- Addressable function names: **1782**
- **Exact name match: 1321 (74.1%)** - the exact-match floor
- + type-dispatch / overload-split heuristics: 1653 (92.8%) - name-backed, no type-agnostic assumption
- + RFC #920 bare-name dispatch (per-type behaviour NOT verified by this audit): 1770 (99.3%)
- Hallucinated (verb only a substring of an unrelated UDF): 4
- Genuinely missing: **8 (0.4%)**

## Aggregates (CREATE AGGREGATE - invisible to a CREATE FUNCTION-only audit)

- Aggregate names: **19**; backed (exact/heuristic): 12; not backed: 7
- Not backed: extent, tsum, wavg, wcount, wmax, wmin, wsum (some are per-type-split or windowed; see the source for which are real gaps)

## Methodology

Parsed `CREATE FUNCTION` + `CREATE AGGREGATE` from `mobilitydb/sql/**/*.in.sql` and `spark.udf().register("name", ...)` (scalar + UDAF) from `MobilitySpark/src/main/java/**/*.java`. Each MobilityDB name is placed in exactly one tier (EXACT / PREFIX / SUFFIX / BARENAME / WRAPPER / MISSING - see the header of `scripts/parity-audit.py`). A name audit does NOT prove per-overload signature parity, and the BARENAME tier (covered only by a type-agnostic RFC #920 bare-name UDF) is flagged, not assumed correct per type. Comparison/ordering operators are counted; PG-only plumbing (index opclasses, `_in/_out/_recv/_send`, `_transfn/...`, planner hooks) is excluded.

## Per-section coverage

| Section | Addr | Exact | Heur | BareName? | Halluc | Missing | OOS | MDB ops |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `cbuffer/150_cbuffer.in.sql` | 27 | 23 | 2 | 0 | 0 | 2 | 4 | 7 |
| `cbuffer/151_cbufferset.in.sql` | 36 | 29 | 7 | 0 | 0 | 0 | 6 | 23 |
| `cbuffer/152_tcbuffer.in.sql` | 79 | 66 | 13 | 0 | 0 | 0 | 5 | 6 |
| `cbuffer/154_tcbuffer_compops.in.sql` | 6 | 0 | 4 | 2 | 0 | 0 | 0 | 18 |
| `cbuffer/155_tcbuffer_spatialfuncs.in.sql` | 11 | 7 | 4 | 0 | 0 | 0 | 0 | 0 |
| `cbuffer/158_tcbuffer_topops.in.sql` | 7 | 2 | 0 | 5 | 0 | 0 | 0 | 25 |
| `cbuffer/159_tcbuffer_posops.in.sql` | 12 | 0 | 4 | 8 | 0 | 0 | 0 | 44 |
| `cbuffer/160_tcbuffer_distance.in.sql` | 5 | 4 | 1 | 0 | 0 | 0 | 0 | 17 |
| `cbuffer/161_tcbuffer_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |
| `cbuffer/162_tcbuffer_spatialrels.in.sql` | 13 | 10 | 3 | 0 | 0 | 0 | 0 | 0 |
| `cbuffer/164_tcbuffer_tempspatialrels.in.sql` | 6 | 6 | 0 | 0 | 0 | 0 | 0 | 0 |
| `cbuffer/167_portable_aliases.in.sql` | 19 | 19 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/050_geoset.in.sql` | 43 | 36 | 7 | 0 | 0 | 0 | 13 | 46 |
| `geo/051_stbox.in.sql` | 75 | 48 | 27 | 0 | 0 | 0 | 8 | 29 |
| `geo/052_tgeo.in.sql` | 70 | 56 | 14 | 0 | 0 | 0 | 10 | 12 |
| `geo/052_tpoint.in.sql` | 70 | 56 | 14 | 0 | 0 | 0 | 8 | 12 |
| `geo/053_tgeo_inout.in.sql` | 18 | 17 | 1 | 0 | 0 | 0 | 0 | 0 |
| `geo/053_tpoint_inout.in.sql` | 18 | 17 | 1 | 0 | 0 | 0 | 0 | 0 |
| `geo/054_tgeo_compops.in.sql` | 6 | 0 | 4 | 2 | 0 | 0 | 1 | 36 |
| `geo/054_tpoint_compops.in.sql` | 6 | 0 | 4 | 2 | 0 | 0 | 0 | 36 |
| `geo/056_tgeo_spatialfuncs.in.sql` | 17 | 12 | 5 | 0 | 0 | 0 | 0 | 0 |
| `geo/056_tpoint_spatialfuncs.in.sql` | 29 | 22 | 7 | 0 | 0 | 0 | 1 | 0 |
| `geo/058_tgeo_tile.in.sql` | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/058_tpoint_tile.in.sql` | 11 | 11 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/060_tgeo_boxops.in.sql` | 13 | 8 | 0 | 5 | 0 | 0 | 0 | 50 |
| `geo/060_tpoint_boxops.in.sql` | 13 | 8 | 0 | 5 | 0 | 0 | 0 | 50 |
| `geo/062_tgeo_posops.in.sql` | 16 | 0 | 4 | 12 | 0 | 0 | 0 | 76 |
| `geo/062_tpoint_posops.in.sql` | 16 | 0 | 4 | 12 | 0 | 0 | 0 | 76 |
| `geo/064_tgeo_distance.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 16 |
| `geo/064_tpoint_distance.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 21 |
| `geo/066_tpoint_similarity.in.sql` | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/068_tgeo_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 9 | 0 |
| `geo/068_tpoint_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 12 | 0 |
| `geo/070_tgeo_spatialrels.in.sql` | 14 | 11 | 3 | 0 | 0 | 0 | 0 | 0 |
| `geo/070_tpoint_spatialrels.in.sql` | 12 | 10 | 2 | 0 | 0 | 0 | 0 | 0 |
| `geo/072_tgeo_tempspatialrels.in.sql` | 6 | 6 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/072_tpoint_tempspatialrels.in.sql` | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/076_tgeo_analytics.in.sql` | 13 | 13 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/076_tpoint_analytics.in.sql` | 18 | 18 | 0 | 0 | 0 | 0 | 0 | 0 |
| `geo/078_tpoint_datagen.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| `geo/079_portable_aliases.in.sql` | 23 | 23 | 0 | 0 | 0 | 0 | 0 | 0 |
| `npoint/081_npoint.in.sql` | 33 | 31 | 2 | 0 | 0 | 0 | 8 | 12 |
| `npoint/082_npointset.in.sql` | 37 | 31 | 6 | 0 | 0 | 0 | 6 | 23 |
| `npoint/083_tnpoint.in.sql` | 73 | 60 | 13 | 0 | 0 | 0 | 4 | 6 |
| `npoint/085_tnpoint_compops.in.sql` | 6 | 0 | 4 | 2 | 0 | 0 | 0 | 18 |
| `npoint/087_tnpoint_spatialfuncs.in.sql` | 12 | 11 | 1 | 0 | 0 | 0 | 0 | 0 |
| `npoint/089_tnpoint_topops.in.sql` | 7 | 2 | 0 | 5 | 0 | 0 | 0 | 25 |
| `npoint/090_tnpoint_posops.in.sql` | 12 | 0 | 4 | 8 | 0 | 0 | 0 | 44 |
| `npoint/091_tnpoint_routeops.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 20 |
| `npoint/093_tnpoint_distance.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 12 |
| `npoint/095_tnpoint_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 8 | 0 |
| `npoint/099_portable_aliases.in.sql` | 19 | 19 | 0 | 0 | 0 | 0 | 0 | 0 |
| `pose/100_pose.in.sql` | 30 | 28 | 2 | 0 | 0 | 0 | 4 | 7 |
| `pose/101_poseset.in.sql` | 40 | 33 | 7 | 0 | 0 | 0 | 6 | 23 |
| `pose/102_tpose.in.sql` | 80 | 66 | 14 | 0 | 0 | 0 | 5 | 6 |
| `pose/104_tpose_compops.in.sql` | 6 | 0 | 4 | 2 | 0 | 0 | 0 | 18 |
| `pose/105_tpose_spatialfuncs.in.sql` | 8 | 7 | 1 | 0 | 0 | 0 | 0 | 0 |
| `pose/108_tpose_topops.in.sql` | 7 | 2 | 0 | 5 | 0 | 0 | 0 | 25 |
| `pose/109_tpose_posops.in.sql` | 16 | 0 | 4 | 12 | 0 | 0 | 0 | 56 |
| `pose/111_tpose_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |
| `pose/113_tpose_distance.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 12 |
| `pose/115_portable_aliases.in.sql` | 23 | 23 | 0 | 0 | 0 | 0 | 0 | 0 |
| `rgeo/122_trgeo.in.sql` | 82 | 66 | 16 | 0 | 0 | 0 | 5 | 6 |
| `rgeo/124_trgeo_compops.in.sql` | 6 | 0 | 4 | 2 | 0 | 0 | 0 | 18 |
| `rgeo/125_trgeo_spatialfuncs.in.sql` | 8 | 7 | 1 | 0 | 0 | 0 | 0 | 0 |
| `rgeo/128_trgeo_topops.in.sql` | 5 | 0 | 0 | 5 | 0 | 0 | 0 | 25 |
| `rgeo/129_trgeo_posops.in.sql` | 12 | 0 | 4 | 8 | 0 | 0 | 0 | 44 |
| `rgeo/131_trgeo_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |
| `rgeo/133_trgeo_distance.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 12 |
| `rgeo/133_trgeo_vclip.in.sql` | 6 | 0 | 0 | 0 | 0 | 6 | 0 | 0 |
| `rgeo/135_portable_aliases.in.sql` | 19 | 19 | 0 | 0 | 0 | 0 | 0 | 0 |
| `temporal/001_set.in.sql` | 48 | 35 | 13 | 0 | 0 | 0 | 34 | 38 |
| `temporal/002_set_ops.in.sql` | 11 | 11 | 0 | 0 | 0 | 0 | 0 | 176 |
| `temporal/003_span.in.sql` | 45 | 38 | 7 | 0 | 0 | 0 | 23 | 30 |
| `temporal/005_span_ops.in.sql` | 12 | 11 | 1 | 0 | 0 | 0 | 0 | 160 |
| `temporal/007_spanset.in.sql` | 60 | 48 | 12 | 0 | 0 | 0 | 21 | 30 |
| `temporal/009_spanset_ops.in.sql` | 14 | 12 | 2 | 0 | 0 | 0 | 0 | 280 |
| `temporal/015_span_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| `temporal/021_tbox.in.sql` | 52 | 33 | 19 | 0 | 0 | 0 | 8 | 21 |
| `temporal/022_temporal.in.sql` | 102 | 79 | 23 | 0 | 0 | 0 | 15 | 24 |
| `temporal/023_temporal_inout.in.sql` | 16 | 15 | 1 | 0 | 0 | 0 | 0 | 0 |
| `temporal/025_temporal_tile.in.sql` | 16 | 16 | 0 | 0 | 0 | 0 | 0 | 0 |
| `temporal/026_tnumber_mathfuncs.in.sql` | 17 | 0 | 13 | 0 | 4 | 0 | 0 | 24 |
| `temporal/028_tbool_boolops.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 7 |
| `temporal/029_ttext_textfuncs.in.sql` | 4 | 1 | 3 | 0 | 0 | 0 | 0 | 3 |
| `temporal/030_temporal_compops.in.sql` | 18 | 0 | 12 | 6 | 0 | 0 | 1 | 180 |
| `temporal/032_temporal_boxops.in.sql` | 11 | 6 | 0 | 5 | 0 | 0 | 0 | 100 |
| `temporal/034_temporal_posops.in.sql` | 8 | 0 | 4 | 4 | 0 | 0 | 0 | 112 |
| `temporal/036_tnumber_distance.in.sql` | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 17 |
| `temporal/038_temporal_similarity.in.sql` | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| `temporal/040_temporal_aggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 40 | 0 |
| `temporal/042_temporal_waggfuncs.in.sql` | 0 | 0 | 0 | 0 | 0 | 0 | 8 | 0 |
| `temporal/046_temporal_analytics.in.sql` | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |
| `temporal/048_portable_aliases.in.sql` | 19 | 19 | 0 | 0 | 0 | 0 | 0 | 0 |
| **TOTAL** | **1782** | **1321** | **332** | **117** | **4** | **8** | | |

## Genuinely missing function names

- **cbuffer** (2): cbuffer_hash, cbuffer_hash_extended
- **rgeo** (6): v_clip_poly_point, v_clip_poly_poly, v_clip_tpoly_point, v_clip_tpoly_poly, v_clip_tpoly_tpoint, v_clip_tpoly_tpoly

## Hallucinated matches (substring only - treat as NOT covered)

- `tnumber_add` [temporal/026_tnumber_mathfuncs.in.sql]
- `tnumber_div` [temporal/026_tnumber_mathfuncs.in.sql]
- `tnumber_mult` [temporal/026_tnumber_mathfuncs.in.sql]
- `tnumber_sub` [temporal/026_tnumber_mathfuncs.in.sql]

