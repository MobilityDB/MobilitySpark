# MobilitySpark — 100% MobilityDB SQL parity

**Achieved**: 2026-05-10. Audit: 858/858 active addressable functions covered.
Tests: 907/907 green. Audit script + per-section report regenerable from the
repo (see [`docs/parity-status.md`](parity-status.md) and
[`scripts/parity-audit.py`](../scripts/parity-audit.py)).

This is one milestone in a cross-platform parity program — MobilitySpark
took its turn first; MobilityDuck and the other bindings progress on the
same audit methodology and timeline.

---

## What "100% active parity" means

The MobilityDB SQL surface (1,179 names + many overloads) is partitioned by
the parity audit into three buckets:

| Bucket | Examples | This release |
|--------|---|---|
| **Active addressable** | Every function a Spark UDF can semantically reproduce | **858 — all covered** |
| **Out of scope** (321) | PG plumbing (`*_in/_out/_recv/_send`, `_transfn/_combinefn/_finalfn`, GiST/SPGiST opclasses, `_cmp/_eq/_ne/_lt/_le/_gt/_ge/_hash`), PG built-in range types (`range`, `multirange`), PG-extension-only entry points (`create_trip` BerlinMOD generator, `transform_gk` SECONDO bridge) | excluded by design |
| **Deferred families** | cbuffer, npoint, pose, rgeo | next phase |

PostGIS types `BOX2D` / `BOX3D` are *not* OOS — PostGIS is embedded in MEOS
so `box2d` / `box3d` UDFs are real, returning the PostGIS text format.

The same audit methodology runs against MobilityDuck via
`MobilityDuck/scripts/parity-audit.py`; the OOS classifications listed here
should propagate to that script in lockstep so both bindings show
consistent gap inventories.

---

## What's now in MobilitySpark

### New UDF classes

| Class | Coverage |
|---|---|
| `TPointSTBoxOpsUDFs`, `TBoxOpsUDFs`, `SpansetOpsUDFs` | 104 cross-type positional / topological predicates |
| `TemporalCompUDFs`, `TemporalBoxOpsUDFs` | 56 temporal comparison + cross-type box predicates |
| `AlwaysSpatialRelsUDFs`, `SetOpsUDFs` | a-family spatial rels, set algebra |
| `IOAliasUDFs` | 100+ typed `*From{HexWKB,Binary,Text,EWKT,EWKB,MFJSON}` |
| `SubtypeConstructorUDFs`, `AccessorAliasUDFs` | typed Inst / Seq / SeqSet aliases, span / spanset accessors, PostGIS `box2d` / `box3d`, mobilitydb version |
| `BucketUDFs`, `GeoAffineUDFs` | scalar bucketing, affine transformations (translate / rotate / transscale) |
| `TileUDFs` | **multidimensional tiling for parallel processing** — `spaceTiles`, `spaceTimeTiles`, `timeTiles`, `valueTiles`, `valueTimeTiles`, `*Boxes`, `*Split`, single-tile `getValueTile` / `getValueTimeTile` / `getTBoxTimeTile` / `getSpaceTile` / `getSpaceTimeTile`, `geoMeasure`, `asMVTGeom`, `makeSimple` |
| `SeqSetGapsUDFs` | `tbool` / `tint` / `tfloat` / `ttext` / `tgeompoint` / `tgeogpoint` / `tgeometry` / `tgeographySeqSetGaps` — closes the long-standing user request from [MobilityDB issue #187](https://github.com/MobilityDB/MobilityDB/issues/187) |

### JNR-FFI surface

All MEOS symbols MobilitySpark calls are provided by `functions.functions.*`
in JMEOS 1.4. The earlier supplementary `MeosNative.java` was removed once
the JMEOS generator gained `Datum → long` and `MeosType → int` lowering
and the 10 private-header residuals (`mobilitydb_version`,
`mobilitydb_full_version`, `temporal_values_p`, `set_make_free`,
`temptype_basetype`, `temporal_mem_size`, `tnumber_value_split`,
`tnumber_value_time_split`, `tnumber_value_time_boxes`,
`tbox_get_value_time_tile`) were appended to the amalgamated MEOS
header consumed by the generator
([JMEOS PR #15](https://github.com/MobilityDB/JMEOS/pull/15)).

---

## Why this matters for the ecosystem

- **Portable SQL benchmark** — the BerlinMOD Q1–Q17 + binary roundtrip
  benchmark now runs unchanged across MobilityDB (PG), MobilityDuck
  (DuckDB), and MobilitySpark (Spark). The same `.sql` file, the same
  expected output. Cross-platform performance comparisons become an
  apples-to-apples exercise.
- **Edge-to-cloud pipeline** — the TemporalParquet RFC realisation
  (MobilityDuck quickstart producer → MobilityDB legacy ingest →
  MobilitySpark Parquet lake consumer) is now backed by a complete UDF
  surface on the cloud side. See
  [`docs/beta-testing-edge-to-cloud.md`](beta-testing-edge-to-cloud.md)
  for the cross-platform beta program.
- **Audit methodology** — `scripts/parity-audit.py` is reusable. The
  same script (with binding-specific source-parsing) runs against
  MobilityDuck. The OOS classifications discovered here (PG range types,
  BerlinMOD generator, SECONDO bridge, btree opclass support) apply
  ecosystem-wide.

---

## What's deferred (and why)

The four "deferred families" — `cbuffer`, `npoint`, `pose`, `rgeo` —
together represent ~250 additional functions. They're stable in MEOS
and MobilityDB but are domain-specific (circular buffers, network
points, spatial poses, rigid geometries). They will be added in a follow-
up sweep using the same audit methodology after the active surface
stabilises in production use.

---

## How to verify

```bash
git checkout feat/jmeos-1.3-berlinmod-poc
mvn test                              # 907/907 should pass
python3 scripts/parity-audit.py \
    --mdb /path/to/MobilityDB \
    --mspark .                        # writes docs/parity-status.md
```

The audit reports `858/858 (100.0%)` against the active surface. Per-section
breakdown (51 sections, all 100%) lives at `docs/parity-status.md`.
