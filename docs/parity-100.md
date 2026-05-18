# MobilitySpark — MobilityDB SQL parity

MobilitySpark covers **858 / 858 active addressable MobilityDB SQL functions
(100%)** on the temporal and geo surface, verified against MobilityDB `master`
by [`scripts/parity-audit.py`](../scripts/parity-audit.py). The per-section
report is [`docs/parity-status.md`](parity-status.md). This is the same scope
and audit methodology by which MobilityDuck reports 100% parity; the two
bindings hold the same parity statement.

## Scope partition

The MobilityDB SQL surface (1,179 names plus overloads) is partitioned by the
audit into three buckets:

| Bucket | Content | State |
|---|---|---|
| **Active addressable** | Every function a Spark UDF can semantically reproduce (temporal + geo) | **858 — all covered** |
| **Out of scope** (321) | PG plumbing (`*_in/_out/_recv/_send`, `_transfn/_combinefn/_finalfn`, GiST/SPGiST opclasses, `_cmp/_eq/_ne/_lt/_le/_gt/_ge/_hash`), PG `range`/`multirange` types, PG-extension-only entry points (`create_trip` BerlinMOD generator, `transform_gk` SECONDO bridge) | excluded by design |
| **Deferred families** | `cbuffer`, `npoint`, `pose`, `rgeo` (~250 functions) | later phase, shared with MobilityDuck |

PostGIS `box2d` / `box3d` are in scope — PostGIS is embedded in MEOS, so those
UDFs are real and return the PostGIS text format.

The same audit runs against MobilityDuck (`MobilityDuck/scripts/parity-audit.py`);
the out-of-scope and deferred classifications are shared so both bindings report
consistent gap inventories.

## Coverage surface

The full temporal + geo surface is registered as Spark SQL UDFs: cross-type
positional and topological predicates, temporal comparisons, ever/always
spatial relationships, set algebra, typed I/O
(`*From{HexWKB,Binary,Text,EWKT,EWKB,MFJSON}`), subtype constructors and
accessor aliases, scalar bucketing and affine transforms, multidimensional
tiling (`spaceTiles`, `spaceTimeTiles`, `timeTiles`, `valueTiles`,
`valueTimeTiles`, `*Boxes`, `*Split`, `geoMeasure`, `asMVTGeom`, `makeSimple`),
and `SeqSetGaps` for every base temporal type. All MEOS symbols are provided by
JMEOS 1.4 (`functions.functions.*`).

## Deferred families

`cbuffer`, `npoint`, `pose`, `rgeo` (~250 functions) are stable in MEOS and
MobilityDB but domain-specific (circular buffers, network points, spatial
poses, rigid geometries). They are a later parity phase on the same audit
methodology and the same timeline as MobilityDuck — neither binding claims
them yet.

## Reproduce

```bash
mvn test
python3 scripts/parity-audit.py --mdb /path/to/MobilityDB --mspark .
```

The audit reports `858/858 (100.0%)` on the active surface; the per-section
breakdown (every active section at 100%) is written to
[`docs/parity-status.md`](parity-status.md).
