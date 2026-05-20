# MobilitySpark — portable dialect parity & MobilityDB SQL surface

Two parity axes, both audited from the repo, both with **all six type
families in scope** — `temporal`, `geo`, `cbuffer`, `npoint`, `pose`,
`rgeo`. No family is deferred or excluded from any headline.

| Axis | State | Gate |
|---|---|---|
| **Portable bare-name dialect** (RFC #920 — the cross-engine contract) | **29/29 canonical bare names registered, 0 unbacked, all six families — 100%** | [`scripts/portable_parity.py`](../scripts/portable_parity.py) |
| **MobilityDB SQL surface** (every `CREATE FUNCTION`, snake→camel match) | **1353/1462 active addressable covered (92.5%)** — the four sibling families now counted, not excluded | [`scripts/parity-audit.py`](../scripts/parity-audit.py) → [`parity-status.md`](parity-status.md) |

---

## Portable bare-name dialect (this is 100%)

Single source of truth: `MobilityDB/MEOS-API meta/portable-aliases.json`
(vendored read-only at [`meta/portable-aliases.json`](../meta/portable-aliases.json),
RFC #920, discussion MobilityDB#861, native in MobilityDB#1075). It maps
**29 SQL operator symbols to 29 portable bare function names**,
type-agnostically.

[`PortableOperatorAliasUDFs`](../src/main/java/org/mobilitydb/spark/portable/PortableOperatorAliasUDFs.java)
registers all 29 bare names as Spark-SQL UDFs, each **reusing the
operator's own existing backing field verbatim** (equivalence by
construction — the alias *is* the operator's backing, it cannot drift).
The backings chosen are the MEOS superclass entrypoints
(`*_temporal_temporal`, `*_tspatial_tspatial`, `t*_temporal_temporal`,
`tdistance_tgeo_tgeo`, `nad_tgeo_*`), which libmeos dispatches internally
for any temporal value carried in the type-erased hex-WKB string — so
`tcbuffer` / `tnpoint` / `tpose` / `trgeometry` are covered by
construction alongside `temporal` and `geo`. The type-qualified
operator spellings they supersede 1:1 (`temporalBefore`, `tnumberLeft`,
`teqTemporal`, …) are dropped — the bare name is the portable contract.

`python3 scripts/portable_parity.py` gates this: **29/29 backed, 0
unbacked, 100%** (same prefix logic as MobilityDB/MEOS-API
`portable_parity.py`).

---

## MobilityDB SQL surface (92.5% — remaining work, with a plan)

The full MobilityDB SQL surface is partitioned by `scripts/parity-audit.py`
into:

| Bucket | This release |
|--------|---|
| **Active addressable** (any function a Spark UDF can semantically reproduce — **all six families**) | **1353 / 1462 covered (92.5%)** |
| **Out of scope** (PG plumbing: `*_in/_out/_recv/_send`, `_transfn/_combinefn/_finalfn`, GiST/SPGiST opclasses, `_cmp/_eq/.../_hash`, PG range types, PG-extension-only entry points) | excluded by mechanism, not by family |

PostGIS `BOX2D`/`BOX3D` are *not* out of scope — PostGIS is embedded in
MEOS, so `box2d`/`box3d` UDFs are real.

**The remaining ~109 (7.5%) is tracked work, not a headline exclusion.**
It is dominated by:

- the per-overload typed UDF surface of `cbuffer` / `npoint` / `pose` /
  `rgeo` (circular buffers, network points, spatial poses, rigid
  geometries). These four are **full user-facing temporal types, in
  scope** — the portable bare-name dialect above already covers them via
  the superclass backings; the gap is the *typed per-family* UDF surface,
  to be filled with the same `MeosNative`/`functions.*` reuse pattern, no
  new operator logic.
- the previously-noted typed-Datum tile / split / value-set returns
  (`getValue`/`getValues`/`*SeqSetGaps`/`timeSplit` double-pointer arrays)
  and `segmentMaxDuration`/`asMVTGeom` multi-array outputs.

Re-run `python3 scripts/parity-audit.py --mdb ../MobilityDB --mspark .`
to regenerate [`parity-status.md`](parity-status.md); `DEFERRED_FAMILIES`
is empty by invariant (cbuffer/npoint/pose/rgeo never deferred).

---

## Why this matters for the ecosystem

- **One reference, every engine.** A user learns the 29 bare names once;
  MobilityDB (native, #1075), MobilityDuck, and MobilitySpark expose the
  identical dialect. The BerlinMOD Q1–Q17 portable SQL runs unchanged
  across all three.
- **Equivalence by construction.** Every alias reuses the operator's own
  backing C symbol — no reimplementation, no second code path, no drift.
- **Reusable audit.** `scripts/parity-audit.py` (surface) and
  `scripts/portable_parity.py` (dialect) regenerate from the repo and run
  in CI; both keep all six families in scope.

---

## How to verify

```bash
mvn test                                   # full unit suite (CI: Linux green)
python3 scripts/portable_parity.py         # 29/29, 0 unbacked  (exit 0)
python3 scripts/parity-audit.py \
    --mdb ../MobilityDB --mspark .          # regenerates docs/parity-status.md
```
