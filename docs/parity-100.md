# MobilitySpark — portable dialect parity & MobilityDB SQL surface

Two parity axes, both audited from the repo, both with **all six type
families in scope** — `temporal`, `geo`, `cbuffer`, `npoint`, `pose`,
`rgeo`. No family is deferred or excluded from any headline.

| Axis | State | Gate |
|---|---|---|
| **Portable bare-name dialect** (RFC #920 — the cross-engine contract) | **29/29 canonical bare names registered, 0 unbacked, all six families — 100%** | [`scripts/portable_parity.py`](../scripts/portable_parity.py) |
| **MobilityDB SQL surface** (every `CREATE FUNCTION`, snake→camel match) | **1571/1577 active addressable covered (99.6%)** — every section 100% except the `v_clip` ABI gap below; all six families counted | [`scripts/parity-audit.py`](../scripts/parity-audit.py) → [`parity-status.md`](parity-status.md) |

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

## MobilityDB SQL surface (99.6%)

The full MobilityDB SQL surface is partitioned by `scripts/parity-audit.py`
into:

| Bucket | This release |
|--------|---|
| **Active addressable** (any function a Spark UDF can semantically reproduce — **all six families**) | **1571 / 1577 covered (99.6%)** |
| **Out of scope** (PG plumbing: `*_in/_out/_recv/_send`, `_transfn/_combinefn/_finalfn`, GiST/SPGiST/GIN index opclasses, `_cmp/_eq/.../_hash`, PG range types, PG-extension-only entry points) | excluded by mechanism, not by family |

PostGIS `BOX2D`/`BOX3D` are *not* out of scope — PostGIS is embedded in
MEOS, so `box2d`/`box3d` UDFs are real.

The `cbuffer` / `npoint` / `pose` / `rgeo` typed per-family UDF surface is
implemented (`CbufferUDFs`, `NpointUDFs`, `PoseUDFs`, `RgeoUDFs`), reusing
each function's own MEOS C symbol via the `MeosNative` raw-FFI / generic
`functions.*` pattern — no reimplementation. Every active section is at
100% with two structural exceptions:

- **Index-plumbing exception (documented, uniform with temporal/geo).**
  The GiST/GIN opclass-support callbacks of
  `cbuffer/166_tcbuffer_indexes`, `npoint/092_tnpoint_gin`,
  `npoint/098_tnpoint_indexes`, `pose/114_tpose_indexes`,
  `rgeo/134_trgeo_indexes` are out of scope — the same PG-only index
  access-method class already excluded for `temporal/043_temporal_gist`,
  `geo/073_tgeo_gist`, etc. Index methods are PG-internal; Spark has no
  equivalent.

- **`rgeo/133_trgeo_vclip` — MEOS-library ABI gap (6 functions).**
  The `v_clip_*` user surface is implemented only in the `mobilitydb`
  PostgreSQL extension (`VClip_*` `MODULE_PATHNAME` wrappers), not in the
  MEOS C library MobilitySpark links. `lib/libmeos.so` exports only the
  low-level kernels `v_clip_tpoly_point` / `v_clip_tpoly_tpoly`, which
  take raw PostGIS `LWPOLY*`/`LWPOINT*`/`Pose*` structs plus
  out-parameters — there is no `GSERIALIZED`/`Temporal` entry point
  reachable from the hex-WKB string convention — and the other four
  (`v_clip_poly_point`, `v_clip_poly_poly`, `v_clip_tpoly_poly`,
  `v_clip_tpoly_tpoint`) are not exported at all. These six are recorded
  as a documented gap and intentionally **not stubbed**: binding them
  requires an `extern "C"` GSERIALIZED/Temporal-level v-clip entry point
  to be added to MEOS upstream (proposed fix: export
  `v_clip_trgeo_geo` / `v_clip_trgeo_trgeo` wrappers that accept
  `GSERIALIZED`/`Temporal` and a `TimestampTz`, returning the scalar
  clip distance — mirroring the existing `tdistance_trgeo_*` exports).

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
