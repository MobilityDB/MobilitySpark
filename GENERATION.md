# MobilitySpark generation

MobilitySpark's UDF surface is generated, not hand-written. This document is the contract for how.

## Policy

Every MobilityDB language/surface binding is a projection of the MEOS-API catalog, and each
binding owns its generator in its own repo. The source of truth is the catalog
(`meos-idl.json`, generated from the MEOS C headers), tracked to MobilityDB master.

## MobilitySpark is a consumer binding

MobilitySpark binds the **JMEOS jar** (the JVM FFI projection of the catalog), not MEOS-API
directly. Its generator is the shared **`tools/codegen_jvm.py --engine spark`**, the single
generator vendored identically by every JVM binding (MobilitySpark, MobilityFlink,
MobilityKafka). The `spark` engine delegates verbatim to the sibling
**`tools/codegen_spark_udfs.py`** (which mirrors the JMEOS `FunctionsGenerator`): it reads the
JMEOS surface and the catalog's `@sqlfn` names and emits the Spark UDF registration layer,
organized **by `@ingroup` group** (one unit per group, the same structure as the reference
manual). Every emitted `register()` is preceded by the per-thread MEOS-init guard, enforced at
build time. Sharing one generator across the JVM bindings is what keeps the surface from
drifting between engines.

## Inputs

The generator reads two inputs, both derived in CI from upstream MobilityDB master:

- the MEOS-API catalog `tools/meos-idl.json`, derived by the shared `provision-meos`
  action (`mobilitydb-ref: master`), and
- the JMEOS jar `org.jmeos:meos` (its `javap` surface), built from JMEOS `main` against
  that same master catalog and libmeos.

Tracking master keeps the source and the `@master` derivation toolchain moving together,
so the catalog and jar never drift from the `run.py` that derives them.

The UDF layer is the generated `registerAll()` — zero hand-written registrations.
