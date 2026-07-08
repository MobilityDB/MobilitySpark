# MobilitySpark generation

MobilitySpark's UDF surface is generated, not hand-written. This document is the contract for how.

## Policy

Every MobilityDB language/surface binding is a projection of the MEOS-API catalog, and each
binding owns its generator in its own repo. The source of truth is the catalog
(`meos-idl.json`, generated from the MEOS C headers), tracked to MobilityDB master.

## MobilitySpark is a consumer binding

MobilitySpark binds the **JMEOS jar** (the JVM FFI projection of the catalog), not MEOS-API
directly. Its generator **`tools/codegen_spark_udfs.py`** mirrors the JMEOS
`FunctionsGenerator`: it reads the JMEOS surface and the catalog's `@sqlfn` names and emits the
Spark UDF registration layer, organized **by `@ingroup` group** (one unit per group, the same
structure as the reference manual). Every emitted `register()` is preceded by the per-thread
MEOS-init guard, enforced at build time.

## Inputs

The generator reads two vendored inputs, both tracked to MobilityDB master:

- the MEOS-API catalog `tools/meos-idl.json`, and
- the JMEOS jar `libs/JMEOS.jar` (its `javap` surface).

The UDF layer is the generated `registerAll()` — zero hand-written registrations.
