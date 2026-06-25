# MobilitySpark generation — the canonical per-binding generator policy

This document is the contract for how MobilitySpark is generated, under the ecosystem-wide
per-binding generator policy.

## The policy (ecosystem-wide)

Every MobilityDB language/surface binding is a **pure projection of the MEOS-API catalog**,
and **each binding owns its own generator, in its own repo**, in a canonical layout. The
single source of truth is the **catalog** (`MEOS-API/output/meos-idl.json`, generated from
the MEOS C headers). A binding is an independent, plug-and-play module that owns its
generation.

Each binding repo satisfies the same invariants: in-repo generator; own
`tools/pin/compose-order.txt`; pinned catalog/jar input; thin language projection
(language-neutral decisions live in the catalog); full automation toward a zero-hand-written
surface (generate-then-retire; the last green-CI version is the equivalence probe).

## MobilitySpark scope: generated Spark UDFs over the JMEOS surface

MobilitySpark is a **consumer** binding: it binds the **JMEOS jar** (the JVM FFI projection
of the catalog), not MEOS-API directly. Its generator **`tools/codegen_spark_udfs.py`**
mirrors the JMEOS `FunctionsGenerator`: it reads the JMEOS surface (and the catalog's
`@sqlfn` names) and emits the Spark UDF registration layer, organized **by `@ingroup` group**
(one unit per group, the same structure as the reference manual). The generator enforces its
own regularity invariant at build time (every emitted `register()` is preceded by the
per-thread MEOS-init guard).

## Generate-then-retire — the green-CI version is the probe

The hand-written `*UDFs.java` registrations are replaced by the generated surface **family
by family, never wipe-first**: generate, build green, **prove generated ⊇ hand** against the
**last green-CI version** (the test suite + the BerlinMOD benchmark), then retire the hand
registrations. End state: the UDF layer is the generated `registerAll()` — zero hand-written
registrations.

## Pinning

The JMEOS jar (and through it the catalog) is pinned to a MobilityDB `ecosystem-pin-*` /
deliverable-PR head. That pin is the *catalog/surface* input; MobilitySpark's own
`tools/pin/compose-order.txt` governs *this repo's* PR accumulate.
