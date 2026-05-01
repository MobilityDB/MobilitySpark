# Bumping MobilitySpark to JMEOS 1.3

This document captures the staged plan for upgrading MobilitySpark to JMEOS 1.3 (the JMEOS bindings that wrap the **MEOS C API 1.3**). It accompanies the tracking issue [#1](https://github.com/MobilityDB/MobilitySpark/issues/1).

## What "JMEOS 1.3" is

It is *not* a JMEOS Maven-version bump — JMEOS is not published on Maven Central, and its `pom.xml` keeps `<version>1.0-SNAPSHOT</version>`. The "1.3" refers to the version of the underlying **MEOS C library** that the bindings wrap.

The corresponding upstream work is [JMEOS PR #8](https://github.com/MobilityDB/JMEOS/pull/8), which lives on the fork branch [`SachaDelsaux/JMEOS:JMEOS_v1.3`](https://github.com/SachaDelsaux/JMEOS/tree/JMEOS_v1.3) and is currently OPEN, BLOCKED on review.

## How MobilitySpark consumes JMEOS today

The integration is JAR-based, not Maven-based. The two vendored JARs in [`libs/`](../libs/) are what the build uses:

| File | Size | What's inside |
|---|---|---|
| `libs/MobilityDB-JMEOS.jar` | 3.9 MB | Pure-Java JMEOS classes |
| `libs/MobilityDB-JMEOS-Linux.jar` | 21 MB | JMEOS classes + bundled MEOS native library (`libmeos.so` + dependencies) |

The `pom.xml` carries this dependency block:

```xml
<dependency>
    <groupId>org.jmeos</groupId>
    <artifactId>meos</artifactId>      <!-- typo: JMEOS publishes as 'jmeos' -->
    <version>1.0</version>             <!-- not on any Maven repo -->
</dependency>
```

This declaration does not resolve from Maven Central or any other public repo. Build setups typically cope by either (a) running `mvn install:install-file` once to install the vendored JAR into the local Maven cache, or (b) configuring IntelliJ to pick up the JARs from `libs/` directly.

## Stages

### Stage 0 — JMEOS PR #8 merges (external blocker)

Status: **OPEN, BLOCKED on review** since 2025-10-10.

The MEOS-1.3-compatible JMEOS bindings only exist on a fork branch. Until PR #8 lands on `MobilityDB/JMEOS:main`, MobilitySpark has no upstream JMEOS to pull from.

Two cleanup items in PR #8 to flag back to upstream:
- The `hs_err_pid17967.log` JVM-crash binary should not be committed.
- The `scale10.csv` (~50 MB) test fixture — PR #8's own description notes this should move to Git LFS or be excluded.

### Stage 1 — Build new JMEOS JARs (~30 min)

After PR #8 merges, build:
- `MobilityDB-JMEOS.jar` — pure-Java JMEOS package.
- `MobilityDB-JMEOS-Linux.jar` — JMEOS package + bundled MEOS 1.3 native library.

Requires a Linux build environment with MEOS 1.3 installed (via `cmake -D MEOS=ON .. && make && sudo make install` from a known-good `MobilityDB/MobilityDB` source state — confirm the MEOS 1.3 release tag before building).

### Stage 2 — Replace JARs in MobilitySpark (~30 min)

- Replace the two JARs in `libs/`.
- Optionally fix the `pom.xml` artefactId typo (`meos` → `jmeos`) and bump the version coordinate to match. This is cosmetic since the artefact does not resolve from any Maven repo, but it documents intent. **Do not change before confirming with the maintainers** that current build setups (especially `mvn install:install-file` workflows) won't break.

### Stage 3 — Verify (~30 min)

- `mvn clean install` should compile cleanly. The MobilitySpark Java code uses only two JMEOS imports (`meos_initialize` and `meos_finalize`), both of which are preserved in PR #8's `functions.java` — so API-compat risk is effectively zero.
- Run the canary path: instantiate the Spark UDTs registered in MobilitySpark, exercise a simple `tfloat` / `tgeompoint` value, verify round-trip.

### Stage 4 — Update README (~15 min)

The README's MEOS install instructions currently say:

```bash
git clone https://github.com/MobilityDB/MobilityDB
mkdir MobilityDB/build && cd MobilityDB/build
cmake -D MEOS=ON .. && make && sudo make install
```

Pin to the MEOS 1.3 release tag explicitly so future contributors get a reproducible build:

```bash
git clone --branch <MEOS-1.3-tag> https://github.com/MobilityDB/MobilityDB
...
```

Update the Prerequisites section to read **JMEOS 1.3** (currently says "JMEOS working version").

## Effort summary

| Stage | Effort | Cumulative |
|---|---|---|
| 0. PR #8 merges | external | — |
| 1. Build JMEOS 1.3 JARs | ~30 min | 30 min |
| 2. Swap JARs + pom hygiene | ~30 min | 1 h |
| 3. Verify build + canary | ~30 min | 1 h 30 min |
| 4. README pin | ~15 min | **~2 hours** |

## Cross-references

- Tracking issue: [#1](https://github.com/MobilityDB/MobilitySpark/issues/1)
- Upstream JMEOS PR: [JMEOS#8](https://github.com/MobilityDB/JMEOS/pull/8)
- JMEOS v1.3 fork branch: [SachaDelsaux/JMEOS:JMEOS_v1.3](https://github.com/SachaDelsaux/JMEOS/tree/JMEOS_v1.3)
- MobilityDB main project (for MEOS 1.3 native library): [MobilityDB/MobilityDB](https://github.com/MobilityDB/MobilityDB)
