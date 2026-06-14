# INTEGRATION BRANCH NOTE — MEOS / JMEOS pin

`integration/berlinmod-bench` builds against ecosystem pin
**`ecosystem-pin-2026-06-11p`**.

- **`libs/JMEOS-1.4.jar`** is the canonical JMEOS regen at that pin
  (JMEOS PR #19): a single generated `functions.GeneratedFunctions`
  surface. The legacy hand-rolled `functions.functions` facade is retired,
  and every UDF binds the generated surface directly.
- **`lib/libmeos.so`** is built from the pin with `-DH3=ON` and the
  CBUFFER / NPOINT / POSE / RGEO families, so the th3index family is backed
  with no build-time special-casing.
- **CI** (`.github/workflows/maven.yml`) builds `libmeos` from the pin on
  Linux and macOS (with H3). The Windows job is non-blocking until the
  `MEOS_TZDATA_DIR` cmake option lands in the pin (it currently lives only on
  the `meos-windows-bootstrap` branch); once folded, Windows repoints to the
  pin like the other platforms.

The full unit suite is green (907/907).
