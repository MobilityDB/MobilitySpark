# JMEOS jar unification — MobilitySpark readiness

MobilitySpark and MobilityFlink converge on the single MEOS-API-generated JMEOS
surface `functions.GeneratedFunctions`, retiring the flat `functions.functions`
generator. JMEOS ships a thin `functions.functions` facade delegating to
`GeneratedFunctions` for call sites whose name and signature are unchanged.

Surface audit of MobilitySpark's 729 `functions.<method>` call sites (48 files)
against the unified `GeneratedFunctions` jar (JMEOS PR #19, pinned to
consolidation branch `regreen/951`):

- 0 signature deltas among methods present under the same name in both surfaces:
  the `bool`→`boolean` and `*_value_n`→DIRECT `(Pointer,int)→Pointer` lowering
  matches, so the facade is a transparent drop-in for those calls.
- 86 absent: th3index / h3index symbols (`Th3IndexUDFs` + `th3index_value_n`).
  The pin must fold in the th3index rollup (#1045/#976).
- 3 absent because of the `mult_*`→`mul_*` rename (#1120, in MobilityDB master):
  `mult_tfloat_float`, `mult_tint_int`, `mult_tnumber_tnumber` in
  `MathUDFs.java` adopt `mul_*` — MobilitySpark's edit, applied with the jar
  swap.
- 3 absent from the curated MEOS-API surface but used by MobilitySpark:
  `interval_in`, `meos_initialize_noexit_error_handler` (MEOS lifecycle, called
  in `MobilitySparkSession`), `tgeogpoint_great_circle_distance`. The unified
  `GeneratedFunctions` surface must retain/expose these, or MobilitySpark adapts
  to the replacement the IDL provides.

Execution once the pin carries th3index and the three helpers: replace
`libs/JMEOS-1.4.jar` with the unified jar, apply the `mult_*`→`mul_*` rename in
`MathUDFs.java`, run the full test suite (926 tests).
