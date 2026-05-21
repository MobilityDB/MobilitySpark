# INTEGRATION BRANCH NOTE — JMEOS regen needed for Th3IndexUDFs

The accumulated integration branch (integration/berlinmod-bench) carries
PR #11 (Th3IndexUDFs full API, 86 UDFs) which calls JMEOS methods like
`always_eq_th3index_h3index` / `always_ne_th3index_h3index`.  Neither the
bundled JMEOS-1.4.jar nor JMEOS-1.5.jar exposes these — they require a
JMEOS regeneration against MEOS-with-th3index.

For the parallel benchmark task: either
  (a) replace libs/JMEOS-1.5.jar with a regen built against the MEOS pin
      that includes the th3index module (estebanzimanyi/MobilityDB:
      fix/meos-pg-symbol-collision-plus-h3 @ b183b12ee692, OR
      split/th3index-stacked, OR integration/meos-1.4-bump merged with #1045),
  (b) comment-out the th3index method call-sites in Th3IndexUDFs.java
      (lines around 498, 540, 580, etc.) — disables only the h3-cell
      h3-cell join queries (Tier-1 Spark column-store prefilter), the
      Trips × Trips queries Q5/Q6/Q10/Q16 still run via portable SQL +
      BROADCAST hints.

Option (a) is the right fix and is the JMEOS-session's standing task.
