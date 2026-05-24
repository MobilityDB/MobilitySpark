## BerlinMOD Portable SQL — Cross-Platform Benchmark

*Generated 2026-05-24 20:02 UTC*

### Machine

| Parameter | Value |
|---|---|
| CPU | Intel(R) Core(TM) Ultra 7 155H |
| Cores / threads | 22 cores / 22 threads |
| RAM | 15.4 GB |
| OS | Linux 5.15.167.4-microsoft-standard-WSL2 (WSL2) |

### Platforms

| Platform | Tier | Version | Vehicles | Trips | Runs |
|---|---|---|---|---|---|
| MobilityDB | 3 | MobilityDB1.4.0 on PostgreSQL 18.1  | 141 | 1620 | 3 |

### Query Timings (median wall-clock)

| Query | Description | MobilityDB |
|---|---|---|
| `q01` | Q1  — vehicle models (relational join) | 6 ms |
| `q02` | Q2  — ever entered region (eIntersects) | 20 ms |
| `q03` | Q3  — position at instant (atTime) | 9.66 s |
| `q04` | Q4  — ever passed point (eIntersects) | 16.42 s |
| `q05` | Q5  — min approach distance (nearestApproachDistance) | 52.64 s |
| `q06` | Q6  — truck pairs within 10 m (eDwithin) | 3.54 s |
| `q07` | Q7  — trip during period (atTime) | 136.43 s |
| `q08` | Q8  — trajectory geometry (trajectory) | 4.24 s |
| `qrt` | QRT — binary round-trip (asHexWKB) | 9.26 s |
| `q09` | Q9  — licence + region ever-intersect | 12.29 s |
| `q10` | Q10 — licence + point ever-intersect | 27.53 s |
| `q11` | Q11 — licence + period overlap | 13.72 s |
| `q12` | Q12 — vehicles ever in multiple regions | 14.00 s |
| `q13` | Q13 — pairs ever within 10 m | 20 ms |
| `q14` | Q14 — vehicles with max speed > threshold | 228 ms |
| `q15` | Q15 — distance travelled per vehicle | 5.13 s |
| `q16` | Q16 — vehicles present during each period | 1.12 s |
| `q17` | Q17 — aggregate: trips per vehicle type | 15.38 s |

### Notes

- All three platforms use **identical SQL** (no operator symbols — named functions only per the portable dialect in Discussion #861).
- Timings are wall-clock (client-side `date +%s%3N`), median of N runs. Data loading is excluded.
- MobilitySpark timings include Spark query planning overhead but **not** JVM startup (all queries run in a single Spark session).
- Queries marked `—` are not yet implemented on that platform.

To share your results, paste this table as a comment on https://github.com/MobilityDB/MobilityDB/discussions/913
