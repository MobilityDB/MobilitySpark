## BerlinMOD Portable SQL — Cross-Platform Benchmark

*Generated 2026-05-25 05:50 UTC*

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
| `q01` | Q1  — vehicle models (relational join) | 5 ms |
| `q02` | Q2  — ever entered region (eIntersects) | 19.93 s |
| `q03` | Q3  — position at instant (atTime) | 9.15 s |
| `q04` | Q4  — ever passed point (eIntersects) | 15.57 s |
| `q05` | Q5  — min approach distance (nearestApproachDistance) | 51.82 s |
| `q06` | Q6  — truck pairs within 10 m (eDwithin) | 3.46 s |
| `q07` | Q7  — trip during period (atTime) | 132.32 s |
| `q08` | Q8  — trajectory geometry (trajectory) | 4.35 s |
| `qrt` | QRT — binary round-trip (asHexWKB) | 9.87 s |
| `q09` | Q9  — licence + region ever-intersect | 13.44 s |
| `q10` | Q10 — licence + point ever-intersect | 27.74 s |
| `q11` | Q11 — licence + period overlap | 27.14 s |
| `q12` | Q12 — vehicles ever in multiple regions | 25.22 s |
| `q13` | Q13 — pairs ever within 10 m | 9.15 s |
| `q14` | Q14 — vehicles with max speed > threshold | 17.68 s |
| `q15` | Q15 — distance travelled per vehicle | 8.91 s |
| `q16` | Q16 — vehicles present during each period | 68.78 s |
| `q17` | Q17 — aggregate: trips per vehicle type | 24.12 s |

### Notes

- All three platforms use **identical SQL** (no operator symbols — named functions only per the portable dialect in Discussion #861).
- Timings are wall-clock (client-side `date +%s%3N`), median of N runs. Data loading is excluded.
- MobilitySpark timings include Spark query planning overhead but **not** JVM startup (all queries run in a single Spark session).
- Queries marked `—` are not yet implemented on that platform.

To share your results, paste this table as a comment on https://github.com/MobilityDB/MobilityDB/discussions/913
