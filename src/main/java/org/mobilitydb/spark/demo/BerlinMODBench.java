/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby retained provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES,
 * INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS
 * TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark.demo;

import org.apache.spark.sql.SparkSession;
import org.mobilitydb.spark.MobilitySparkSession;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BerlinMOD benchmark runner for MobilitySpark.
 *
 * Loads the shared CSV dataset once (caching tables in Spark), then runs each
 * BerlinMOD portable SQL query {@code runs} times and records wall-clock time
 * per run.  Writes a JSON file with per-query timing lists, compatible with
 * the bench/report.py report generator.
 *
 * Usage:
 * <pre>
 *   spark-submit --class org.mobilitydb.spark.demo.BerlinMODBench \
 *       target/mobilityspark-*-spark.jar  data_dir  output.json  [runs]
 * </pre>
 *
 * Args:
 *   data_dir   — directory containing vehicles.csv, trips.csv, …
 *   output     — path to write the timing JSON file
 *   runs       — number of timed runs per query (default: 3)
 *
 * The SQL files are read from the same directory that contains this JAR's
 * class path.  If running from the repository root, pass
 * {@code berlinmod/} as the SQL directory via the system property
 * {@code berlinmod.sql.dir} (default: {@code berlinmod/}).
 */
public final class BerlinMODBench {

    private static final String[] QUERY_ORDER = {
        "q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "qrt",
        "q09", "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q17"
    };

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: BerlinMODBench <data_dir> <output.json> [runs] [queries]");
            System.err.println("  queries — page-range syntax: '3', '2-5', 'q02', 'q02-q05', 'qrt'");
            System.exit(1);
        }
        String dataDir    = args[0];
        String outputPath = args[1];
        int    runs       = args.length >= 3 ? Integer.parseInt(args[2]) : 3;
        String queryRange = args.length >= 4 ? args[3] : null;

        // Resolve which queries to run from the page-range argument.
        // Accepted forms: "3", "2-5", "q02", "q02-q05", "qrt", "q04,qrt,q07"
        List<String> queryList = resolveQueryRange(queryRange);

        // SQL files live next to the berlinmod/data/ directory
        String sqlDir = Paths.get(dataDir).getParent() != null
                ? Paths.get(dataDir).getParent().toString()
                : ".";
        // Allow override via system property
        sqlDir = System.getProperty("berlinmod.sql.dir", sqlDir);

        SparkSession spark = SparkSession.builder()
                .appName("MobilitySpark — BerlinMOD Benchmark")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== BerlinMODBench: " + runs + " run(s) per query ===");

        Map<String, List<Long>> timings = new LinkedHashMap<>();
        String version = "unknown";

        try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
            // Register BerlinMOD-specific UDFs (length, bboxOverlaps, valueAtTimestamp, etc.)
            BerlinMODUDFs.registerAll(spark);

            // Load and cache all tables — loading time is NOT in the query timings
            System.out.println("=== Loading data from: " + dataDir + " ===");
            BerlinMODDemo.loadFromCsvPublic(spark, dataDir);
            spark.catalog().cacheTable("Vehicles");
            spark.catalog().cacheTable("Trips");
            spark.catalog().cacheTable("QueryLicences");
            spark.catalog().cacheTable("QueryInstants");
            spark.catalog().cacheTable("QueryPoints");
            spark.catalog().cacheTable("QueryRegions");
            spark.catalog().cacheTable("QueryPeriods");
            // Warm up the cache
            spark.sql("SELECT count(*) FROM Trips").collect();
            System.out.println("    done.");

            // Try to capture version
            try {
                version = spark.sql("SELECT mobilitydb_version()")
                        .collectAsList().get(0).getString(0)
                        + " on Spark " + spark.version();
            } catch (Exception e) {
                version = "MobilitySpark on Spark " + spark.version();
            }

            // Time each query — flush results after every query so a crash
            // still leaves a valid JSON file with the timings collected so far.
            for (String q : queryList) {
                Path sqlFile = Paths.get(sqlDir, q + ".sql");
                if (!Files.exists(sqlFile)) {
                    System.out.printf("  [skip] %s — SQL file not found%n", q);
                    continue;
                }
                String sql = preprocessForSpark(stripComments(Files.readString(sqlFile)));
                List<Long> qTimes = new ArrayList<>(runs);

                System.out.printf("  timing %-6s: ", q);
                for (int run = 0; run < runs; run++) {
                    try {
                        long t0 = System.currentTimeMillis();
                        spark.sql(sql).count();          // forces full evaluation
                        long elapsed = System.currentTimeMillis() - t0;
                        qTimes.add(elapsed);
                        System.out.printf("%d ", elapsed);
                    } catch (Exception e) {
                        System.out.printf("[err:%s] ", e.getClass().getSimpleName());
                    }
                }
                System.out.println("ms");
                if (!qTimes.isEmpty()) {
                    timings.put(q, qTimes);
                    writeJson(outputPath, version, dataDir, runs, timings);
                }
            }
        } finally {
            spark.stop();
        }

        System.out.println("=== Results written to " + outputPath + " ===");
    }

    /** Strip leading-comment lines so Spark SQL doesn't choke on them. */
    private static String stripComments(String sql) {
        return Stream.of(sql.split("\n"))
                .filter(line -> !line.stripLeading().startsWith("--"))
                .collect(Collectors.joining("\n"))
                .replaceAll(";\\s*$", "");
    }

    /**
     * Rewrite portable BerlinMOD SQL to Spark-compatible SQL.
     *
     * Spark SQL cannot define custom infix operators, so the portable SQL's
     * {@code &&} bounding-box overlap operator and PostgreSQL-specific cast
     * syntax ({@code ::numeric}) must be rewritten before passing to
     * {@link SparkSession#sql}.  Transformations applied in order:
     * <ol>
     *   <li>{@code stbox(geom, t)} → {@code geoTimeStbox(geom, t)} (2-arg form only)</li>
     *   <li>{@code expr && expr2} → {@code bboxOverlaps(expr, expr2)} (per line)</li>
     *   <li>{@code ::numeric} → removed (Spark ROUND accepts DOUBLE directly)</li>
     *   <li>{@code ST_Contains(} → {@code geomContains(}</li>
     * </ol>
     */
    private static String preprocessForSpark(String sql) {
        // 1. Replace 2-arg stbox(geom, time_or_period) with geoTimeStbox(geom, time_or_period).
        //    The \b word boundary ensures stboxHasx etc. are not affected.
        //    The [^,)]+ / [^)]+ pattern matches exactly 2 args (comma present).
        sql = sql.replaceAll("\\bstbox\\(([^,)]+),\\s*([^)]+)\\)", "geoTimeStbox($1, $2)");

        // 2. Replace bounding-box overlap operator && with the bboxOverlaps UDF.
        //    Pattern: table.column && right_hand_expr (rest of line).
        sql = sql.replaceAll("([\\w]+\\.[\\w]+)\\s+&&\\s+(.+)", "bboxOverlaps($1, $2)");

        // 3. Remove PostgreSQL :: cast to numeric (Spark ROUND handles DOUBLE natively).
        sql = sql.replace("::numeric", "");

        // 4. Replace PostGIS ST_Contains with the registered geomContains UDF.
        sql = sql.replace("ST_Contains(", "geomContains(");

        return sql;
    }

    /** Write a JSON result file compatible with report.py. */
    private static void writeJson(String outputPath,
                                   String version,
                                   String dataDir,
                                   int runs,
                                   Map<String, List<Long>> timings) throws IOException {
        // Count trips and vehicles from the timing data indirectly — just
        // report what we know from disk.
        long trips    = countLines(Paths.get(dataDir, "trips.csv"))    - 1; // minus header
        long vehicles = countLines(Paths.get(dataDir, "vehicles.csv")) - 1;

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"platform\": \"mobilityspark\",\n");
        sb.append("  \"version\": \"").append(escapeJson(version)).append("\",\n");
        sb.append("  \"data_vehicles\": ").append(vehicles).append(",\n");
        sb.append("  \"data_trips\": ").append(trips).append(",\n");
        sb.append("  \"runs\": ").append(runs).append(",\n");
        sb.append("  \"timestamp\": \"").append(Instant.now().toString()).append("\",\n");
        sb.append("  \"queries\": {\n");

        boolean firstQ = true;
        for (Map.Entry<String, List<Long>> e : timings.entrySet()) {
            if (!firstQ) sb.append(",\n");
            firstQ = false;
            sb.append("    \"").append(e.getKey()).append("\": [");
            sb.append(e.getValue().stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(", ")));
            sb.append("]");
        }
        sb.append("\n  }\n}\n");

        // Atomic write: write to a .tmp file then rename so a crash mid-write
        // never leaves a truncated JSON (the previous file stays intact).
        Path dest = Paths.get(outputPath);
        Path tmp  = dest.resolveSibling(dest.getFileName() + ".tmp");
        Files.writeString(tmp, sb.toString());
        Files.move(tmp, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                               java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private static long countLines(Path path) {
        try (Stream<String> lines = Files.lines(path)) {
            return lines.count();
        } catch (IOException e) {
            return 0;
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Resolve a page-range style query selector to a list of query IDs.
     *
     * Accepted forms (case-insensitive):
     *   null / "" / "all"       → all 18 queries in canonical order
     *   "3"                     → ["q03"]
     *   "2-5"                   → ["q02","q03","q04","q05"]
     *   "q02"                   → ["q02"]
     *   "q02-q05"               → ["q02","q03","q04","q05"]
     *   "qrt"                   → ["qrt"]
     *   "q04,qrt,q07"           → ["q04","qrt","q07"]
     */
    private static List<String> resolveQueryRange(String spec) {
        List<String> all = java.util.Arrays.asList(QUERY_ORDER);
        if (spec == null || spec.isBlank() || spec.equalsIgnoreCase("all")) {
            return all;
        }

        List<String> result = new ArrayList<>();
        for (String token : spec.split(",")) {
            token = token.strip();
            if (token.contains("-")) {
                String[] parts = token.split("-", 2);
                int from = parseQueryIndex(parts[0].strip(), all);
                int to   = parseQueryIndex(parts[1].strip(), all);
                if (from < 0 || to < 0) {
                    throw new IllegalArgumentException("Unknown query in range: " + token);
                }
                int lo = Math.min(from, to), hi = Math.max(from, to);
                result.addAll(all.subList(lo, hi + 1));
            } else {
                int idx = parseQueryIndex(token, all);
                if (idx < 0) {
                    throw new IllegalArgumentException("Unknown query: " + token);
                }
                result.add(all.get(idx));
            }
        }
        return result;
    }

    /**
     * Parse a query token to its 0-based index in the canonical order.
     * Accepts "q03", "qrt", or bare numbers like "3" (1-based, so "1"→q01).
     * Returns -1 if not found.
     */
    private static int parseQueryIndex(String token, List<String> all) {
        String lower = token.toLowerCase(java.util.Locale.ROOT);
        // Direct match: "q02", "qrt"
        int idx = all.indexOf(lower);
        if (idx >= 0) return idx;
        // Bare number: "3" → "q03"
        try {
            int n = Integer.parseInt(lower);
            String padded = String.format("q%02d", n);
            idx = all.indexOf(padded);
            return idx;
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
