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
 * The SQL files come from the canonical {@code berlinmod-portability}
 * suite, vendored as the {@code berlinmod/suite/} git submodule (the single
 * source shared by MobilityDB, MobilityDuck, and MobilitySpark).  Override
 * the location with the system property {@code berlinmod.sql.dir}; the
 * runner {@code bench/bench_mspark.sh} sets it to {@code berlinmod/suite}.
 * When unset it falls back to the parent of the data directory.
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

        // Pre-load any existing results so a partial run can be resumed
        // without losing previously collected timings.
        Map<String, List<Long>> timings = loadExistingTimings(outputPath);
        String version = "unknown";

        try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
            // MobilitySparkSession.create registers the full catalog-generated surface
            // (length, valueAtTimestamp, expandSpace, ... are @sqlfn names on it).

            // Load and cache all tables — loading time is NOT in the query timings
            System.out.println("=== Loading data from: " + dataDir + " ===");
            loadFromCsv(spark, dataDir);

            // The trip_h3 column (temporal H3 cell sequence used by the portable
            // BerlinMOD SQL as a spatial prefilter for the Trips×Trips queries) is
            // part of the canonical data produced once by
            // berlinmod_portability_export(); it is loaded from the CSV, not
            // recomputed here, so every engine consumes the identical column.

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
            // Every engine runs the identical canonical <query>.sql with bare
            // portable names (no Spark-specific variant, no SQL rewriting).
            for (String q : queryList) {
                Path sqlFile = Paths.get(sqlDir, q + ".sql");
                if (!Files.exists(sqlFile)) {
                    System.out.printf("  [skip] %s — SQL file not found%n", q);
                    continue;
                }
                String sql = stripComments(Files.readString(sqlFile));
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
                        String msg = e.getMessage();
                        if (msg != null) msg = msg.split("\n")[0].substring(0, Math.min(120, msg.split("\n")[0].length()));
                        System.out.printf("[err:%s: %s] ", e.getClass().getSimpleName(), msg);
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
        sb.append("  \"tier\": 1,\n");
        sb.append("  \"data_vehicles\": ").append(vehicles).append(",\n");
        sb.append("  \"data_trips\": ").append(trips).append(",\n");
        sb.append("  \"runs\": ").append(runs).append(",\n");
        sb.append("  \"timestamp\": \"").append(Instant.now().toString()).append("\",\n");
        sb.append("  \"queries\": {\n");

        // Emit in canonical QUERY_ORDER; any extra keys follow at the end.
        List<String> order = new ArrayList<>(java.util.Arrays.asList(QUERY_ORDER));
        for (String k : timings.keySet()) { if (!order.contains(k)) order.add(k); }

        boolean firstQ = true;
        for (String q : order) {
            if (!timings.containsKey(q)) continue;
            if (!firstQ) sb.append(",\n");
            firstQ = false;
            sb.append("    \"").append(q).append("\": [");
            sb.append(timings.get(q).stream()
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
     * Load timings from an existing JSON results file, returning them in
     * canonical QUERY_ORDER.  Returns an empty map if the file does not exist
     * or cannot be parsed.
     */
    private static Map<String, List<Long>> loadExistingTimings(String outputPath) {
        Map<String, List<Long>> raw = new LinkedHashMap<>();
        Path p = Paths.get(outputPath);
        if (!Files.exists(p)) return raw;
        try {
            String json = Files.readString(p);
            java.util.regex.Pattern qPat =
                java.util.regex.Pattern.compile("\"(q\\w+)\":\\s*\\[([^\\]]+)\\]");
            java.util.regex.Matcher m = qPat.matcher(json);
            while (m.find()) {
                String key = m.group(1);
                List<Long> vals = new ArrayList<>();
                for (String v : m.group(2).split(",")) {
                    try { vals.add(Long.parseLong(v.trim())); }
                    catch (NumberFormatException ignored) {}
                }
                if (!vals.isEmpty()) raw.put(key, vals);
            }
        } catch (IOException e) {
            System.err.println("  [warn] could not load existing timings: " + e.getMessage());
        }
        // Re-order to QUERY_ORDER (LinkedHashMap preserves insertion order)
        Map<String, List<Long>> ordered = new LinkedHashMap<>();
        for (String q : QUERY_ORDER) {
            if (raw.containsKey(q)) ordered.put(q, raw.get(q));
        }
        return ordered;
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

    /**
     * Load the canonical BerlinMOD CSV corpus (produced by the single shared
     * generator {@code setup/generate_data.sh} -> MobilityDB-BerlinMOD) into the
     * temp views the canonical {@code berlinmod/suite} queries reference. Trips are
     * hex-WKB strings the generated UDFs parse via temporal_from_hexwkb.
     */
    static void loadFromCsv(SparkSession spark, String dataDir) {
        String dir = dataDir.endsWith("/") ? dataDir : dataDir + "/";

        spark.read().option("header", "true").option("inferSchema", "true")
             .csv(dir + "vehicles.csv").createOrReplaceTempView("Vehicles");
        spark.read().option("header", "true").option("inferSchema", "true")
             .csv(dir + "trips.csv").createOrReplaceTempView("Trips");
        spark.read().option("header", "true").option("inferSchema", "true")
             .csv(dir + "query_licences.csv").createOrReplaceTempView("QueryLicences");

        spark.read().option("header", "true")
             .csv(dir + "query_instants.csv").createOrReplaceTempView("QueryInstantsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryInstants AS "
                + "SELECT CAST(instantid AS INT) AS instantId, "
                + "CAST(instant AS TIMESTAMP) AS instant FROM QueryInstantsRaw").count();

        spark.read().option("header", "true")
             .csv(dir + "query_points.csv").createOrReplaceTempView("QueryPointsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryPoints AS "
                + "SELECT CAST(pointid AS INT) AS pointId, geom, geom AS geomWKT "
                + "FROM QueryPointsRaw").count();

        spark.read().option("header", "true")
             .csv(dir + "query_regions.csv").createOrReplaceTempView("QueryRegionsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryRegions AS "
                + "SELECT CAST(regionid AS INT) AS regionId, geom FROM QueryRegionsRaw").count();

        spark.read().option("header", "true")
             .csv(dir + "query_periods.csv").createOrReplaceTempView("QueryPeriodsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryPeriods AS "
                + "SELECT CAST(periodid AS INT) AS periodId, period FROM QueryPeriodsRaw").count();
    }
}
