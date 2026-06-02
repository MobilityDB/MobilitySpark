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

        // Pre-load any existing results so a partial run can be resumed
        // without losing previously collected timings.
        Map<String, List<Long>> timings = loadExistingTimings(outputPath);
        String version = "unknown";

        try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
            // Register BerlinMOD-specific UDFs (length, bboxOverlaps, valueAtTimestamp, etc.)
            BerlinMODUDFs.registerAll(spark);

            // Load and cache all tables — loading time is NOT in the query timings
            System.out.println("=== Loading data from: " + dataDir + " ===");
            BerlinMODDemo.loadFromCsvPublic(spark, dataDir);

            // Materialise the th3index column on Trips at load time.  Each trip's
            // tgeompoint is converted to a temporal H3 cell sequence at the
            // chosen resolution (default 7 ≈ 1.2 km cells).  The trip_h3 column
            // is used by the portable BerlinMOD SQL (Q4 / Q5 / Q6 / Q10) as a
            // spatial prefilter — a cheap cell-membership test that runs before
            // the expensive eIntersects / nearestApproachDistance / eDwithin /
            // tDwithin calls.  All three benchmarked platforms compute the
            // column at load time so the comparison is apples-to-apples; on
            // PostgreSQL the load script also adds a GiST index on the column
            // so the prefilter becomes a true index seek.
            //
            // Disabled when berlinmod.bench.th3index.disable=true so before-vs-
            // after measurement is reproducible without a rebuild — note that
            // disabling it makes the prefilter expressions in the portable SQL
            // reference a non-existent column, so use this only with a custom
            // SQL set that omits the prefilter.
            //
            // If the source CSV already carries a trip_h3 column (as produced
            // by berlinmod_portability_export() in MobilityDB-BerlinMOD), drop
            // it first so we can rematerialise at our chosen resolution — this
            // guarantees a consistent resolution across all three platforms
            // regardless of how the CSV was produced.
            // The tgeompointToTh3Index UDF (registered by Th3IndexUDFs via
            // MobilitySparkSession) materialises the column from each trip's
            // tgeompoint over the H3 public surface.  Default resolution 7
            // (~1.2 km cells).  This block is a no-op unless
            // berlinmod.bench.th3index.enable=true is set, so the before-vs-
            // after prefilter measurement is reproducible without a rebuild.
            {
                int res = Integer.getInteger("berlinmod.bench.th3index.resolution", 7);
                System.out.println("=== Materialising trip_h3 (res " + res + ") + trip_bbox prefilter columns ===");
                String[] cols = spark.table("Trips").schema().fieldNames();
                String selectCols = java.util.Arrays.stream(cols)
                    .filter(c -> !"trip_h3".equalsIgnoreCase(c) && !"trip_bbox".equalsIgnoreCase(c))
                    .collect(Collectors.joining(", "));
                // The trip_h3 (temporal H3 cell index) and trip_bbox (STBox of
                // the whole trip) columns are the cross-join spatial prefilter
                // mechanism: a cheap cell / bounding-box test that runs before
                // the expensive eIntersects / nearestApproachDistance / tDwithin
                // calls.  Materialising them once per trip means the prefilter
                // never re-parses the full multi-thousand-instant trip per pair.
                // Materialise via a Dataset rather than CREATE OR REPLACE
                // TEMPORARY VIEW Trips AS SELECT ... FROM Trips: the latter
                // trips Spark's checkCyclicViewReference (the new view names
                // itself in its own definition) and aborts with RECURSIVE_VIEW.
                // Reading the current Trips into a Dataset first resolves the
                // source plan, then the registration replaces the view atomically.
                spark.sql(
                    "SELECT " + selectCols + ", " +
                    "       tgeompointToTh3Index(trip, " + res + ") AS trip_h3, " +
                    "       expandSpace(trip, 0.0)             AS trip_bbox " +
                    "FROM Trips"
                ).createOrReplaceTempView("Trips");
            }

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
            //
            // Single canonical source: berlinmod/queries.sql holds every query,
            // each delimited by a `-- @query <id>` marker. All three runners
            // (PostgreSQL, DuckDB, Spark) split the same file, so the SQL cannot
            // drift between platforms. Spark applies preprocessForSpark as a
            // dialect transform; the query intent is identical across engines.
            Map<String, String> queries = splitQueries(
                    Files.readString(Paths.get(sqlDir, "queries.sql")));
            for (String q : queryList) {
                String raw = queries.get(q);
                if (raw == null) {
                    System.out.printf("  [skip] %s — not in queries.sql%n", q);
                    continue;
                }
                String sql = preprocessForSpark(stripComments(raw));
                List<Long> qTimes = new ArrayList<>(runs);

                System.out.printf("  timing %-6s: ", q);
                for (int run = 0; run < runs; run++) {
                    try {
                        long t0 = System.currentTimeMillis();
                        // Force full materialisation of every output column via the noop
                        // sink. count() alone lets Spark prune projection-only expressions
                        // (e.g. minDistance(arr, arr) in Q5), which would mistime the query.
                        spark.sql(sql).write().format("noop").mode("overwrite").save();
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

    /**
     * Split the canonical queries.sql into id → SQL, keyed by the
     * {@code -- @query <id>} markers. The text before the first marker (the
     * file header) is discarded. Insertion order is preserved.
     */
    private static Map<String, String> splitQueries(String text) {
        Map<String, String> out = new LinkedHashMap<>();
        String id = null;
        StringBuilder body = new StringBuilder();
        for (String line : text.split("\n")) {
            String t = line.stripLeading();
            if (t.startsWith("-- @query")) {
                if (id != null) out.put(id, body.toString());
                id = t.substring("-- @query".length()).trim();
                body.setLength(0);
            } else if (id != null) {
                body.append(line).append('\n');
            }
        }
        if (id != null) out.put(id, body.toString());
        return out;
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
     *   <li>th3index prefilter injection for {@code eIntersects(t.<col>, p.<col>)}
     *       on point geometries — see {@link #injectTh3IndexPrefilter}.</li>
     * </ol>
     */
    private static String preprocessForSpark(String sql) {
        // 1. Replace 2-arg stbox(geom, time_or_period) with geoTimeStbox(geom, time_or_period).
        //    The \b word boundary ensures stboxHasx etc. are not affected.
        //    The [^,)]+ / [^)]+ pattern matches exactly 2 args (comma present).
        sql = sql.replaceAll("\\bstbox\\(([^,)]+),\\s*([^)]+)\\)", "geoTimeStbox($1, $2)");

        // 2a. Route the spatial bounding-box prefilter to the materialised
        //     trip_bbox (STBox) column so the cross-join never re-parses the
        //     full trip.  expandSpace accepts an STBox hex directly, so feed it
        //     the precomputed trip_bbox (strictly inside expandSpace(...)).
        sql = sql.replaceAll("expandSpace\\(\\s*(\\w+)\\.trip\\s*,", "expandSpace($1.trip_bbox,");
        //     Spatial `trip && <stbox>` (geoTimeStbox / expandSpace RHS) →
        //     stboxOverlaps over the precomputed trip_bbox column.
        sql = sql.replaceAll("(\\w+)\\.trip\\s+&&\\s+((?:geoTimeStbox|expandSpace)\\(.+)",
                             "stboxOverlaps($1.trip_bbox, $2)");

        // 2b. Any remaining bounding-box overlap operator && (temporal-span
        //     RHS, e.g. trip && period) → the bboxOverlaps UDF on the trip.
        sql = sql.replaceAll("([\\w]+\\.[\\w]+)\\s+&&\\s+(.+)", "bboxOverlaps($1, $2)");

        // 3. Remove PostgreSQL :: cast to numeric (Spark ROUND handles DOUBLE natively).
        sql = sql.replace("::numeric", "");

        // 4. Replace PostGIS ST_Contains with the registered geomContains UDF.
        sql = sql.replace("ST_Contains(", "geomContains(");

        // 5. Normalise the th3index set-membership prefilter spelling to the
        //    registered camelCase UDF name.
        sql = sql.replace("everIntersectsH3IndexSet_Th3Index", "everIntersectsH3IndexSetTh3Index");

        // 6. Resolve the cross-join spatial prefilter through the materialised
        //    trip_bbox (STBox) column.  The th3index cell prefilter is a true
        //    index seek on PostgreSQL / DuckDB, but in Spark's string-storage
        //    model the trip_h3 sequence is re-parsed per candidate pair; the
        //    STBox bounding-box overlap is the cheap, coordinate-system-agnostic
        //    equivalent that the same dialect already uses via the `&&` operator
        //    (Q9 / Q11 / Q16).  Each rewrite is a sound spatial superset filter:
        //    the exact eIntersects / nearestApproachDistance still runs on the
        //    surviving pairs, so results are unchanged.
        //    everIntersectsH3IndexSetTh3Index(geoToH3IndexSet(g, r), t.trip_h3)
        //      → stboxOverlaps(t.trip_bbox, geoToStbox(g))
        sql = sql.replaceAll(
            "everIntersectsH3IndexSetTh3Index\\(geoToH3IndexSet\\(([^,]+),\\s*\\d+\\),\\s*(\\w+)\\.trip_h3\\)",
            "stboxOverlaps($2.trip_bbox, geoToStbox($1))");
        //    COALESCE(everEqH3IndexTh3Index(geomToH3Cell(g, r), t.trip_h3), TRUE)
        //      → stboxOverlaps(t.trip_bbox, geoToStbox(g))
        sql = sql.replaceAll(
            "COALESCE\\(everEqH3IndexTh3Index\\(geomToH3Cell\\(([^,]+),\\s*\\d+\\),\\s*(\\w+)\\.trip_h3\\),\\s*TRUE\\)",
            "stboxOverlaps($2.trip_bbox, geoToStbox($1))");
        //    everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)
        //      → stboxOverlaps(t1.trip_bbox, t2.trip_bbox)
        sql = sql.replaceAll(
            "everEqTh3IndexTh3Index\\((\\w+)\\.trip_h3,\\s*(\\w+)\\.trip_h3\\)",
            "stboxOverlaps($1.trip_bbox, $2.trip_bbox)");

        // 7. Set-set spatial-join SRF (#1148): the portable
        //    `, LATERAL <fn>(<args>) AS <alias>(<cols>)` table function (the
        //    PostgreSQL/DuckDB SRF form) becomes Spark's table generator
        //    `LATERAL VIEW inline(<fn>(<args>)) <alias> AS <cols>`.  The UDF
        //    returns array<struct<...>>; inline() expands each struct to a row.
        //    Consumes the preceding comma so the FROM-list item turns into a
        //    Spark lateral view.  Arg lists carry no nested parentheses.
        sql = sql.replaceAll(
            ",\\s*LATERAL\\s+(\\w+\\([^)]*\\))\\s+AS\\s+(\\w+)\\s*\\(([^)]*)\\)",
            " LATERAL VIEW inline($1) $2 AS $3");

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
}
