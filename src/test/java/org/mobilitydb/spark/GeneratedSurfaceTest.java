package org.mobilitydb.spark;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.spark.generated.GeneratedSpatioTemporalUDFs;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runtime verification that the catalog-GENERATED UDF surface actually binds and
 * executes against libmeos through JMEOS — the safety gate before the hand-written
 * UDF layers (PRs #22/#24/#25/#26) are retired. Exercises a representative set of
 * generated 1:1 UDFs (scalar results + I/O) across families, driven by a known
 * tint hex-WKB ([1@2001-01-01, 2@2001-01-02, 1@2001-01-03]).
 */
class GeneratedSurfaceTest {

    // [1@2001-01-01, 2@2001-01-02, 1@2001-01-03] as MEOS hex-WKB (variant 0).
    private static final String TINT_HEX =
        "0123000A030000000301000000009C57D3C11C00000200000000FC2EF1D51C000001000000005C060FEA1C0000";

    private static SparkSession spark;
    private static Map<String, String> byOperator;

    @BeforeAll
    static void setup() throws Exception {
        spark = SparkSession.builder().appName("gen-verify").master("local[1]")
                .config("spark.ui.enabled", "false").getOrCreate();
        GeneratedSpatioTemporalUDFs.registerAll(spark);
        // The operator->bare-name dialect, read from the SAME catalog the generator emits
        // from, so a dialect rename (e.g. ?= ever_eq->eEq, <-> tdistance->tDistance) updates
        // this test automatically instead of hard-coding the names. byOperator is a flat
        // string->string map; parse it directly (Spark's bundled jackson is version-skewed).
        byOperator = new HashMap<>();
        String json = Files.readString(Paths.get("tools/meos-idl.json"));
        int b = json.indexOf('{', json.indexOf("\"byOperator\""));
        Matcher m = Pattern.compile("\"([^\"]+)\"\\s*:\\s*\"([^\"]+)\"")
                .matcher(json.substring(b + 1, json.indexOf('}', b)));
        while (m.find()) byOperator.put(m.group(1), m.group(2));
    }

    @AfterAll
    static void teardown() { if (spark != null) spark.stop(); }

    /** Canonical bare name for a SQL operator, per the catalog's byOperator map. */
    private static String op(String operator) {
        String n = byOperator.get(operator);
        assertNotNull(n, "catalog byOperator has no entry for " + operator);
        return n;
    }

    private Object scalar(String sql) {
        Row r = spark.sql(sql).collectAsList().get(0);
        return r.isNullAt(0) ? null : r.get(0);
    }

    @Test
    void generatedSurface_registers_and_runs() {
        // scalar accessor: 3 instants
        assertEquals(3, ((Number) scalar(
            "SELECT temporal_num_instants('" + TINT_HEX + "')")).intValue());
        // scalar accessor: start value = 1
        assertEquals(1, ((Number) scalar(
            "SELECT tint_start_value('" + TINT_HEX + "')")).intValue());
        // I/O round-trip: tint_out is non-null and renders the int values 1,2,1
        // (timestamps are rendered in UTC, so don't assert on the wall-clock date).
        Object out = scalar("SELECT tint_out('" + TINT_HEX + "')");
        assertNotNull(out);
        String s = out.toString();
        assertTrue(s.startsWith("[1@") && s.contains("2@"),
                   "tint_out should render the values, got: " + s);
    }

    @Test
    void numeric_op_on_generated_surface() {
        // tnumber_integral over the linear-free (step) tint is a finite double
        Object integral = scalar("SELECT tnumber_integral('" + TINT_HEX + "')");
        assertNotNull(integral);
        assertTrue(((Number) integral).doubleValue() >= 0.0);
    }

    // [1.5@2001-01-01, 2.5@2001-01-02] (linear tfloat)
    private static final String TFLOAT_HEX =
        "0121000E0200000003000000000000F83F009C57D3C11C0000000000000000044000FC2EF1D51C0000";
    // [Point(1 1)@2001-01-01, Point(2 2)@2001-01-02] (tgeompoint)
    private static final String TGEOMPOINT_HEX =
        "012E000E02000000030101000000000000000000F03F000000000000F03F009C57D3C11C0000010100000000"
        + "00000000000040000000000000004000FC2EF1D51C0000";

    @Test
    void breadth_across_families_and_marshalling_kinds() {
        // Double marshalling: start value of the tfloat = 1.5
        assertEquals(1.5, ((Number) scalar(
            "SELECT tfloat_start_value('" + TFLOAT_HEX + "')")).doubleValue(), 1e-9);
        // Double op: time-weighted average of a linear [1.5 -> 2.5] segment = 2.0
        assertEquals(2.0, ((Number) scalar(
            "SELECT tnumber_twavg('" + TFLOAT_HEX + "')")).doubleValue(), 1e-9);
        // Boolean marshalling: tfloat ever equals 1.5 (true) but never 5.0 (false).
        // Cast the literal: Spark SQL parses 1.5 as decimal; the UDF takes a double.
        assertEquals(Boolean.TRUE, scalar(
            "SELECT ever_eq_tfloat_float('" + TFLOAT_HEX + "', CAST(1.5 AS DOUBLE))"));
        assertEquals(Boolean.FALSE, scalar(
            "SELECT ever_eq_tfloat_float('" + TFLOAT_HEX + "', CAST(5.0 AS DOUBLE))"));
        // geo temporal: a tgeompoint has 2 timestamps (exercises the geo family + hex)
        assertEquals(2, ((Number) scalar(
            "SELECT temporal_num_timestamps('" + TGEOMPOINT_HEX + "')")).intValue());
    }

    // [Cbuffer(Point(1 1),0.5)@2001-01-01, Cbuffer(Point(2 2),1.5)@2001-01-02]
    private static final String TCBUFFER_HEX =
        "013B000E0200000003000000000000F03F000000000000F03F000000000000E03F009C57D3C11C0000"
        + "00000000000000400000000000000040000000000000F83F00FC2EF1D51C0000";
    // [Npoint(1,0.2)@2001-01-01, Npoint(1,0.8)@2001-01-02]
    private static final String TNPOINT_HEX =
        "0133000E02000000030101000000000000009A9999999999C93F009C57D3C11C0000010100000000000000"
        + "9A9999999999E93F00FC2EF1D51C0000";

    @Test
    void extended_families_cbuffer_and_npoint() {
        // tcbuffer (2 instants) + a cbuffer-family op (radius -> a tfloat, non-null)
        assertEquals(2, ((Number) scalar(
            "SELECT temporal_num_instants('" + TCBUFFER_HEX + "')")).intValue());
        assertNotNull(scalar("SELECT tcbuffer_radius('" + TCBUFFER_HEX + "')"));
        // tnpoint (2 instants) — exercises the network-point family marshalling
        assertEquals(2, ((Number) scalar(
            "SELECT temporal_num_instants('" + TNPOINT_HEX + "')")).intValue());
    }

    @Test
    void json_path_query_surface() {
        // The JSON-path query surface (jsonb_path_exists/match/query_*, jsonbset_path_*,
        // tjsonb_path_*) is reached by the JsonPath marshalling rule (jsonpath_in/out) —
        // the sibling of the Jsonb rule. This proves the generated jsonpath_in arg-parse
        // binds and runs: a path present in the document is found (1), an absent one is
        // not (0). The trailing vars('{}')/silent/tz args are the C-faithful signature.
        assertEquals(1, ((Number) scalar(
            "SELECT jsonb_path_exists('{\"a\": 1}', '$.a', '{}', false, false)")).intValue());
        assertEquals(0, ((Number) scalar(
            "SELECT jsonb_path_exists('{\"a\": 1}', '$.b', '{}', false, false)")).intValue());
    }

    @Test
    void scalar_value_array_accessors() {
        // The value-array accessors (tint_values / tfloat_values / *set_values / ...) return
        // a native scalar array with the element count via an int* out-param, marshalled to
        // a Spark array<int|long|double>. Inspect via Spark SQL size()/array_contains() (no
        // Java-side Seq casting). tint_values of [1@.., 2@.., 1@..] = the distinct set {1, 2}.
        assertEquals(2, ((Number) scalar(
            "SELECT size(tint_values('" + TINT_HEX + "'))")).intValue());
        assertEquals(Boolean.TRUE, scalar(
            "SELECT array_contains(tint_values('" + TINT_HEX + "'), 1)"));
        assertEquals(Boolean.TRUE, scalar(
            "SELECT array_contains(tint_values('" + TINT_HEX + "'), 2)"));
        // double element type: tfloat_values of [1.5@.., 2.5@..] = {1.5, 2.5}
        assertEquals(2, ((Number) scalar(
            "SELECT size(tfloat_values('" + TFLOAT_HEX + "'))")).intValue());
    }

    @Test
    void portable_bare_name_dispatch_surface() {
        // The portable bare-name operator dialect, emitted by the generator's DISPATCH pass
        // — NOT hand-registered. The bare names are read from the catalog's byOperator map
        // (op(...)) rather than hard-coded, so the dialect (e.g. ?=->eEq, <->->tDistance) is
        // the single source: a rename updates this test automatically. One assertion per
        // family proves the superclass entrypoint dispatches the concrete subtype from hex-WKB.
        // topology (&&): two identical tints overlap in time → true
        assertEquals(Boolean.TRUE, scalar(
            "SELECT " + op("&&") + "('" + TINT_HEX + "', '" + TINT_HEX + "')"));
        // same (~=): a value equals itself
        assertEquals(Boolean.TRUE, scalar(
            "SELECT " + op("~=") + "('" + TINT_HEX + "', '" + TINT_HEX + "')"));
        // time position (&<#): a value's period overbefore-overlaps itself → true
        assertEquals(Boolean.TRUE, scalar(
            "SELECT " + op("&<#") + "('" + TINT_HEX + "', '" + TINT_HEX + "')"));
        // temporal comparison (#=): of a value with itself is a temporal bool, non-null
        assertNotNull(scalar("SELECT " + op("#=") + "('" + TINT_HEX + "', '" + TINT_HEX + "')"));
        // ever comparison (?=): same value → true
        assertEquals(Boolean.TRUE, scalar(
            "SELECT " + op("?=") + "('" + TINT_HEX + "', '" + TINT_HEX + "')"));
        // space-X axis classifier (&<): a value is overleft-of itself on its value axis →
        // true (exercises axisBool)
        assertEquals(Boolean.TRUE, scalar(
            "SELECT " + op("&<") + "('" + TINT_HEX + "', '" + TINT_HEX + "')"));
        // distance (<->): lifted distance between two coincident tgeompoints → a temporal
        // (hex-WKB) of all-zero distance, non-null
        assertNotNull(scalar(
            "SELECT " + op("<->") + "('" + TGEOMPOINT_HEX + "', '" + TGEOMPOINT_HEX + "')"));
    }

    @Test
    void sqlfn_canonical_names_with_argkind_dispatch() {
        // The @sqlfn pass emits the canonical MobilityDB SQL names. Several map to
        // multiple C overloads dispatched by arg KIND at runtime — verify the dispatch
        // routes correctly on the tgeompoint trip ([Point(1 1)@.., Point(2 2)@..]).
        // eIntersects(tgeo, tgeo): trip ever-intersects itself -> true. int(1/0/-1) C
        // return is marshalled to Boolean by the <e|a><Verb> predicate convention.
        assertEquals(Boolean.TRUE, scalar(
            "SELECT eIntersects('" + TGEOMPOINT_HEX + "', '" + TGEOMPOINT_HEX + "')"));
        // eDwithin(tgeo, tgeo, dist): 3-arg overload (P,P,Double), within 10 of itself -> true
        assertEquals(Boolean.TRUE, scalar(
            "SELECT eDwithin('" + TGEOMPOINT_HEX + "', '" + TGEOMPOINT_HEX + "', CAST(10.0 AS DOUBLE))"));
        // isHex guard + arg-kind dispatch on a MIXED (tgeo, geo-WKT) call: the WKT literal
        // must NOT crash the hex parser (isHex(false) -> skip the tgeo_tgeo candidate),
        // and the (tgeo, geo) overload then runs and returns a valid boolean (not null,
        // not a segfault). The exact truth value depends on the trip interpolation.
        assertNotNull(scalar(
            "SELECT eIntersects('" + TGEOMPOINT_HEX + "', 'LINESTRING(1.5 0, 1.5 5)')"));
        // nearestApproachDistance(tgeo, tgeo): coincident -> 0.0 (tgeo_tgeo, NOT tgeo_geo)
        assertEquals(0.0, ((Number) scalar(
            "SELECT nearestApproachDistance('" + TGEOMPOINT_HEX + "', '" + TGEOMPOINT_HEX + "')")).doubleValue(), 1e-9);
        // (atTime over a text period is a span text-vs-hex input concern handled in the
        //  bench-rebuild phase; the arg-kind dispatch mechanism is proven above.)
        // atTime time-restrict polymorphism via a runtime-classified String arg: a
        // period "[..]" routes to temporal_at_tstzspan, a bare timestamp to
        // temporal_at_timestamptz (Spark can't overload, so both go through one UDF).
        assertEquals(2, ((Number) scalar("SELECT numInstants(atTime('" + TGEOMPOINT_HEX
            + "', '[2001-01-01, 2001-01-03]'))")).intValue());
        assertEquals(1, ((Number) scalar("SELECT numInstants(atTime('" + TGEOMPOINT_HEX
            + "', '2001-01-01 00:00:00+00'))")).intValue());
        // trajectory: SQL-faithful 1-arg (the C tpoint_trajectory(Temporal, bool) flag is
        // SQL-optional/defaulted, consumed from sqlArity).
        assertNotNull(scalar("SELECT trajectory('" + TGEOMPOINT_HEX + "')"));
        // asHexWKB: SQL-faithful 1-arg (the WKB variant defaulted), round-trips.
        assertEquals(3, ((Number) scalar(
            "SELECT numInstants(temporal_from_hexwkb(asHexWKB('" + TINT_HEX + "')))")).intValue());
        // numInstants: a single-overload @sqlfn accessor under its canonical SQL name
        assertEquals(3, ((Number) scalar(
            "SELECT numInstants('" + TINT_HEX + "')")).intValue());
    }

    @Test
    void as_hexwkb_family_with_swallowed_size_out_param() {
        // temporal_as_hexwkb returns char* with a trailing size_t* size_out out-param
        // that JMEOS swallows, plus an `unsigned char variant` -> ByteType. Generated
        // now that the size_out is dropped and unsigned char maps to byte. Round-trips:
        // parse the hex, re-serialize (variant 4 = canonical hex), re-parse -> 3 instants.
        Object hex = scalar(
            "SELECT temporal_as_hexwkb(tint_in('[1@2001-01-01, 2@2001-01-02, 1@2001-01-03]'), CAST(4 AS BYTE))");
        assertNotNull(hex);
        assertEquals(3, ((Number) scalar(
            "SELECT temporal_num_instants(temporal_from_hexwkb('" + hex + "'))")).intValue());
    }

    @Test
    void parser_round_trip_entirely_on_the_generated_surface() {
        // tint_in is the newly-generated cstring (WKT) parser: a full parse->operate
        // round-trip driven only by generated UDFs, no externally-computed hex.
        String wkt = "[1@2001-01-01, 2@2001-01-02, 1@2001-01-03]";
        assertEquals(3, ((Number) scalar(
            "SELECT temporal_num_instants(tint_in('" + wkt + "'))")).intValue());
        Object out = scalar("SELECT tint_out(tint_in('" + wkt + "'))");
        assertNotNull(out);
        assertTrue(out.toString().contains("1@") && out.toString().contains("2@"),
                   "round-trip should render the values, got: " + out);
    }

    @Test
    void nxn_array_tgeoarr_udfs() {
        // The generated array-in UDFs: each (Temporal**, int) C pair is a Spark array<string>.
        String p = "array('" + TGEOMPOINT_HEX + "')";          // a 1-element trip array
        // minDistance(array,array) -> double: distance of a trip set to itself = 0.
        assertEquals(0.0, ((Number) scalar(
            "SELECT minDistance(" + p + ", " + p + ")")).doubleValue(), 1e-9);
        // eDwithinPairs -> array<struct<i,j>>: the single trip is ever-within 1000 of itself,
        // so exactly one [0,0] pair (indices 0-based).
        assertEquals(1, ((Number) scalar(
            "SELECT size(eDwithinPairs(" + p + ", " + p + ", CAST(1000.0 AS DOUBLE)))")).intValue());
        assertEquals(0, ((Number) scalar(
            "SELECT eDwithinPairs(" + p + ", " + p + ", CAST(1000.0 AS DOUBLE))[0].i")).intValue());
        assertEquals(0, ((Number) scalar(
            "SELECT eDwithinPairs(" + p + ", " + p + ", CAST(1000.0 AS DOUBLE))[0].j")).intValue());
        // tDwithinPairs -> array<struct<i,j,periods>>: same pair, with a non-null period hex-WKB.
        assertEquals(1, ((Number) scalar(
            "SELECT size(tDwithinPairs(" + p + ", " + p + ", CAST(1000.0 AS DOUBLE)))")).intValue());
        assertNotNull(scalar(
            "SELECT tDwithinPairs(" + p + ", " + p + ", CAST(1000.0 AS DOUBLE))[0].periods"));
        // aDisjointPairs: a trip is never always-disjoint from itself -> no pairs.
        assertEquals(0, ((Number) scalar(
            "SELECT size(aDisjointPairs(" + p + ", " + p + "))")).intValue());
        // The bench's q06/q10 consumption shape: LATERAL VIEW explode over the array<struct>.
        // One [0,0] pair explodes to a single row; pr.i / pr.j index back into the 0-based arrays.
        assertEquals(1, ((Number) scalar(
            "SELECT count(*) FROM (SELECT explode(eDwithinPairs(" + p + ", " + p
            + ", CAST(1000.0 AS DOUBLE))) AS pr)")).intValue());
        // A BARE numeric literal (1000.0) is a Spark decimal, not a double — the canonical
        // queries write it bare, so the dist arg must coerce via Number (not a Double cast).
        assertEquals(1, ((Number) scalar(
            "SELECT size(eDwithinPairs(" + p + ", " + p + ", 1000.0))")).intValue());
    }
}
