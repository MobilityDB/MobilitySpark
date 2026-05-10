package org.mobilitydb.spark.temporal;
import org.junit.jupiter.api.*;
import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsMinimalTest {
    private static String TRIP;
    private static String TINT_SEQ;
    private static String TBOOL_SEQ;
    private static String TFLOAT_SEQ;
    private static String TTEXT_SEQ;
    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00, POINT(2 0)@2020-01-02 00:00:00+00]"),
            (byte) 0);
        TINT_SEQ   = temporal_as_hexwkb(tint_in("[1@2020-01-01, 3@2020-01-03]"),                (byte) 0);
        TBOOL_SEQ  = temporal_as_hexwkb(tbool_in("[t@2020-01-01, t@2020-01-02, f@2020-01-03]"), (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(tfloat_in("[1.5@2020-01-01, 3.5@2020-01-02]"),         (byte) 0);
        TTEXT_SEQ  = temporal_as_hexwkb(ttext_in("[AAA@2020-01-01, ZZZ@2020-01-03]"),          (byte) 0);
    }
    @Test @Order(1)
    void temporalSubtype_tsequence_returns_Sequence() throws Exception {
        String r = MoreAccessorUDFs.temporalSubtype.call(TRIP);
        assertNotNull(r);
        assertTrue(r.contains("Sequence"));
    }
    @Test @Order(2)
    void startInstant_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.startInstant.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }
    @Test @Order(3)
    void numTimestamps_returns_positive_int() throws Exception {
        Integer r = MoreAccessorUDFs.numTimestamps.call(TRIP);
        assertNotNull(r);
        assertTrue(r >= 1);
    }
    @Test @Order(4)
    void timestampN_returns_nonnull_timestamp() throws Exception {
        java.sql.Timestamp r = MoreAccessorUDFs.timestampN.call(TRIP, 1);
        assertNotNull(r);
    }
    @Test @Order(5)
    void lowerInc_returns_boolean() throws Exception {
        Boolean r = MoreAccessorUDFs.lowerInc.call(TRIP);
        assertNotNull(r);
    }
    @Test @Order(6)
    void duration_returns_nonnull_string() throws Exception {
        String r = MoreAccessorUDFs.duration.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }
    @Test @Order(7)
    void ttextMinValue_returns_string() throws Exception {
        String r = MoreAccessorUDFs.ttextMinValue.call(TTEXT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }
    @Test @Order(8)
    void startInstant_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.startInstant.call(null));
    }
}
