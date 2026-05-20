package org.mobilitydb.spark.util;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * Centralised boundary conversions between Spark / Java native timestamp
 * types and MEOS canonical {@code TimestampTz} (microseconds since the
 * PostgreSQL epoch, 2000-01-01 UTC).
 *
 * <p>This is the closed-algebra-boundary principle applied to time:
 * everywhere in MobilitySpark that crosses the JVM ↔ MEOS boundary, use
 * the helpers below.  Inlining the magic constant {@code 946684800}
 * (seconds between Unix epoch and PG epoch) at individual call-sites
 * scatters the conversion logic and risks drift if one site is updated
 * and another is missed.
 *
 * <p>Companion file: {@code MobilityDuck/src/include/time_util.hpp}
 * (same role on the DuckDB side).
 */
public final class TimeUtil {

    private TimeUtil() {}

    /**
     * Seconds between the Unix epoch (1970-01-01 UTC) and the PostgreSQL /
     * MEOS epoch (2000-01-01 UTC).  10957 days × 86400 seconds.
     */
    public static final long PG_UNIX_EPOCH_OFFSET_S = 946684800L;

    /** Milliseconds equivalent of {@link #PG_UNIX_EPOCH_OFFSET_S}. */
    public static final long PG_UNIX_EPOCH_OFFSET_MS = PG_UNIX_EPOCH_OFFSET_S * 1000L;

    /** Microseconds equivalent of {@link #PG_UNIX_EPOCH_OFFSET_S}. */
    public static final long PG_UNIX_EPOCH_OFFSET_US = PG_UNIX_EPOCH_OFFSET_S * 1_000_000L;

    /** Days between the Unix epoch and the PG epoch.  Used for date-only conversions. */
    public static final long PG_UNIX_EPOCH_OFFSET_DAYS = 10957L;

    /**
     * Spark / JDBC {@link Timestamp} (Unix-epoch milliseconds) → MEOS
     * {@code TimestampTz} (PG-epoch microseconds).
     */
    public static long toMeosTimestamp(Timestamp ts) {
        return (ts.getTime() - PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
    }

    /**
     * JMEOS-side {@link OffsetDateTime} (which encodes PG-epoch microseconds
     * in its epoch-seconds slot — a JMEOS internal convention) →
     * MEOS-canonical {@code TimestampTz}.
     */
    public static long toMeosTimestamp(OffsetDateTime odt) {
        return odt.toEpochSecond();
    }

    /**
     * MEOS {@code TimestampTz} (PG-epoch microseconds) → Spark / JDBC
     * {@link Timestamp} (Unix-epoch milliseconds).
     */
    public static Timestamp fromMeosTimestamp(long meosMicros) {
        return new Timestamp(meosMicros / 1000L + PG_UNIX_EPOCH_OFFSET_MS);
    }

    /**
     * MEOS {@code TimestampTz} → {@link Instant} (Unix-epoch).  Preserves
     * microsecond precision via the nanos slot.
     */
    public static Instant fromMeosInstant(long meosMicros) {
        long unixMicros = meosMicros + PG_UNIX_EPOCH_OFFSET_US;
        long unixSeconds = Math.floorDiv(unixMicros, 1_000_000L);
        int  nanos       = (int) Math.floorMod(unixMicros, 1_000_000L) * 1000;
        return Instant.ofEpochSecond(unixSeconds, nanos);
    }

    /**
     * JMEOS {@link OffsetDateTime} (PG-epoch μs in epoch-seconds slot) →
     * Spark / JDBC {@link Timestamp}.
     */
    public static Timestamp jmeosOdtToSparkTimestamp(OffsetDateTime odt) {
        return fromMeosTimestamp(toMeosTimestamp(odt));
    }
}
