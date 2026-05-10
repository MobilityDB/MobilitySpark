/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark.temporal;

import functions.functions;
import jnr.ffi.Pointer;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for restricting temporal values by value or timestamp set.
 *
 * All temporal values are encoded as hex-WKB Strings. Geometry inputs are
 * WKT Strings. Timestamp-set/span/spanset inputs are hex-WKB Strings.
 *
 * Memory management: every native Pointer allocated by MEOS is freed via
 * MeosMemory.free() in a finally block to prevent native heap leakage.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
public final class RestrictionUDFs {

    private RestrictionUDFs() {}

    // ------------------------------------------------------------------
    // Timestamp-set restriction
    // ------------------------------------------------------------------

    // temporalAtTstzspan(s STRING, spanHex STRING) → STRING
    // MEOS: temporal_at_tstzspan(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> temporalAtTstzspan =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanPtr = functions.span_from_hexwkb(spanHex);
                if (spanPtr == null) return null;
                try {
                    Pointer result = functions.temporal_at_tstzspan(tptr, spanPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalAtTstzspanset(s STRING, spansetHex STRING) → STRING
    // MEOS: temporal_at_tstzspanset(const Temporal *, const SpanSet *) → Temporal *
    public static final UDF2<String, String, String> temporalAtTstzspanset =
        (s, spansetHex) -> {
            if (s == null || spansetHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer ssPtr = functions.spanset_from_hexwkb(spansetHex);
                if (ssPtr == null) return null;
                try {
                    Pointer result = functions.temporal_at_tstzspanset(tptr, ssPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(ssPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalAtTstzset(s STRING, tstzsetHex STRING) → STRING
    // MEOS: temporal_at_tstzset(const Temporal *, const Set *) → Temporal *
    public static final UDF2<String, String, String> temporalAtTstzset =
        (s, tstzsetHex) -> {
            if (s == null || tstzsetHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer setptr = functions.set_from_hexwkb(tstzsetHex);
                if (setptr == null) return null;
                try {
                    Pointer result = functions.temporal_at_tstzset(tptr, setptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(setptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalMinusTstzset(s STRING, tstzsetHex STRING) → STRING
    // MEOS: temporal_minus_tstzset(const Temporal *, const Set *) → Temporal *
    public static final UDF2<String, String, String> temporalMinusTstzset =
        (s, tstzsetHex) -> {
            if (s == null || tstzsetHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer setptr = functions.set_from_hexwkb(tstzsetHex);
                if (setptr == null) return null;
                try {
                    Pointer result = functions.temporal_minus_tstzset(tptr, setptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(setptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalMinusTstzspan(s STRING, spanHex STRING) → STRING
    // MEOS: temporal_minus_tstzspan(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> temporalMinusTstzspan =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanptr = functions.span_from_hexwkb(spanHex);
                if (spanptr == null) return null;
                try {
                    Pointer result = functions.temporal_minus_tstzspan(tptr, spanptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalMinusTstzspanset(s STRING, ssHex STRING) → STRING
    // MEOS: temporal_minus_tstzspanset(const Temporal *, const SpanSet *) → Temporal *
    public static final UDF2<String, String, String> temporalMinusTstzspanset =
        (s, ssHex) -> {
            if (s == null || ssHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer ssptr = functions.spanset_from_hexwkb(ssHex);
                if (ssptr == null) return null;
                try {
                    Pointer result = functions.temporal_minus_tstzspanset(tptr, ssptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(ssptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Delete operations (gap-free removal)
    // ------------------------------------------------------------------

    // temporalDeleteTstzspan(s STRING, spanHex STRING) → STRING
    // MEOS: temporal_delete_tstzspan(const Temporal *, const Span *, bool connect) → Temporal *
    public static final UDF2<String, String, String> temporalDeleteTstzspan =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanptr = functions.span_from_hexwkb(spanHex);
                if (spanptr == null) return null;
                try {
                    Pointer result = functions.temporal_delete_tstzspan(tptr, spanptr, false);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalDeleteTstzspanset(s STRING, ssHex STRING) → STRING
    // MEOS: temporal_delete_tstzspanset(const Temporal *, const SpanSet *, bool connect) → Temporal *
    public static final UDF2<String, String, String> temporalDeleteTstzspanset =
        (s, ssHex) -> {
            if (s == null || ssHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer ssptr = functions.spanset_from_hexwkb(ssHex);
                if (ssptr == null) return null;
                try {
                    Pointer result = functions.temporal_delete_tstzspanset(tptr, ssptr, false);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(ssptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalDeleteTstzset(s STRING, setHex STRING) → STRING
    // MEOS: temporal_delete_tstzset(const Temporal *, const Set *, bool connect) → Temporal *
    public static final UDF2<String, String, String> temporalDeleteTstzset =
        (s, setHex) -> {
            if (s == null || setHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer sptr = functions.set_from_hexwkb(setHex);
                if (sptr == null) return null;
                try {
                    Pointer result = functions.temporal_delete_tstzset(tptr, sptr, false);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(sptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Value restriction: tfloat
    // ------------------------------------------------------------------

    // tfloatAtValue(s STRING, value DOUBLE) → STRING
    // MEOS: tfloat_at_value(const Temporal *, double) → Temporal *
    public static final UDF2<String, Double, String> tfloatAtValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer result = functions.tfloat_at_value(tptr, value);
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tfloatMinusValue(s STRING, value DOUBLE) → STRING
    // MEOS: tfloat_minus_value(const Temporal *, double) → Temporal *
    public static final UDF2<String, Double, String> tfloatMinusValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer result = functions.tfloat_minus_value(tptr, value);
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tintAtValue(s STRING, value INT) → STRING
    // MEOS: tint_at_value(const Temporal *, int) → Temporal *
    public static final UDF2<String, Integer, String> tintAtValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer result = functions.tint_at_value(tptr, value);
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Value-range restriction: tnumber (floatspan / intspan)
    // ------------------------------------------------------------------

    // tnumberAtSpan(s STRING, spanHex STRING) → STRING
    // MEOS: tnumber_at_span(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> tnumberAtSpan =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanPtr = functions.span_from_hexwkb(spanHex);
                if (spanPtr == null) return null;
                try {
                    Pointer result = functions.tnumber_at_span(tptr, spanPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tnumberMinusSpan(s STRING, spanHex STRING) → STRING
    // MEOS: tnumber_minus_span(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> tnumberMinusSpan =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanPtr = functions.span_from_hexwkb(spanHex);
                if (spanPtr == null) return null;
                try {
                    Pointer result = functions.tnumber_minus_span(tptr, spanPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tnumberAtSpanset(s STRING, spansetHex STRING) → STRING
    // MEOS: tnumber_at_spanset(const Temporal *, const SpanSet *) → Temporal *
    public static final UDF2<String, String, String> tnumberAtSpanset =
        (s, spansetHex) -> {
            if (s == null || spansetHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer ssPtr = functions.spanset_from_hexwkb(spansetHex);
                if (ssPtr == null) return null;
                try {
                    Pointer result = functions.tnumber_at_spanset(tptr, ssPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(ssPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tnumberMinusSpanset(s STRING, spansetHex STRING) → STRING
    // MEOS: tnumber_minus_spanset(const Temporal *, const SpanSet *) → Temporal *
    public static final UDF2<String, String, String> tnumberMinusSpanset =
        (s, spansetHex) -> {
            if (s == null || spansetHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer ssPtr = functions.spanset_from_hexwkb(spansetHex);
                if (ssPtr == null) return null;
                try {
                    Pointer result = functions.tnumber_minus_spanset(tptr, ssPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(ssPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Value restriction: tbool
    // ------------------------------------------------------------------

    // tboolAtValue(s STRING, value BOOLEAN) → STRING
    // MEOS: tbool_at_value(const Temporal *, bool) → Temporal *
    public static final UDF2<String, Boolean, String> tboolAtValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer result = functions.tbool_at_value(tptr, value);
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tboolMinusValue(s STRING, value BOOLEAN) → STRING
    // MEOS: tbool_minus_value(const Temporal *, bool) → Temporal *
    public static final UDF2<String, Boolean, String> tboolMinusValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer result = functions.tbool_minus_value(tptr, value);
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Value restriction: ttext
    // ------------------------------------------------------------------

    // ttextAtValue(s STRING, value STRING) → STRING
    // MEOS: ttext_at_value(const Temporal *, text *) → Temporal *
    // The value String is converted to a MEOS text* via cstring2text.
    public static final UDF2<String, String, String> ttextAtValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer txtptr = functions.cstring2text(value);
                if (txtptr == null) return null;
                try {
                    Pointer result = functions.ttext_at_value(tptr, txtptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(txtptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ttextMinusValue(s STRING, value STRING) → STRING
    // MEOS: ttext_minus_value(const Temporal *, text *) → Temporal *
    public static final UDF2<String, String, String> ttextMinusValue =
        (s, value) -> {
            if (s == null || value == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer txtptr = functions.cstring2text(value);
                if (txtptr == null) return null;
                try {
                    Pointer result = functions.ttext_minus_value(tptr, txtptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(txtptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Value restriction: tpoint
    // ------------------------------------------------------------------

    // tpointAtValue(s STRING, geomWkt STRING) → STRING
    // MEOS: tpoint_at_value(const Temporal *, GSERIALIZED *) → Temporal *
    // The geometry WKT is parsed via geo_from_text with SRID 0.
    public static final UDF2<String, String, String> tpointAtValue =
        (s, geomWkt) -> {
            if (s == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer gsptr = functions.geo_from_text(geomWkt, 0);
                if (gsptr == null) return null;
                try {
                    Pointer result = functions.tpoint_at_value(tptr, gsptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(gsptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tpointMinusValue(s STRING, geomWkt STRING) → STRING
    // MEOS: tpoint_minus_value(const Temporal *, GSERIALIZED *) → Temporal *
    public static final UDF2<String, String, String> tpointMinusValue =
        (s, geomWkt) -> {
            if (s == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer gsptr = functions.geo_from_text(geomWkt, 0);
                if (gsptr == null) return null;
                try {
                    Pointer result = functions.tpoint_minus_value(tptr, gsptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(gsptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // STBox and elevation restriction (tpoint)
    // ------------------------------------------------------------------

    // tgeoAtStbox(s STRING, stboxHex STRING) → STRING
    // MEOS: tgeo_at_stbox(const Temporal *, const STBox *, bool border_inc) → Temporal *
    public static final UDF2<String, String, String> tgeoAtStbox =
        (s, stboxHex) -> {
            if (s == null || stboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer boxPtr = functions.stbox_from_hexwkb(stboxHex);
                if (boxPtr == null) return null;
                try {
                    Pointer result = functions.tgeo_at_stbox(tptr, boxPtr, true);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(boxPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tgeoMinusStbox(s STRING, stboxHex STRING) → STRING
    // MEOS: tgeo_minus_stbox(const Temporal *, const STBox *, bool border_inc) → Temporal *
    public static final UDF2<String, String, String> tgeoMinusStbox =
        (s, stboxHex) -> {
            if (s == null || stboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer boxPtr = functions.stbox_from_hexwkb(stboxHex);
                if (boxPtr == null) return null;
                try {
                    Pointer result = functions.tgeo_minus_stbox(tptr, boxPtr, true);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(boxPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tpointAtElevation(s STRING, floatspanHex STRING) → STRING
    // MEOS: tpoint_at_elevation(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> tpointAtElevation =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanPtr = functions.span_from_hexwkb(spanHex);
                if (spanPtr == null) return null;
                try {
                    Pointer result = functions.tpoint_at_elevation(tptr, spanPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tpointMinusElevation(s STRING, floatspanHex STRING) → STRING
    // MEOS: tpoint_minus_elevation(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> tpointMinusElevation =
        (s, spanHex) -> {
            if (s == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer spanPtr = functions.span_from_hexwkb(spanHex);
                if (spanPtr == null) return null;
                try {
                    Pointer result = functions.tpoint_minus_elevation(tptr, spanPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(spanPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Extrema restriction — at maximum / minimum value
    // ------------------------------------------------------------------

    // temporalAtMax(s STRING) → STRING  (restricts to instants at the maximum value)
    // MEOS: temporal_at_max(const Temporal *) → Temporal *
    public static final UDF1<String, String> temporalAtMax =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_at_max(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // temporalAtMin(s STRING) → STRING  (restricts to instants at the minimum value)
    // MEOS: temporal_at_min(const Temporal *) → Temporal *
    public static final UDF1<String, String> temporalAtMin =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_at_min(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Spatial restriction — at/minus geometry
    // ------------------------------------------------------------------

    // tgeoAtGeom(s STRING, geomWkt STRING) → STRING
    // MEOS: tgeo_at_geom(const Temporal *, const GSERIALIZED *) → Temporal *
    public static final UDF2<String, String, String> tgeoAtGeom =
        (s, geomWkt) -> {
            if (s == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer gsptr = functions.geo_from_text(geomWkt, 0);
                if (gsptr == null) return null;
                try {
                    Pointer r = functions.tgeo_at_geom(tptr, gsptr);
                    if (r == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(gsptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tgeoMinusGeom(s STRING, geomWkt STRING) → STRING
    // MEOS: tgeo_minus_geom(const Temporal *, const GSERIALIZED *) → Temporal *
    public static final UDF2<String, String, String> tgeoMinusGeom =
        (s, geomWkt) -> {
            if (s == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer gsptr = functions.geo_from_text(geomWkt, 0);
                if (gsptr == null) return null;
                try {
                    Pointer r = functions.tgeo_minus_geom(tptr, gsptr);
                    if (r == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(gsptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // Timestamp-span/spanset restriction
        spark.udf().register("temporalAtTstzspan",       temporalAtTstzspan,       DataTypes.StringType);
        spark.udf().register("temporalAtTstzspanset",    temporalAtTstzspanset,    DataTypes.StringType);
        // Timestamp-set restriction
        spark.udf().register("temporalAtTstzset",        temporalAtTstzset,        DataTypes.StringType);
        spark.udf().register("temporalMinusTstzset",     temporalMinusTstzset,     DataTypes.StringType);
        spark.udf().register("temporalMinusTstzspan",    temporalMinusTstzspan,    DataTypes.StringType);
        spark.udf().register("temporalMinusTstzspanset", temporalMinusTstzspanset, DataTypes.StringType);
        // Delete operations
        spark.udf().register("temporalDeleteTstzspan",    temporalDeleteTstzspan,    DataTypes.StringType);
        spark.udf().register("temporalDeleteTstzspanset", temporalDeleteTstzspanset, DataTypes.StringType);
        spark.udf().register("temporalDeleteTstzset",     temporalDeleteTstzset,     DataTypes.StringType);
        // Value restriction: tfloat / tint
        spark.udf().register("tfloatAtValue",   tfloatAtValue,   DataTypes.StringType);
        spark.udf().register("tfloatMinusValue", tfloatMinusValue, DataTypes.StringType);
        spark.udf().register("tintAtValue",     tintAtValue,     DataTypes.StringType);
        // Value-range restriction: tnumber
        spark.udf().register("tnumberAtSpan",      tnumberAtSpan,      DataTypes.StringType);
        spark.udf().register("tnumberMinusSpan",   tnumberMinusSpan,   DataTypes.StringType);
        spark.udf().register("tnumberAtSpanset",   tnumberAtSpanset,   DataTypes.StringType);
        spark.udf().register("tnumberMinusSpanset", tnumberMinusSpanset, DataTypes.StringType);
        // Value restriction: tbool
        spark.udf().register("tboolAtValue",    tboolAtValue,    DataTypes.StringType);
        spark.udf().register("tboolMinusValue", tboolMinusValue, DataTypes.StringType);
        // Value restriction: ttext
        spark.udf().register("ttextAtValue",    ttextAtValue,    DataTypes.StringType);
        spark.udf().register("ttextMinusValue", ttextMinusValue, DataTypes.StringType);
        // Value restriction: tpoint
        spark.udf().register("tpointAtValue",   tpointAtValue,   DataTypes.StringType);
        spark.udf().register("tpointMinusValue", tpointMinusValue, DataTypes.StringType);
        // STBox and elevation restriction
        spark.udf().register("tgeoAtStbox",           tgeoAtStbox,           DataTypes.StringType);
        spark.udf().register("tgeoMinusStbox",        tgeoMinusStbox,        DataTypes.StringType);
        spark.udf().register("tpointAtElevation",     tpointAtElevation,     DataTypes.StringType);
        spark.udf().register("tpointMinusElevation",  tpointMinusElevation,  DataTypes.StringType);
        // Extrema restriction
        spark.udf().register("temporalAtMax",   temporalAtMax,   DataTypes.StringType);
        spark.udf().register("temporalAtMin",   temporalAtMin,   DataTypes.StringType);
        // Spatial restriction
        spark.udf().register("tgeoAtGeom",      tgeoAtGeom,      DataTypes.StringType);
        spark.udf().register("tgeoMinusGeom",   tgeoMinusGeom,   DataTypes.StringType);
    }
}
