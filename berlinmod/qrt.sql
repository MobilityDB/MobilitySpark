-- BerlinMOD QRT: Binary roundtrip verification.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Protocol: text in, binary out, byte-equal on reception.
--   Each trip was loaded from WKT text (CSV input).
--   asHexWKB() serializes it to the canonical MEOS hex-WKB (variant 0,
--   little-endian NDR) — the same C function on all three platforms.
--   The hex-WKB strings must be byte-for-byte identical across platforms.

SELECT tripId AS tripid,
       asHexEWKB(trip) AS trip_hexwkb
FROM   Trips
ORDER  BY tripId;
