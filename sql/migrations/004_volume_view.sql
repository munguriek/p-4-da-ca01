-- 004: telemetry volume per topic and UTC hour, for setting the sampling percentage
-- payload_bytes is the uncompressed JSON as received, so disk use is lower
CREATE VIEW control.v_raw_volume_hourly AS
SELECT
    topic,
    date_trunc('hour', kafka_timestamp, 'UTC') AS hour,
    count(*) AS messages,
    sum(payload_bytes) AS payload_bytes
FROM raw.otel
GROUP BY topic, date_trunc('hour', kafka_timestamp, 'UTC');