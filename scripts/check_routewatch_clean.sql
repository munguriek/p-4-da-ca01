SELECT
    status,
    count(*) AS raw_messages,
    sum(event_count) AS prefix_events
FROM routewatch_clean.processing
WHERE topic = 'routewatch.ris.raw'
  AND transform_version = 1
GROUP BY status
ORDER BY status;

SELECT count(*) AS clean_prefix_events
FROM routewatch_clean.prefix_events
WHERE topic = 'routewatch.ris.raw'
  AND transform_version = 1;

SELECT
    action_type,
    prefix,
    ip_version,
    origin,
    origin_asn,
    as_path_length,
    prepending_count,
    community_count
FROM routewatch_clean.prefix_events
WHERE topic = 'routewatch.ris.raw'
  AND transform_version = 1
ORDER BY
    kafka_cluster_id,
    kafka_partition,
    kafka_offset,
    action_index
LIMIT 5;