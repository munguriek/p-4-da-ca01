BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;

WITH event_counts AS (
    SELECT
        kafka_cluster_id, topic, kafka_partition, kafka_offset,
        count(*) AS actual_events
    FROM routewatch_clean.prefix_events
    WHERE topic = 'routewatch.ris.raw'
      AND transform_version = 1
    GROUP BY kafka_cluster_id, topic, kafka_partition, kafka_offset
),
processing_scope AS (
    SELECT
        kafka_cluster_id, topic, kafka_partition, kafka_offset,
        status, event_count
    FROM routewatch_clean.processing
    WHERE topic = 'routewatch.ris.raw'
      AND transform_version = 1
)
SELECT
    count(*) AS raw_messages,
    count(p.status) AS processed_messages,
    count(*) FILTER (WHERE p.status IS NULL) AS pending_messages,
    count(*) FILTER (
        WHERE p.status IS NOT NULL
          AND p.event_count IS DISTINCT FROM coalesce(e.actual_events, 0)
    ) AS receipt_count_mismatches
FROM routewatch_raw.messages AS r
LEFT JOIN processing_scope AS p
    USING (kafka_cluster_id, topic, kafka_partition, kafka_offset)
LEFT JOIN event_counts AS e
    USING (kafka_cluster_id, topic, kafka_partition, kafka_offset)
WHERE r.topic = 'routewatch.ris.raw';

SELECT
    count(*) FILTER (
        WHERE as_path_length IS DISTINCT FROM
            CASE WHEN as_path IS NULL THEN NULL
                 ELSE jsonb_array_length(as_path) END
    ) AS path_length_mismatches,
    count(*) FILTER (
        WHERE community_count IS DISTINCT FROM
            CASE WHEN communities IS NULL THEN NULL
                 ELSE jsonb_array_length(communities) END
    ) AS community_count_mismatches
FROM routewatch_clean.prefix_events
WHERE topic = 'routewatch.ris.raw'
  AND transform_version = 1;

SELECT
    action_type,
    ip_version,
    count(*) AS prefix_events,
    count(*) FILTER (WHERE as_path IS NULL) AS missing_path,
    count(*) FILTER (WHERE origin IS NULL) AS missing_origin,
    count(*) FILTER (WHERE communities IS NULL) AS missing_communities
FROM routewatch_clean.prefix_events
WHERE topic = 'routewatch.ris.raw'
  AND transform_version = 1
GROUP BY action_type, ip_version
ORDER BY action_type, ip_version;

COMMIT;