INSERT INTO enriched_events
WITH raw_events_stream AS (
  SELECT
    event_id,
    user_id,
    event_timestamp,
    event_type,
    SIZE(event_properties.tags) as tag_count,
    ARRAY_CONTAINS(event_properties.tags, 'premium') as has_premium_tag,
    EXPLODE(event_properties.tags) as individual_tag,
    event_properties.tags[0] as first_tag,
    event_metadata.device_info.os as device_os,
    event_metadata.device_info.browser as device_browser,
    event_metadata.location.country as user_country,
    event_metadata.location.city as user_city,
    event_properties.custom_fields['campaign_id'] as campaign_id,
    event_properties.custom_fields['source'] as traffic_source,
    MAP_KEYS(event_properties.custom_fields) as custom_field_keys,
    MAP_VALUES(event_properties.custom_fields) as custom_field_values,
    GET_JSON_OBJECT(raw_event_data, '$.user.preferences.language') as user_language,
    GET_JSON_OBJECT(raw_event_data, '$.session.duration') as session_duration,
    CONCAT_WS(',', event_properties.tags) as tags_concatenated,
    ARRAY_JOIN(event_properties.tags, '|') as tags_pipe_separated,
    CASE
      WHEN ARRAY_CONTAINS(event_properties.tags, 'vip')
      THEN event_metadata.location.country
      ELSE 'unknown'
    END as vip_user_country
  FROM raw_events
  WHERE event_timestamp >= $ROWTIME - INTERVAL '1' DAY
    AND event_properties IS NOT NULL
    AND event_metadata IS NOT NULL
)
SELECT
  event_id,
  user_id,
  event_timestamp,
  event_type,
  tag_count,
  has_premium_tag,
  individual_tag,
  first_tag,
  device_os,
  device_browser,
  user_country,
  user_city,
  campaign_id,
  traffic_source,
  custom_field_keys,
  custom_field_values,
  user_language,
  session_duration,
  tags_concatenated,
  tags_pipe_separated,
  vip_user_country,
  COLLECT_LIST(event_type) OVER (
    PARTITION BY user_id
    ORDER BY event_timestamp
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
  ) as recent_event_types,
  COLLECT_SET(event_metadata.location.country) OVER (
    PARTITION BY user_id
    ORDER BY event_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as all_user_countries
FROM raw_events_stream
ORDER BY user_id, event_timestamp