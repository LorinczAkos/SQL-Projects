-- Rate comparison based on different Laning Pages
WITH session_events AS (
  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
    AND event_name IN ('session_start', 'purchase')
),
session_start AS (
  SELECT
    user_pseudo_id,
    session_id,
    MIN(event_timestamp) AS session_start_time,
    REGEXP_EXTRACT(page_location, r'^https?://[^/]+(/[^?#]*)') AS page_path
  FROM
    session_events
  WHERE
    event_name = 'session_start'
  GROUP BY
    user_pseudo_id, session_id, page_path
),
purchases AS (
  SELECT
    user_pseudo_id,
    session_id,
    COUNT(*) purchase_count
  FROM
    session_events
  WHERE
    event_name = 'purchase'
  GROUP BY
    user_pseudo_id, session_id
),
combined_data AS (
  SELECT
    s.page_path,
    s.user_pseudo_id,
    s.session_id,
    COUNT(DISTINCT s.session_id) AS unique_sessions_per_user,
    COALESCE(p.purchase_count, 0) AS purchase_count,
    CASE
      WHEN COALESCE(p.purchase_count, 0) > 0 THEN 1
      ELSE 0
    END AS session_to_purchase_conversion
  FROM
    session_start s
  LEFT JOIN
    purchases p
  ON
    s.user_pseudo_id = p.user_pseudo_id
    AND s.session_id = p.session_id
  GROUP BY
    s.page_path, s.user_pseudo_id, s.session_id, p.purchase_count
)
SELECT
  page_path,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  COUNT(DISTINCT session_id) AS unique_sessions, --We have zero values because there are zero session_id's therefor it returns zero purchase values
  SUM(purchase_count) AS total_purchases,
  CASE
    WHEN COUNT(DISTINCT session_id) > 0 THEN
      SUM(session_to_purchase_conversion) / COUNT(DISTINCT session_id)
    ELSE 0
  END AS session_to_purchase_conversion_rate
FROM
  combined_data
GROUP BY
  page_path
ORDER BY
  unique_sessions asc
LIMIT 100;
