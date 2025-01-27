WITH session_events AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    event_name,
    geo.country,
    device.category,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name AS campaign
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
    AND event_name IN (
      'session_start',
      'view_item',
      'add_to_cart',
      'begin_checkout',
      'add_shipping_info',
      'add_payment_info',
      'purchase'
    )
),
Session_start AS (
  SELECT
    session_id,
    user_pseudo_id,
    MIN(event_timestamp) AS event_date
  FROM session_events
  WHERE event_name = 'session_start'
  GROUP BY
    session_id,
    user_pseudo_id
),
conversion_steps AS (
  SELECT 
    session_id,
    user_pseudo_id,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS visit_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS visit_to_checkout,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS visit_to_purchase
  FROM session_events
  GROUP BY 
    session_id,
    user_pseudo_id
),
combined_data AS (
  SELECT 
    s.source,
    s.medium,
    s.campaign,
    ss.event_date,
    COUNT(DISTINCT s.user_pseudo_id) AS user_sessions_count,
    SUM(c.visit_to_cart) AS visit_to_cart,
    SUM(c.visit_to_checkout) AS visit_to_checkout,
    SUM(c.visit_to_purchase) AS visit_to_purchase
  FROM Session_start ss
  JOIN session_events s ON ss.session_id = s.session_id AND ss.user_pseudo_id = s.user_pseudo_id
  FULL JOIN conversion_steps c ON ss.user_pseudo_id = c.user_pseudo_id AND ss.session_id = c.session_id
  WHERE 
    s.source IS NOT NULL AND
    s.medium IS NOT NULL AND
    s.campaign IS NOT NULL AND
    ss.event_date IS NOT NULL
  GROUP BY 
    s.source,
    s.medium,
    s.campaign,
    ss.event_date
)
SELECT
  event_date,
  source,
  medium,
  campaign,
  user_sessions_count,
  visit_to_cart,
  visit_to_checkout,
  visit_to_purchase
FROM combined_data
ORDER BY
  event_date,
  source,
  medium,
  campaign
LIMIT 100;
