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
 AND
  event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',
    'add_payment_info',
    'purchase'
  )
  AND EXISTS (
    SELECT 1
    FROM UNNEST(event_params) AS params
    WHERE params.key = 'ga_session_id'
  )
LIMIT 100;