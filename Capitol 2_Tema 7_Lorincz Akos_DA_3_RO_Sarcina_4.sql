-- Correlation  verification based on user and purchase implication
with user_sessions as (
  select
	user_pseudo_id ||
  	cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)
  	as user_session_id,
	sum(
  	coalesce(
    	(select value.int_value from unnest(event_params) where key = 'engagement_time_msec'), 0))
	as total_engagement_time,
	case
  	when
    	sum(
      	coalesce(
            safe_cast(
              (select value.string_value from unnest(event_params) where key = 'session_engaged') as integer), 0)
    	) > 0
  	then 1
  	else 0
	end as is_session_engaged
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  group by 1
),
 
 
purchases as (
  select
	user_pseudo_id ||
  	cast((select value.int_value from e.event_params where key = 'ga_session_id') as string)
  	as user_session_id
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where
	event_name = 'purchase'
)
 
 
select
  corr(s.total_engagement_time, case when p.user_session_id is not null then 1 else 0 end) as engagement_time_to_purchase_corr,
  corr(s.is_session_engaged, case when p.user_session_id is not null then 1 else 0 end) as engaged_session_to_purchase_corr,
from user_sessions s
left join purchases p using(user_session_id)
