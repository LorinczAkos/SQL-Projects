--Homework 2
select
campaign_id,
ad_date,
SUM (spend) as spend,
SUM (value) as value,
sum (clicks) as clicks,
SUM (impressions) as impressions,
SUM (cast(spend as float)/cast(impressions as float)*1000) as CPM,
SUM (((clicks::float)/(impressions::FLOAT))*100) as CTR,
sum ((spend::FLOAT)/(clicks::FLOAT)*100) as CPC,
sum ((((value::float)/(spend::float))/(spend::float))*100) as ROMI
from facebook_ads_basic_daily fabd 
where clicks > 0 and spend > 0 and impressions > 0
group by ad_date,
campaign_id
order by ad_date asc,
spend asc;

--Homework 2 BONUS
select
campaign_id,
SUM (spend::float) as spend,
SUM ((((value::float)/(spend::float))/(spend::float))*100) as ROMI
from facebook_ads_basic_daily fabd
where spend > 0
group by campaign_id
having sum (spend) > 500000
order by ROMI desc 
limit 1;