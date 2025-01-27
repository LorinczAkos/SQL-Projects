-- Introductory project, I was tasked with connect to the DataBase
-- and write a short query to see on which day there was the most amount of clicks

select ad_date,
spend,
clicks,
spend/clicks as Ratio_Ad_Spend
from facebook_ads_basic_daily
where clicks > 0
order by ad_date desc 