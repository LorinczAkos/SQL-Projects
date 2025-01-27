-- More advanced SQL scripts using CTEs and Window functions
--in order to create Samples for several Marketing Key metrics
WITH COMBINED_DATA AS (
SELECT
	FBD.AD_DATE,
	COALESCE(FBD.URL_PARAMETERS, FBD.URL_PARAMETERS)AS URL_PARAMETERS,
	COALESCE(FBD.SPEND,0) AS SPEND,
	COALESCE(FBD.IMPRESSIONS,0) AS IMPRESSIONS,
	COALESCE(FBD.REACH,0) AS REACH,
	COALESCE(FBD.CLICKS,0) AS CLICKS,
	COALESCE(FBD.LEADS,0) AS LEADS,
	COALESCE(FBD.VALUE,0) AS VALUE
FROM
	FACEBOOK_ADS_BASIC_DAILY FBD
LEFT JOIN FACEBOOK_ADSET FB_ADSET ON
	FBD.ADSET_ID = FB_ADSET.ADSET_ID
LEFT JOIN FACEBOOK_CAMPAIGN FBCS ON
	FBD.CAMPAIGN_ID = FBCS.CAMPAIGN_ID
UNION ALL
	SELECT
	AD_DATE,
	GAD.URL_PARAMETERS AS URL_PARAMETERS,
	COALESCE(GAD.SPEND,0) AS SPEND,
	COALESCE(GAD.IMPRESSIONS,0) AS IMPRESSIONS,
	COALESCE(GAD.REACH,0) AS REACH,
	COALESCE(GAD.CLICKS,0) AS CLICKS,
	COALESCE(GAD.LEADS,0) AS LEADS,
	COALESCE(GAD.VALUE,0) AS VALUE
	FROM GOOGLE_ADS_BASIC_DAILY GAD
),
monthly_stats as (
select
	AD_DATE,
	LOWER (NULLIF(SUBSTRING(URL_PARAMETERS FROM 'utm_campaign=([^&]*)'), 'NAN')) AS UTM_CAMPAIGN,
	extract ('month' from ad_date) as ad_month,
	sum(SPEND) as total_spend,
	sum(impressions) as total_impressions,
	sum(clicks) as total_clicks,
	sum(value) as total_value,
	CASE
		WHEN SUM(IMPRESSIONS) > 0 THEN (SUM(CLICKS) * 100.0) / SUM(IMPRESSIONS)
		ELSE 0 
	END AS CTR,
	CASE
		WHEN SUM(CLICKS) > 0 THEN SUM(SPEND) / SUM(CLICKS)
		ELSE 0
	END AS CPC,
	CASE
		WHEN SUM(IMPRESSIONS) > 0 THEN (SUM(SPEND) * 1000.0) / SUM(IMPRESSIONS)
		ELSE 0
	END AS CPM,
	CASE
		WHEN SUM(SPEND) > 0 THEN (SUM(VALUE)::NUMERIC - SUM(SPEND )::NUMERIC)/ SUM(SPEND)::NUMERIC
		ELSE 0
	END AS ROMI
FROM
	COMBINED_DATA
	group by UTM_Campaign,
	ad_date
),
monthly_stats_with_changes as (
select
	UTM_Campaign,
	ad_month,
	sum(total_spend) as total_spend,
	sum(total_impressions) as total_impressions,
	sum(total_clicks) as total_clicks,
	sum(total_value) as total_value,
	CTR,
	CPC,
	CPM,
	ROMI,
	lag(CTR,1) over (partition by UTM_Campaign order by ad_month asc) as prev_CTR,
	lag(CPC,1) over (partition by UTM_Campaign order by ad_month asc) as prev_CPC,
	lag(CPM,1) over (partition by UTM_Campaign order by ad_month asc) as prev_CPM,
	lag(ROMI,1) over (partition by UTM_Campaign order by ad_month asc) as prev_ROMI
from monthly_stats
group by UTM_Campaign,
	ad_month,
	CTR,
	CPC,
	CPM,
	ROMI
order by ad_month asc
)
select UTM_Campaign,
	ad_month,
	sum(total_spend) as total_spend,
	sum(total_impressions) as total_ipressions,
	sum(total_clicks) as total_clicks,
	sum(total_value) total_value,
	sum(ctr) as CTR,
CASE 
    WHEN SUM(prev_CTR)::numeric > 0 THEN (SUM(ctr)::numeric * 100) / SUM(prev_CTR)::numeric
    WHEN SUM(prev_CTR) = 0 AND SUM(CTR) > 0 THEN 100
    ELSE 0
END AS ctr_change,
	sum(cpc) as cpc,
CASE 
    WHEN SUM(prev_cpc)::numeric > 0 THEN (SUM(cpc)::numeric * 100) / SUM(prev_cpc)::numeric
    WHEN SUM(prev_cpc) = 0 AND SUM(cpc) > 0 THEN 100
    ELSE 0
END AS cpc_change,
sum(cpm) as cpm,
CASE 
    WHEN SUM(prev_cpm)::numeric > 0 THEN (SUM(cpm)::numeric * 100) / SUM(prev_cpm)::numeric
    WHEN SUM(prev_cpm) = 0 AND SUM(cpm) > 0 THEN 100
    ELSE 0
END AS cpm_change,
sum(romi) as romi,
CASE 
    WHEN SUM(prev_romi)::numeric > 0 THEN (SUM(romi)::numeric * 100) / SUM(prev_romi)::numeric
    WHEN SUM(prev_romi) = 0 AND SUM(romi) > 0 THEN 100
    ELSE 0
END AS romi_change
from monthly_stats_with_changes
group by 
	UTM_Campaign,
	ad_month
order by 
	UTM_Campaign asc,
	ad_month asc 
