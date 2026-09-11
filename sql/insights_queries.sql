-- Example questions an Insights Agent or analyst can answer from the warehouse.

-- Spend, CTR, and conversions by account and day
SELECT
    full_date,
    account_name,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions,
    SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr
FROM vw_ad_performance
GROUP BY full_date, account_name
ORDER BY full_date, spend DESC;

-- Campaigns where spend is high but conversion volume is zero
SELECT
    account_name,
    campaign_name,
    SUM(spend) AS spend,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions
FROM vw_ad_performance
GROUP BY account_name, campaign_name
HAVING SUM(spend) > 10 AND SUM(conversions) = 0
ORDER BY spend DESC;

-- Device mix
SELECT
    device_type,
    device_os,
    SUM(impressions) AS impressions,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
FROM vw_ad_performance
GROUP BY device_type, device_os
ORDER BY spend DESC;
