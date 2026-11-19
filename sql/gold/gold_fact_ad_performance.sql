CREATE OR REPLACE TABLE `project-57724b2c-291a-4792-ba5.gold.fact_ad_performance` AS
SELECT 
    report_date,
    platform,
    account_name AS game_name,
    campaign_id,
    campaign_name,
    ad_id,
    ad_name,
    country_code,
    
    -- Các chỉ số cơ bản
    SUM(spend_usd) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(installs) AS total_installs,
    SUM(purchases) AS total_purchases,
    SUM(purchase_value_usd) AS total_revenue,
    
    -- Các chỉ số tính toán (Metrics)
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr_percent,
    SAFE_DIVIDE(SUM(spend_usd), SUM(installs)) AS cpi,
    SAFE_DIVIDE(SUM(purchase_value_usd), SUM(spend_usd)) AS roas

FROM `project-57724b2c-291a-4792-ba5.silver.fact_marketing_clean`
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;