CREATE OR REPLACE TABLE `project-57724b2c-291a-4792-ba5.gold.fact_campaign_daily` AS
WITH marketing_agg AS (
    -- 1. Tổng hợp chi phí Marketing theo ngày, game, nền tảng
    SELECT 
        report_date,
        account_name AS game_name,
        platform,
        SUM(spend_usd) AS total_spend,
        SUM(installs) AS total_installs,
        SUM(purchases) AS total_purchases,
        SUM(purchase_value_usd) AS total_revenue
    FROM `project-57724b2c-291a-4792-ba5.silver.fact_marketing_clean`
    GROUP BY 1, 2, 3
),

behavior_agg AS (
    -- 2. Tổng hợp doanh thu từ Hành vi (chỉ lấy event in_app_purchase)
    SELECT 
        event_date AS report_date,
        platform,
        SUM(purchase_value) AS behavior_revenue,
        COUNT(DISTINCT user_pseudo_id) AS active_users
    FROM `project-57724b2c-291a-4792-ba5.silver.fact_user_behavior_clean`
    WHERE event_name = 'in_app_purchase'
    GROUP BY 1, 2
)

-- 3. JOIN 2 nguồn lại để tính ROAS (Doanh thu / Chi phí)
SELECT 
    m.report_date,
    m.game_name,
    m.platform,
    m.total_spend,
    m.total_installs,
    m.total_purchases,
    m.total_revenue AS ads_revenue,
    b.behavior_revenue,
    b.active_users,
    
    -- Tính CPI và ROAS từ Ads
    SAFE_DIVIDE(m.total_spend, m.total_installs) AS cpi,
    SAFE_DIVIDE(m.total_revenue, m.total_spend) AS roas_ads,
    
    -- So sánh doanh thu 2 nguồn (Ads vs Behavior)
    m.total_revenue - b.behavior_revenue AS revenue_diff

FROM marketing_agg m
LEFT JOIN behavior_agg b 
    ON m.report_date = b.report_date 
    AND m.platform = b.platform
ORDER BY m.report_date DESC, m.total_spend DESC;