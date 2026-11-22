
CREATE OR REPLACE TABLE
`project-57724b2c-291a-4792-ba5.gold.fact_campaign_daily`
AS

SELECT
    SAFE_CAST(s.report_date AS DATE) AS report_date,
    s.platform,
    s.account_id,
    s.account_name,
    s.campaign_id,
    s.campaign_name,

    ROUND(SUM(CAST(s.spend_usd AS NUMERIC)), 2)
        AS total_spend_usd,

    SUM(s.impressions) AS total_impressions,
    SUM(s.clicks) AS total_clicks,
    SUM(s.installs) AS total_installs,
    SUM(s.purchases) AS total_purchases,

    ROUND(SUM(CAST(s.purchase_value_usd AS NUMERIC)), 2)
        AS total_purchase_value_usd,

    ROUND(
        SAFE_DIVIDE(SUM(s.clicks), SUM(s.impressions)) * 100,
        2
    ) AS ctr_percent,

    ROUND(
        SAFE_DIVIDE(
            SUM(CAST(s.spend_usd AS NUMERIC)),
            SUM(s.installs)
        ),
        2
    ) AS cpi,

    ROUND(
        SAFE_DIVIDE(
            SUM(CAST(s.purchase_value_usd AS NUMERIC)),
            SUM(CAST(s.spend_usd AS NUMERIC))
        ),
        4
    ) AS roas

FROM
    `project-57724b2c-291a-4792-ba5.silver.fact_marketing_clean` AS s

GROUP BY
    1, 2, 3, 4, 5, 6;
