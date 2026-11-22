CREATE OR REPLACE TABLE
`project-57724b2c-291a-4792-ba5.gold.fact_user_revenue_daily`
AS

WITH revenue_events AS (

    SELECT
        SAFE_CAST(event_date AS DATE) AS event_date,
        platform,
        currency,

        COUNTIF(event_name = 'in_app_purchase')
            AS purchase_count,

        COUNT(DISTINCT IF(
            event_name = 'in_app_purchase',
            user_pseudo_id,
            NULL
        )) AS purchasing_users,

        SUM(IF(
            event_name = 'in_app_purchase',
            SAFE_CAST(purchase_value_raw AS NUMERIC),
            NULL
        )) AS iap_revenue_raw,

        COUNTIF(event_name = 'ad_impression')
            AS ad_impression_count,

        COUNT(DISTINCT IF(
            event_name = 'ad_impression',
            user_pseudo_id,
            NULL
        )) AS ad_viewers,

        SUM(IF(
            event_name = 'ad_impression',
            SAFE_CAST(ad_revenue_raw AS NUMERIC),
            NULL
        )) AS ad_revenue_raw

    FROM
        `project-57724b2c-291a-4792-ba5.silver.fact_user_behavior_clean`

    WHERE
        event_name IN ('in_app_purchase', 'ad_impression')
        AND event_date IS NOT NULL
        AND platform IS NOT NULL

    GROUP BY
        1, 2, 3
)

SELECT
    event_date,
    platform,
    currency,
    purchase_count,
    purchasing_users,
    iap_revenue_raw,
    ad_impression_count,
    ad_viewers,
    ad_revenue_raw

FROM revenue_events;