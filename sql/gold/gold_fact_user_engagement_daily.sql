CREATE OR REPLACE TABLE
`project-57724b2c-291a-4792-ba5.gold.fact_user_engagement_daily`
AS

SELECT
    SAFE_CAST(event_date AS DATE) AS event_date,
    platform,

    COUNT(*) AS total_events,

    COUNT(DISTINCT user_pseudo_id) AS dau,

    COUNTIF(event_name = 'first_open')
        AS first_open_count,

    COUNT(DISTINCT IF(
        event_name = 'first_open',
        user_pseudo_id,
        NULL
    )) AS first_open_users,

    COUNTIF(event_name = 'session_start')
        AS session_start_count,

    COUNT(DISTINCT IF(
        event_name = 'session_start',
        user_pseudo_id,
        NULL
    )) AS session_start_users

FROM
    `project-57724b2c-291a-4792-ba5.silver.fact_user_behavior_clean`

WHERE
    event_date IS NOT NULL
    AND user_pseudo_id IS NOT NULL
    AND platform IS NOT NULL

GROUP BY
    1, 2;