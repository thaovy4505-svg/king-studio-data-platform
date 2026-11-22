
CREATE OR REPLACE TABLE
`project-57724b2c-291a-4792-ba5.gold.fact_level_funnel_daily`
AS

SELECT
    SAFE_CAST(event_date AS DATE) AS event_date,
    platform,
    level,

    COUNTIF(event_name = 'level_start')
        AS level_start_count,

    COUNTIF(event_name = 'level_end')
        AS level_end_count,

    COUNT(DISTINCT IF(
        event_name = 'level_start',
        user_pseudo_id,
        NULL
    )) AS level_start_users,

    COUNT(DISTINCT IF(
        event_name = 'level_end',
        user_pseudo_id,
        NULL
    )) AS level_end_users

FROM
    `project-57724b2c-291a-4792-ba5.silver.fact_user_behavior_clean`

WHERE
    event_name IN ('level_start', 'level_end')
    AND level IS NOT NULL

GROUP BY
    1, 2, 3;
