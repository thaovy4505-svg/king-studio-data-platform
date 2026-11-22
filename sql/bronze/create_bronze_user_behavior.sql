CREATE TABLE IF NOT EXISTS
`project-57724b2c-291a-4792-ba5.bronze.fact_user_behavior_raw` (
    event_date STRING,
    event_timestamp INT64,
    event_name STRING,
    user_pseudo_id STRING,
    platform STRING,
    screen_name STRING,
    level INT64,
    country STRING,
    purchase_value FLOAT64,
    extracted_at TIMESTAMP
);