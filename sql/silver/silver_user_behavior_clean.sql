CREATE OR REPLACE TABLE `project-57724b2c-291a-4792-ba5.silver.fact_user_behavior_clean` AS
SELECT 
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    platform,
    screen_name,
    level,
    country,
    purchase_value,
    extracted_at
FROM (
    -- Vẫn dùng ROW_NUMBER để loại bỏ trùng lặp trong lần chạy hiện tại
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY user_pseudo_id, event_name, event_timestamp 
            ORDER BY extracted_at DESC
        ) as row_num
    FROM `project-57724b2c-291a-4792-ba5.bronze.fact_user_behavior_raw`
)
WHERE row_num = 1;