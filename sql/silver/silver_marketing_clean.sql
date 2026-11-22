
CREATE OR REPLACE TABLE
`project-57724b2c-291a-4792-ba5.silver.fact_marketing_clean`
AS

SELECT
    report_date,
    platform,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    ad_id,
    ad_name,
    country_code,
    spend_usd,
    impressions,
    clicks,
    installs,
    purchases,
    purchase_value_usd,
    TIMESTAMP(extracted_at) AS extracted_at

FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                report_date,
                platform,
                account_id,
                campaign_id,
                adset_id,
                ad_id,
                country_code
            ORDER BY extracted_at DESC
        ) AS row_num

    FROM
        `project-57724b2c-291a-4792-ba5.bronze.fact_marketing_raw`
)

WHERE row_num = 1;
