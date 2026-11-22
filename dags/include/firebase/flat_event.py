
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

SOURCE_KEY_PATH = "secrets/gcp-sa-source.json"
SOURCE_PROJECT_ID = "sound-machine-513207-u6"


def get_bq_source_client():
    credentials = service_account.Credentials.from_service_account_file(
        SOURCE_KEY_PATH
    )

    return bigquery.Client(
        credentials=credentials,
        project=SOURCE_PROJECT_ID
    )


def extract_firebase(run_date: str) -> pd.DataFrame:
    """
    Extract Firebase events.

    - Purchase: giữ value, price, currency, product_id.
    - Ad impression: giữ value và currency riêng.
    - Không tự ý quy đổi đơn vị tiền tệ.
    """

    date_str = run_date.replace("-", "")

    table_name = (
        f"{SOURCE_PROJECT_ID}.candycrushsaga.events_intraday_{date_str}"
    )

    client = get_bq_source_client()

    query = f"""
    SELECT
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        platform,

        (
            SELECT value.string_value
            FROM UNNEST(event_params)
            WHERE key = 'firebase_screen'
        ) AS screen_name,

        (
            SELECT value.int_value
            FROM UNNEST(event_params)
            WHERE key = 'level'
        ) AS level,

        (
            SELECT value.string_value
            FROM UNNEST(event_params)
            WHERE key = 'country'
        ) AS country,

        CASE
            WHEN event_name = 'in_app_purchase' THEN (
                SELECT COALESCE(
                    value.double_value,
                    CAST(value.int_value AS FLOAT64)
                )
                FROM UNNEST(event_params)
                WHERE key = 'value'
            )
        END AS purchase_value_raw,

        CASE
            WHEN event_name = 'in_app_purchase' THEN (
                SELECT COALESCE(
                    value.double_value,
                    CAST(value.int_value AS FLOAT64)
                )
                FROM UNNEST(event_params)
                WHERE key = 'price'
            )
        END AS purchase_price_raw,

        CASE
            WHEN event_name = 'ad_impression' THEN (
                SELECT COALESCE(
                    value.double_value,
                    CAST(value.int_value AS FLOAT64)
                )
                FROM UNNEST(event_params)
                WHERE key = 'value'
            )
        END AS ad_revenue_raw,

        (
            SELECT value.string_value
            FROM UNNEST(event_params)
            WHERE key = 'currency'
        ) AS currency,

        (
            SELECT value.string_value
            FROM UNNEST(event_params)
            WHERE key = 'product_id'
        ) AS product_id

    FROM `{table_name}`

    WHERE event_name IN (
        'first_open',
        'session_start',
        'level_start',
        'level_end',
        'in_app_purchase',
        'ad_impression'
    )
    """

    print(f"Đang lấy dữ liệu Firebase từ: {table_name}")

    # Không nuốt lỗi: nếu BigQuery thất bại, Airflow phải báo task failed.
    df = client.query(query).to_dataframe()

    if df.empty:
        print(f"Không có dữ liệu Firebase ngày {run_date}.")
        return df

    df["event_date"] = df["event_date"].astype("string")

    string_columns = [
        "screen_name",
        "country",
        "currency",
        "product_id"
    ]

    for column in string_columns:
        df[column] = df[column].astype("string")

    numeric_columns = [
        "event_timestamp",
        "level",
        "purchase_value_raw",
        "purchase_price_raw",
        "ad_revenue_raw"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df["extracted_at"] = datetime.now(timezone.utc)

    print(f"Extract thành công: {len(df)} dòng.")

    return df
