
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timezone

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
    Đọc dữ liệu Firebase từ bảng events_intraday.
    run_date có định dạng YYYY-MM-DD.
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

        (
            SELECT COALESCE(
                value.double_value,
                CAST(value.int_value AS FLOAT64)
            )
            FROM UNNEST(event_params)
            WHERE key = 'value'
        ) AS purchase_value

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

    print(f"Đang lấy dữ liệu Firebase từ bảng: {table_name}...")

    try:
        df = client.query(query).to_dataframe()

        # Giữ event_date dạng STRING (YYYYMMDD) trong Bronze
        df["event_date"] = df["event_date"].astype(str)

        df["screen_name"] = df["screen_name"].astype("string")
        df["country"] = df["country"].astype("string")

        df["purchase_value"] = pd.to_numeric(
            df["purchase_value"],
            errors="coerce"
        )

        df["extracted_at"] = datetime.now(timezone.utc)

        print(f"Lấy được {len(df)} dòng dữ liệu hành vi.")

        return df

    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu Firebase: {e}")
        return pd.DataFrame()
