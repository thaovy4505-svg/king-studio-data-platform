
import os
import uuid
from google.cloud import bigquery
from datetime import date, datetime
from google.oauth2 import service_account
import pandas as pd

# Đường dẫn tới key cá nhân
DEST_KEY_PATH = "secrets/gcp-sa-dest.json"

# Project ID cá nhân
DEST_PROJECT_ID = "project-57724b2c-291a-4792-ba5"


def get_dest_client():
    credentials = service_account.Credentials.from_service_account_file(
        DEST_KEY_PATH
    )

    return bigquery.Client(
        credentials=credentials,
        project=DEST_PROJECT_ID
    )


def load_to_bronze(df: pd.DataFrame, table_id: str = None):
    """
    Ghi dữ liệu Marketing vào Bronze bằng WRITE_APPEND.
    Giữ nguyên cơ chế hiện tại.
    """

    if table_id is None:
        table_id = f"{DEST_PROJECT_ID}.bronze.fact_marketing_raw"

    client = get_dest_client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    df = df.copy()
    df["extracted_at"] = pd.to_datetime(
        df["extracted_at"], utc=True
    )

    print(f"Đang ghi {len(df)} dòng lên BigQuery ({table_id})...")

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    print(f"Ghi thành công {job.output_rows} dòng vào {table_id}.")


def normalize_event_date(value):
    """
    Chuẩn hóa event_date thành STRING YYYYMMDD.
    """

    if pd.isna(value):
        return None

    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.strftime("%Y%m%d")

    value = str(value).strip()

    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        value = value.replace("-", "")

    return value


def load_user_behavior_to_bronze(df: pd.DataFrame):
    """
    Nạp Firebase vào staging trước.
    Sau đó thay thế dữ liệu của đúng event_date trong Bronze.

    Chạy lại cùng một ngày không tạo thêm batch trùng.
    """

    if df.empty:
        print("DataFrame rỗng. Không có dữ liệu để nạp.")
        return

    target_table = (
        f"{DEST_PROJECT_ID}.bronze.fact_user_behavior_raw"
    )

    # Chuẩn bị dữ liệu
    df = df.copy()

    df["event_date"] = df["event_date"].apply(
        normalize_event_date
    )

    event_dates = df["event_date"].dropna().unique().tolist()

    if len(event_dates) != 1:
        raise ValueError(
            f"Mỗi lần load chỉ được chứa một event_date. "
            f"Nhận được: {event_dates}"
        )

    if df["event_date"].isna().any():
        raise ValueError("Phát hiện event_date bị NULL.")

    df["screen_name"] = df["screen_name"].astype("string")
    df["country"] = df["country"].astype("string")

    df["purchase_value"] = pd.to_numeric(
        df["purchase_value"],
        errors="coerce"
    )

    df["extracted_at"] = pd.to_datetime(
        df["extracted_at"],
        utc=True
    )

    schema = [
        bigquery.SchemaField("event_date", "STRING"),
        bigquery.SchemaField("event_timestamp", "INT64"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("user_pseudo_id", "STRING"),
        bigquery.SchemaField("platform", "STRING"),
        bigquery.SchemaField("screen_name", "STRING"),
        bigquery.SchemaField("level", "INT64"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("purchase_value", "FLOAT64"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]

    client = get_dest_client()

    # Tạo staging table riêng cho lần chạy này
    staging_table = (
        f"{DEST_PROJECT_ID}.bronze._stg_user_behavior_{uuid.uuid4().hex}"
    )

    print(
        f"Đang upload {len(df)} dòng vào staging "
        f"cho ngày {event_dates[0]}..."
    )

    try:
        # Bước 1: Upload dữ liệu vào staging
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )

        job = client.load_table_from_dataframe(
            df,
            staging_table,
            job_config=job_config
        )

        job.result()

        print("Upload staging thành công.")

        # Bước 2: Thay thế dữ liệu đúng ngày trong Bronze
        sql = f"""
        BEGIN TRANSACTION;

        DELETE FROM `{target_table}`
        WHERE event_date IN (
            SELECT DISTINCT event_date
            FROM `{staging_table}`
        );

        INSERT INTO `{target_table}` (
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
        )
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
        FROM `{staging_table}`;

        COMMIT TRANSACTION;
        """

        client.query(sql).result()

        print(
            f"Thay thế thành công dữ liệu ngày {event_dates[0]} "
            f"với {len(df)} dòng."
        )

    finally:
        # Xóa staging sau khi hoàn tất hoặc khi gặp lỗi
        client.delete_table(
            staging_table,
            not_found_ok=True
        )
