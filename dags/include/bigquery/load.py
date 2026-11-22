
import os
import uuid
from google.cloud import bigquery
from datetime import date, datetime
from google.oauth2 import service_account
import pandas as pd

DEST_KEY_PATH = "secrets/gcp-sa-dest.json"

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
    Ghi dữ liệu Marketing vào Bronze.
    Giữ nguyên cơ chế WRITE_APPEND.
    """

    if df.empty:
        print("Marketing DataFrame rỗng.")
        return

    if table_id is None:
        table_id = f"{DEST_PROJECT_ID}.bronze.fact_marketing_raw"

    client = get_dest_client()

    df = df.copy()

    df["extracted_at"] = pd.to_datetime(
        df["extracted_at"],
        utc=True
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    print(
        f"Đang ghi {len(df)} dòng Marketing lên {table_id}..."
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    print(
        f"Ghi thành công {job.output_rows} dòng Marketing."
    )


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
    Nạp Firebase vào staging table.

    Sau đó tạo lại Bronze bằng CTAS:
    - Giữ nguyên dữ liệu của các ngày khác.
    - Thay thế toàn bộ dữ liệu của ngày đang load.
    - Không sử dụng DELETE hoặc INSERT DML.
    """

    if df.empty:
        print("Firebase DataFrame rỗng. Bỏ qua.")
        return

    target_table = (
        f"{DEST_PROJECT_ID}.bronze.fact_user_behavior_raw"
    )

    df = df.copy()


    df["event_date"] = df["event_date"].apply(
        normalize_event_date
    )

    event_dates = df["event_date"].dropna().unique().tolist()

    if len(event_dates) != 1 or df["event_date"].isna().any():
        raise ValueError(
            "Mỗi lần load phải chứa đúng một event_date. "
            f"Nhận được: {event_dates}"
        )

    event_date = event_dates[0]

    datetime.strptime(event_date, "%Y%m%d")

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

    # Tạo tên staging và backup riêng cho mỗi lần chạy
    suffix = uuid.uuid4().hex

    staging_table = (
        f"{DEST_PROJECT_ID}.bronze._stg_user_behavior_{suffix}"
    )

    backup_table = (
        f"{DEST_PROJECT_ID}.bronze._backup_user_behavior_{suffix}"
    )

    columns = """
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
    """


    try:

        print(
            f"Đang upload {len(df)} dòng vào staging "
            f"cho ngày {event_date}..."
        )

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


        backup_sql = f"""
        CREATE TABLE `{backup_table}` AS

        SELECT
            {columns}

        FROM `{target_table}`

        WHERE event_date IS NULL
           OR event_date != '{event_date}'
        """

        client.query(backup_sql).result()

        print("Đã tạo backup dữ liệu Bronze.")

        replace_sql = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS

        SELECT
            {columns}
        FROM `{backup_table}`

        UNION ALL

        SELECT
            {columns}
        FROM `{staging_table}`
        """

        client.query(replace_sql).result()

        print(
            f"Thay thế thành công dữ liệu ngày {event_date}: "
            f"{len(df)} dòng."
        )

    finally:


        client.delete_table(
            staging_table,
            not_found_ok=True
        )

        client.delete_table(
            backup_table,
            not_found_ok=True
        )

        print("Đã hoàn tất dọn staging và backup.")
