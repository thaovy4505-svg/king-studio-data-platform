import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Đường dẫn tới key CÁ NHÂN:
DEST_KEY_PATH = "secrets/gcp-sa-dest.json"
# Project ID cá nhân:
DEST_PROJECT_ID = "project-57724b2c-291a-4792-ba5" 

def load_to_bronze(df: pd.DataFrame, table_id: str = None):
    """
    Ghi DataFrame lên BigQuery tầng Bronze.
    Sử dụng WRITE_APPEND để nối thêm dữ liệu mới.
    """
    if table_id is None:
        table_id = f"{DEST_PROJECT_ID}.bronze.fact_marketing_raw"

    # Khởi tạo client
    credentials = service_account.Credentials.from_service_account_file(DEST_KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=DEST_PROJECT_ID)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", 
        autodetect=True,                  
    )
    
    print(f"Đang ghi {len(df)} dòng lên BigQuery ({table_id})...")
    
    # Đảm bảo cột extracted_at là kiểu datetime chuẩn của BigQuery
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  
    
    print(f"Ghi thành công {job.output_rows} dòng vào {table_id}.")
    

def load_user_behavior_to_bronze(df: pd.DataFrame):
    """
    Ghi dữ liệu hành vi người dùng vào Bronze
    với schema được khai báo tường minh.
    """

    table_id = f"{DEST_PROJECT_ID}.bronze.fact_user_behavior_raw"

    credentials = service_account.Credentials.from_service_account_file(
        DEST_KEY_PATH
    )

    client = bigquery.Client(
        credentials=credentials,
        project=DEST_PROJECT_ID
    )

    df = df.copy()

    df["event_date"] = pd.to_datetime(
        df["event_date"]
    ).dt.date

    df["screen_name"] = df["screen_name"].astype("string")
    df["country"] = df["country"].astype("string")
    df["purchase_value"] = pd.to_numeric(
        df["purchase_value"],
        errors="coerce"
    )

    df["extracted_at"] = pd.to_datetime(
        df["extracted_at"], utc=True
    )

    schema = [
        bigquery.SchemaField("event_date", "DATE"),
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

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema
    )

    print(
        f"Đang ghi {len(df)} dòng hành vi lên BigQuery ({table_id})..."
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    print(
        f"Ghi thành công {job.output_rows} dòng vào {table_id}."
    )
