import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Đường dẫn tới key CÁ NHÂN:
DEST_KEY_PATH = "secrets/gcp-sa-dest.json"
# Project ID cá nhân:
DEST_PROJECT_ID = "project-9d554ae6-759f-4a54-807" 

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