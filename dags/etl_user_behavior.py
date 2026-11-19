from airflow.sdk import dag, task
from datetime import datetime, timedelta
from include.firebase.flat_event import extract_firebase
from include.bigquery.load import load_user_behavior_to_bronze
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

# Cấu hình
DEST_KEY_PATH = "/opt/airflow/secrets/gcp-sa-dest.json" 
DEST_PROJECT_ID = "project-57724b2c-291a-4792-ba5"

def get_bq_client():
    """Khởi tạo BigQuery Client bằng key cá nhân"""
    credentials = service_account.Credentials.from_service_account_file(DEST_KEY_PATH)
    return bigquery.Client(credentials=credentials, project=DEST_PROJECT_ID)

@dag(
    dag_id="etl_user_behavior",
    schedule="0 7 * * *", # Chạy 7h sáng hàng ngày
    start_date=datetime(2026, 11, 15),
    catchup=False,
    params={"run_date": ""},
)
def etl_user_behavior():
    
    @task
    def resolve_run_date(params) -> str:
        """Xử lý tham số ngày chạy, mặc định là ngày hôm qua (D-1)"""
        user_date = params["run_date"]
        if user_date == "":
            yesterday = datetime.now() - timedelta(days=1)
            return yesterday.strftime("%Y-%m-%d")
        return user_date

    @task
    def init_bronze_table():
        """Tạo bảng Bronze nếu chưa tồn tại (Đọc từ file SQL)"""
        client = get_bq_client()
        sql_path = "/opt/airflow/sql/bronze/create_bronze_user_behavior.sql"
        with open(sql_path, "r") as f:
            sql = f.read()
        client.query(sql).result()
        print("Đã kiểm tra/tạo bảng Bronze User Behavior.")

    @task
    def extract_and_load(run_date: str):
        """Lấy dữ liệu từ Firebase và ghi vào bảng Bronze"""
        # Gọi hàm extract_firebase (Hàm này phải trả về DataFrame)
        df = extract_firebase(run_date)
        
        if not df.empty:
            # Gọi hàm load_user_behavior_to_bronze (Đã cấu hình trong file load.py)
            load_user_behavior_to_bronze(df)
            print(f"Đã ghi {len(df)} dòng vào Bronze.")
        else:
            print(f"Không có dữ liệu Firebase cho ngày {run_date}. Bỏ qua bước Load.")

    @task
    def transform_silver():
        """Chạy câu lệnh SQL để làm sạch dữ liệu từ Bronze sang Silver"""
        client = get_bq_client()
        sql_path = "/opt/airflow/sql/silver/silver_user_behavior_clean.sql"
        
        with open(sql_path, "r") as f:
            sql = f.read()
            
        # Chạy câu lệnh CREATE OR REPLACE TABLE
        client.query(sql).result()
        print("Đã transform dữ liệu sang bảng Silver thành công.")

    # --- LUỒNG CHẠY CỦA DAG ---
    # Khai báo thứ tự các task
    run_date = resolve_run_date()
    
    # Luồng: Tạo bảng -> Extract & Load -> Transform
    task_init = init_bronze_table()
    task_extract = extract_and_load(run_date)
    task_silver = transform_silver()

    task_init >> task_extract >> task_silver

# Khởi tạo DAG
etl_user_behavior()