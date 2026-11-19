from airflow.sdk import dag, task
from datetime import datetime, timedelta
from include.ads_api.meta_ads import lay_meta
from include.ads_api.tiktok import lay_tiktok
from include.ads_api.google import lay_google
from include.bigquery.load import load_to_bronze
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

DEST_KEY_PATH = "/opt/airflow/secrets/gcp-sa-dest.json" 
DEST_PROJECT_ID = "project-57724b2c-291a-4792-ba5"

def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(DEST_KEY_PATH)
    return bigquery.Client(credentials=credentials, project=DEST_PROJECT_ID)

@dag(
    dag_id="etl_marketing", 
    schedule="0 7 * * *", 
    start_date=datetime(2026, 11, 15), 
    catchup=False, 
    params={"run_date": ""}
)
def etl_marketing():
    @task
    def resolve_run_date(params) -> str:
        user_date = params["run_date"]
        if user_date == "": 
            return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return user_date

    @task
    def init_bronze_table():
        client = get_bq_client()
        with open("/opt/airflow/sql/bronze/create_bronze_marketing.sql", "r") as f:
            client.query(f.read()).result()

    @task
    def extract_and_load(run_date: str):
        end_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=2)
        
        print(f"Đang chạy Rolling Window từ {start_date} đến {end_date}")
        
        # Gọi API lấy dữ liệu 3 ngày
        df_meta = lay_meta(start_date, end_date)
        df_tiktok = lay_tiktok(start_date, end_date)
        df_google = lay_google(start_date, end_date)
        
        # Gộp lại
        df_all = pd.concat([df_meta, df_tiktok, df_google], ignore_index=True)
        df_all["extracted_at"] = datetime.now()
        
        # Ghi vào Bronze (WRITE_APPEND sẽ tự động nối thêm, tầng Silver sẽ lọc trùng sau)
        load_to_bronze(df_all)

    @task
    def transform_silver():
        client = get_bq_client()
        with open("/opt/airflow/sql/silver/silver_marketing_clean.sql", "r") as f:
            client.query(f.read()).result()

    @task
    def transform_gold():
        client = get_bq_client()
        with open("/opt/airflow/sql/gold/gold_fact_campaign_daily.sql", "r") as f: 
            client.query(f.read()).result()
        with open("/opt/airflow/sql/gold/gold_fact_ad_performance.sql", "r") as f: 
            client.query(f.read()).result()

    run_date = resolve_run_date()
    init_bronze_table() >> extract_and_load(run_date) >> transform_silver() >> transform_gold()

etl_marketing()