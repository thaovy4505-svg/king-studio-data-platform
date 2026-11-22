
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
    credentials = service_account.Credentials.from_service_account_file(
        DEST_KEY_PATH
    )

    return bigquery.Client(
        credentials=credentials,
        project=DEST_PROJECT_ID
    )


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
            return (
                datetime.now() - timedelta(days=1)
            ).strftime("%Y-%m-%d")

        return user_date

    @task
    def init_bronze_table():
        client = get_bq_client()

        sql_path = "/opt/airflow/sql/bronze/create_bronze_marketing.sql"

        with open(sql_path, "r") as f:
            query = f.read()

        client.query(query).result()

        print("Bronze table initialized successfully.")

    @task
    def extract_and_load(run_date: str):
        end_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=2)

        print(
            f"Đang chạy Rolling Window từ {start_date} đến {end_date}"
        )

        # Extract dữ liệu từ 3 nền tảng quảng cáo
        df_meta = lay_meta(start_date, end_date)
        df_tiktok = lay_tiktok(start_date, end_date)
        df_google = lay_google(start_date, end_date)

        # Gộp dữ liệu
        df_all = pd.concat(
            [df_meta, df_tiktok, df_google],
            ignore_index=True
        )

        df_all["extracted_at"] = datetime.now()

        # Load vào Bronze
        load_to_bronze(df_all)

        print(f"Đã load {len(df_all)} dòng vào Bronze.")

    @task
    def transform_silver():
        client = get_bq_client()

        sql_path = "/opt/airflow/sql/silver/silver_marketing_clean.sql"

        with open(sql_path, "r") as f:
            query = f.read()

        client.query(query).result()

        print("Silver transformation completed.")

    @task
    def transform_gold():
        client = get_bq_client()

        gold_sql_files = [
            "gold_fact_campaign_daily.sql",
            "gold_fact_adset_daily.sql",
            "gold_fact_ad_performance.sql"
        ]

        for sql_file in gold_sql_files:

            sql_path = f"/opt/airflow/sql/gold/{sql_file}"

            print(f"Đang chạy Gold transformation: {sql_file}")

            with open(sql_path, "r") as f:
                query = f.read()

            client.query(query).result()

            print(f"Hoàn thành: {sql_file}")

        print("All Gold transformations completed.")

    # DAG execution flow
    run_date = resolve_run_date()

    init_bronze = init_bronze_table()
    extract = extract_and_load(run_date)
    silver = transform_silver()
    gold = transform_gold()

    init_bronze >> extract >> silver >> gold


etl_marketing()
