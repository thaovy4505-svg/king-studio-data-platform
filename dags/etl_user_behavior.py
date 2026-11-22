
from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pendulum

from include.firebase.flat_event import extract_firebase
from include.bigquery.load import load_user_behavior_to_bronze

from google.cloud import bigquery
from google.oauth2 import service_account


# Cấu hình
DEST_KEY_PATH = "/opt/airflow/secrets/gcp-sa-dest.json"
DEST_PROJECT_ID = "project-57724b2c-291a-4792-ba5"

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        DEST_KEY_PATH
    )

    return bigquery.Client(
        credentials=credentials,
        project=DEST_PROJECT_ID
    )


@dag(
    dag_id="etl_user_behavior",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026, 11, 15, tz="Asia/Ho_Chi_Minh"
    ),
    catchup=False,
    params={"run_date": ""},
)
def etl_user_behavior():

    @task
    def resolve_run_date(params) -> str:
        """Mặc định lấy ngày hôm qua theo giờ Việt Nam."""
        user_date = params["run_date"]

        if user_date == "":
            yesterday = (
                pendulum.now(LOCAL_TZ) - timedelta(days=1)
            )
            return yesterday.strftime("%Y-%m-%d")

        return user_date

    @task
    def init_bronze_table():
        """Tạo bảng Bronze nếu chưa tồn tại."""
        client = get_bq_client()

        sql_path = "/opt/airflow/sql/bronze/create_bronze_user_behavior.sql"

        with open(sql_path, "r") as f:
            sql = f.read()

        client.query(sql).result()

        print("Đã kiểm tra/tạo bảng Bronze User Behavior.")

    @task
    def extract_and_load(run_date: str):
        """
        Lấy dữ liệu Rolling Window:
        từ D-2 đến D, tổng cộng 3 ngày.
        """
        end_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=2)

        print(
            f"Đang chạy Rolling Window từ {start_date} đến {end_date}"
        )

        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")

            print(f"Đang lấy dữ liệu Firebase cho ngày: {date_str}")

            df = extract_firebase(date_str)

            if not df.empty:
                load_user_behavior_to_bronze(df)

                print(
                    f"Đã ghi {len(df)} dòng vào Bronze "
                    f"cho ngày {date_str}."
                )
            else:
                print(
                    f"Không có dữ liệu Firebase cho ngày {date_str}. "
                    "Bỏ qua."
                )

            current_date += timedelta(days=1)

    @task
    def transform_silver():
        """Transform Bronze sang Silver."""
        client = get_bq_client()

        sql_path = "/opt/airflow/sql/silver/silver_user_behavior_clean.sql"

        with open(sql_path, "r") as f:
            sql = f.read()

        client.query(sql).result()

        print("Đã transform dữ liệu sang bảng Silver thành công.")

    @task
    def transform_gold():
        """Transform Silver sang 3 bảng Gold."""
        client = get_bq_client()

        gold_sql_files = [
            "gold_fact_level_funnel_daily.sql",
            "gold_fact_user_engagement_daily.sql",
            "gold_fact_user_revenue_daily.sql"
        ]

        for sql_file in gold_sql_files:
            sql_path = f"/opt/airflow/sql/gold/{sql_file}"

            print(f"Đang chạy Gold transformation: {sql_file}")

            with open(sql_path, "r") as f:
                sql = f.read()

            client.query(sql).result()

            print(f"Hoàn thành: {sql_file}")

        print("All Firebase Gold transformations completed.")

    # Xác định ngày chạy
    run_date = resolve_run_date()

    # Khởi tạo các task
    task_init = init_bronze_table()
    task_extract = extract_and_load(run_date)
    task_silver = transform_silver()
    task_gold = transform_gold()

    # Thứ tự thực thi
    task_init >> task_extract >> task_silver >> task_gold


etl_user_behavior()
