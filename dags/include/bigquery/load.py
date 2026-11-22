
import uuid
from datetime import date, datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


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
    Ghi dữ liệu Marketing vào Bronze bằng WRITE_APPEND.
    """

    if df.empty:
        print("Marketing DataFrame rỗng.")
        return

    if table_id is None:
        table_id = (
            f"{DEST_PROJECT_ID}.bronze.fact_marketing_raw"
        )

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


def get_user_behavior_schema():
    """
    Schema chuẩn cho Firebase User Behavior Bronze.
    """

    return [
        bigquery.SchemaField("event_date", "STRING"),
        bigquery.SchemaField("event_timestamp", "INT64"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("user_pseudo_id", "STRING"),
        bigquery.SchemaField("platform", "STRING"),
        bigquery.SchemaField("screen_name", "STRING"),
        bigquery.SchemaField("level", "INT64"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("purchase_value_raw", "FLOAT64"),
        bigquery.SchemaField("purchase_price_raw", "FLOAT64"),
        bigquery.SchemaField("ad_revenue_raw", "FLOAT64"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]



def prepare_user_behavior_dataframe(df: pd.DataFrame):
    """
    Chuẩn hóa và kiểm tra DataFrame trước khi upload.
    """

    df = df.copy()

    required_columns = [
        "event_date",
        "event_timestamp",
        "event_name",
        "user_pseudo_id",
        "platform",
        "screen_name",
        "level",
        "country",
        "purchase_value_raw",
        "purchase_price_raw",
        "ad_revenue_raw",
        "currency",
        "product_id",
        "extracted_at",
    ]

    # Bổ sung cột thiếu để DataFrame khớp schema.
    for column in required_columns:
        if column not in df.columns:
            df[column] = None

    df = df[required_columns]

    # Chuẩn hóa ngày.
    df["event_date"] = df["event_date"].apply(
        normalize_event_date
    )

    event_dates = (
        df["event_date"]
        .dropna()
        .unique()
        .tolist()
    )

    if len(event_dates) != 1 or df["event_date"].isna().any():
        raise ValueError(
            "Mỗi lần load phải chứa đúng một event_date. "
            f"Nhận được: {event_dates}"
        )

    event_date = event_dates[0]

    datetime.strptime(event_date, "%Y%m%d")

    # Kiểm tra trường định danh.
    for column in [
        "event_timestamp",
        "event_name",
        "user_pseudo_id"
    ]:
        if df[column].isna().any():
            raise ValueError(
                f"Phát hiện NULL trong trường bắt buộc: {column}"
            )

    # Timestamp.
    df["event_timestamp"] = pd.to_numeric(
        df["event_timestamp"],
        errors="raise"
    ).astype("int64")

    # Level.
    df["level"] = pd.to_numeric(
        df["level"],
        errors="coerce"
    ).astype("Int64")

    # Các trường số.
    numeric_columns = [
        "purchase_value_raw",
        "purchase_price_raw",
        "ad_revenue_raw"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        ).astype("float64")

    # Các trường chuỗi.
    string_columns = [
        "event_date",
        "event_name",
        "user_pseudo_id",
        "platform",
        "screen_name",
        "country",
        "currency",
        "product_id"
    ]

    for column in string_columns:
        df[column] = df[column].astype("string")

    # Thời gian extract.
    df["extracted_at"] = pd.to_datetime(
        df["extracted_at"],
        utc=True
    )

    if df["extracted_at"].isna().any():
        raise ValueError(
            "Phát hiện NULL trong extracted_at."
        )

    # Kiểm tra giao dịch mua hàng.
    invalid_purchase = (
        (df["event_name"] == "in_app_purchase")
        & df["purchase_value_raw"].isna()
    )

    if invalid_purchase.any():
        raise ValueError(
            f"Có {invalid_purchase.sum()} giao dịch "
            "in_app_purchase thiếu purchase_value_raw."
        )

    # Ad revenue có thể NULL, chỉ cảnh báo.
    invalid_ad_revenue = (
        (df["event_name"] == "ad_impression")
        & df["ad_revenue_raw"].isna()
    )

    if invalid_ad_revenue.any():
        print(
            "Cảnh báo: "
            f"{invalid_ad_revenue.sum()} ad_impression "
            "không có ad_revenue_raw."
        )

    # Purchase không được ghi nhầm sang ad revenue.
    invalid_revenue_mapping = (
        (df["event_name"] == "in_app_purchase")
        & df["ad_revenue_raw"].notna()
    )

    if invalid_revenue_mapping.any():
        raise ValueError(
            "Phát hiện in_app_purchase có ad_revenue_raw."
        )

    # Ad impression không được ghi nhầm sang purchase.
    invalid_purchase_mapping = (
        (df["event_name"] == "ad_impression")
        & df["purchase_value_raw"].notna()
    )

    if invalid_purchase_mapping.any():
        raise ValueError(
            "Phát hiện ad_impression có purchase_value_raw."
        )

    return df, event_date



def build_legacy_projection(existing_fields):
    """
    Chuyển schema Bronze cũ sang schema mới.

    Nếu bảng cũ có purchase_value:
    - in_app_purchase -> purchase_value_raw
    - ad_impression -> ad_revenue_raw

    Các trường mới chưa có dữ liệu sẽ được bổ sung NULL.
    """

    field_names = set(existing_fields)

    types = {
        "event_date": "STRING",
        "event_timestamp": "INT64",
        "event_name": "STRING",
        "user_pseudo_id": "STRING",
        "platform": "STRING",
        "screen_name": "STRING",
        "level": "INT64",
        "country": "STRING",
        "purchase_value_raw": "FLOAT64",
        "purchase_price_raw": "FLOAT64",
        "ad_revenue_raw": "FLOAT64",
        "currency": "STRING",
        "product_id": "STRING",
        "extracted_at": "TIMESTAMP",
    }

    expressions = []

    for column, data_type in types.items():

        if column in field_names:
            expression = column

        elif (
            column == "purchase_value_raw"
            and "purchase_value" in field_names
        ):
            expression = """
                IF(
                    event_name = 'in_app_purchase',
                    CAST(purchase_value AS FLOAT64),
                    NULL
                ) AS purchase_value_raw
            """

        elif (
            column == "ad_revenue_raw"
            and "purchase_value" in field_names
        ):
            expression = """
                IF(
                    event_name = 'ad_impression',
                    CAST(purchase_value AS FLOAT64),
                    NULL
                ) AS ad_revenue_raw
            """

        else:
            expression = (
                f"CAST(NULL AS {data_type}) AS {column}"
            )

        expressions.append(expression)

    return ",\n".join(expressions)



def validate_bronze_after_load(
    client,
    target_table,
    event_date,
    expected_rows
):
    """
    Kiểm tra dữ liệu sau khi thay thế Bronze.

    Điều kiện:
    1. Số dòng của ngày load phải khớp staging.
    2. Schema phải có đủ 14 cột theo chuẩn mới.
    """

    count_sql = f"""
    SELECT COUNT(*) AS total_rows

    FROM `{target_table}`

    WHERE event_date = @event_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "event_date",
                "STRING",
                event_date
            )
        ]
    )

    result = list(
        client.query(
            count_sql,
            job_config=job_config
        ).result()
    )

    actual_rows = result[0]["total_rows"]

    print(
        f"Kiểm tra số dòng ngày {event_date}: "
        f"expected={expected_rows}, actual={actual_rows}"
    )

    if actual_rows != expected_rows:
        raise ValueError(
            "Data Quality failed: số dòng Bronze không khớp staging. "
            f"Expected={expected_rows}, actual={actual_rows}"
        )

    # Kiểm tra schema.
    table = client.get_table(target_table)

    expected_columns = [
        field.name for field in get_user_behavior_schema()
    ]

    actual_columns = [
        field.name for field in table.schema
    ]

    if actual_columns != expected_columns:
        raise ValueError(
            "Data Quality failed: schema Bronze không khớp. "
            f"Expected={expected_columns}, actual={actual_columns}"
        )

    print("Data Quality validation thành công.")

    return actual_rows


def restore_bronze_from_backup(
    client,
    target_table,
    backup_table
):
    """
    Phục hồi Bronze từ snapshot nếu validation hoặc replace thất bại.
    """

    print("Đang phục hồi Bronze từ backup...")

    restore_sql = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS

    SELECT *
    FROM `{backup_table}`
    """

    client.query(restore_sql).result()

    print("Phục hồi Bronze thành công.")



def load_user_behavior_to_bronze(df: pd.DataFrame):
    """
    Firebase -> Staging -> Backup -> Bronze.

    - Chỉ thay thế dữ liệu của ngày đang load.
    - Giữ nguyên dữ liệu các ngày khác.
    - Tương thích schema Bronze cũ.
    - Tạo snapshot trước khi thay thế.
    - Kiểm tra số dòng và schema sau khi thay thế.
    - Tự động phục hồi nếu validation thất bại.
    - Chỉ xóa backup sau khi validation thành công.
    - Không sử dụng DELETE / INSERT DML.
    """

    if df.empty:
        print("Firebase DataFrame rỗng. Bỏ qua.")
        return

    target_table = (
        f"{DEST_PROJECT_ID}.bronze.fact_user_behavior_raw"
    )

    df, event_date = prepare_user_behavior_dataframe(df)

    expected_rows = len(df)

    client = get_dest_client()

    # Kiểm tra bảng Bronze hiện tại.
    target = client.get_table(target_table)

    existing_fields = [
        field.name for field in target.schema
    ]

    # Tạo tên bảng riêng cho mỗi lần chạy.
    suffix = uuid.uuid4().hex

    staging_table = (
        f"{DEST_PROJECT_ID}.bronze."
        f"_stg_user_behavior_{suffix}"
    )

    backup_table = (
        f"{DEST_PROJECT_ID}.bronze."
        f"_backup_user_behavior_{suffix}"
    )

    columns = [
        field.name for field in get_user_behavior_schema()
    ]

    column_sql = ", ".join(columns)

    staging_created = False
    backup_created = False
    replacement_started = False
    validation_success = False

    try:


        print(
            f"Đang upload {expected_rows} dòng vào staging "
            f"cho ngày {event_date}..."
        )

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=get_user_behavior_schema()
        )

        job = client.load_table_from_dataframe(
            df,
            staging_table,
            job_config=job_config
        )

        job.result()

        staging_created = True

        print("Upload staging thành công.")

        # Xác minh staging trước khi thay thế.
        staging = client.get_table(staging_table)

        if staging.num_rows != expected_rows:
            raise ValueError(
                "Staging row count không khớp DataFrame. "
                f"Expected={expected_rows}, "
                f"actual={staging.num_rows}"
            )

        print(
            f"Staging validation OK: {staging.num_rows} dòng."
        )


        backup_sql = f"""
        CREATE TABLE `{backup_table}` AS

        SELECT *
        FROM `{target_table}`
        """

        client.query(backup_sql).result()

        backup_created = True

        backup = client.get_table(backup_table)

        print(
            f"Đã tạo backup: {backup_table} "
            f"({backup.num_rows} dòng)."
        )


        legacy_projection = build_legacy_projection(
            existing_fields
        )


        replace_sql = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS

        SELECT
            {legacy_projection}

        FROM `{target_table}`

        WHERE event_date IS NULL
           OR event_date != '{event_date}'

        UNION ALL

        SELECT
            {column_sql}

        FROM `{staging_table}`
        """

        replacement_started = True

        client.query(replace_sql).result()

        print(
            f"Đã thay thế dữ liệu ngày {event_date}."
        )


        validate_bronze_after_load(
            client=client,
            target_table=target_table,
            event_date=event_date,
            expected_rows=expected_rows
        )

        validation_success = True

        print(
            f"Load và validation thành công ngày {event_date}."
        )

    except Exception as e:

        print(
            f"Lỗi trong quá trình load User Behavior: {e}"
        )

        # Nếu đã có backup, phục hồi khi replace đã bắt đầu.
        if backup_created and replacement_started:
            try:
                restore_bronze_from_backup(
                    client,
                    target_table,
                    backup_table
                )

            except Exception as restore_error:
                print(
                    "NGHIÊM TRỌNG: Phục hồi Bronze thất bại. "
                    f"Backup vẫn được giữ tại {backup_table}. "
                    f"Lỗi phục hồi: {restore_error}"
                )

        raise

    finally:


        if staging_created:
            try:
                client.delete_table(
                    staging_table,
                    not_found_ok=True
                )

                print("Đã xóa staging table.")

            except Exception as cleanup_error:
                print(
                    "Cảnh báo: Không xóa được staging table: "
                    f"{cleanup_error}"
                )

        if backup_created and validation_success:
            try:
                client.delete_table(
                    backup_table,
                    not_found_ok=True
                )

                print(
                    "Đã xác minh dữ liệu thành công và "
                    "xóa backup."
                )

            except Exception as cleanup_error:
                print(
                    "Cảnh báo: Không xóa được backup. "
                    f"Bảng vẫn được giữ tại {backup_table}. "
                    f"Lỗi: {cleanup_error}"
                )

        elif backup_created:
            print(
                "Giữ lại backup để kiểm tra/phục hồi: "
                f"{backup_table}"
            )

        print("Hoàn tất xử lý Firebase Bronze.")
