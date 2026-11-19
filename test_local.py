import os
import pandas as pd
from dotenv import load_dotenv
from datetime import date, timedelta, datetime, timezone

# Nạp biến môi trường từ file .env
load_dotenv()

# Import các hàm
from dags.include.ads_api.meta_ads import lay_meta
from dags.include.ads_api.tiktok import lay_tiktok
from dags.include.ads_api.google import lay_google
from dags.include.bigquery.load import load_to_bronze

if __name__ == "__main__":
    print("=== BẮT ĐẦU TEST DATA PIPELINE ===")
    
    # 1. Xác định ngày cần lấy (ngày hôm qua)
    yesterday = date.today() - timedelta(days=1)
    # Hoặc bạn có thể fix cứng ngày để test: 
    # yesterday = date(2026, 11, 18) 
    
    print(f"Ngày lấy dữ liệu: {yesterday}\n")

    # 2. Lấy dữ liệu từ 3 nền tảng
    print("--- Đang lấy Meta ---")
    df_meta = lay_meta(start_date=yesterday, end_date=yesterday)
    print(f"Lấy được {len(df_meta)} dòng từ Meta.\n")

    print("--- Đang lấy TikTok ---")
    df_tiktok = lay_tiktok(start_date=yesterday, end_date=yesterday)
    print(f"Lấy được {len(df_tiktok)} dòng từ TikTok.\n")

    print("--- Đang lấy Google ---")
    df_google = lay_google(start_date=yesterday, end_date=yesterday)
    print(f"Lấy được {len(df_google)} dòng từ Google.\n")

    # 3. Gộp 3 bảng lại
    print("--- Đang gộp dữ liệu ---")
    df_all = pd.concat([df_meta, df_tiktok, df_google], ignore_index=True)
    df_all["extracted_at"] = datetime.now(timezone.utc)
    print(f"Tổng cộng: {len(df_all)} dòng.\n")

    # 4. Kiểm tra nhanh (Sanity Check) trước khi ghi lên BigQuery
    print("--- KIỂM TRA NHANH ---")
    print("Tổng chi phí 3 nền tảng:", round(df_all["spend_usd"].sum(), 2))
    print("Tổng lượt cài (installs):", df_all["installs"].sum())
    print("\n5 dòng đầu tiên:")
    print(df_all[["report_date", "platform", "account_name", "spend_usd", "installs"]].head())

    # 5. Ghi lên BigQuery Bronze
    print("\n--- Đang ghi lên BigQuery ---")
    load_to_bronze(df_all)
    
    print("\n=== HOÀN TẤT ===")