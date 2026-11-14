# test_local.py
import os
from dotenv import load_dotenv
from datetime import date, timedelta

# Nạp file .env
load_dotenv()

# Import hàm từ module của bạn
from dags.include.ads_api.meta_ads import lay_meta

if __name__ == "__main__":
    print("Bắt đầu test API Meta...")
    
    # Lấy dữ liệu ngày hôm qua
    yesterday = date.today() - timedelta(days=1)
    
    # Gọi hàm lấy dữ liệu
    df = lay_meta(start_date=yesterday, end_date=yesterday)
    
    print("\n--- KẾT QUẢ ---")
    print(f"Lấy được {len(df)} dòng.")
    if len(df) > 0:
        print(df.head())
        print("Tổng chi phí:", df["spend_usd"].sum())
    # Lọc riêng game Candy Crush Saga (Android) để đối soát
    ten_game = "Candy Crush Saga (Android)"
    df_mot_game = df[df["account_name"] == ten_game]

    print(f"\n--- ĐỐI SOÁT GAME: {ten_game} ---")
    print("Tổng Spend:", round(df_mot_game["spend_usd"].sum(), 2))
    print("Tổng Impressions:", df_mot_game["impressions"].sum())
    print("Tổng Clicks:", df_mot_game["clicks"].sum())
    print("Tổng Installs:", df_mot_game["installs"].sum())