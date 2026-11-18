# test_local.py
import os
from dotenv import load_dotenv
from datetime import date, timedelta

# Nạp file .env
load_dotenv()

# Import hàm từ module
from dags.include.ads_api.meta_ads import lay_meta
from dags.include.ads_api.tiktok import lay_tiktok 
from dags.include.ads_api.google import lay_google 


if __name__ == "__main__":
    print("Bắt đầu test API Meta...")
    
    # Lấy dữ liệu ngày hôm qua
    yesterday = date.today() - timedelta(days=1)
    
    # Gọi hàm lấy dữ liệu
    df = lay_meta(start_date=yesterday, end_date=yesterday)
    
    print("\n KẾT QUẢ META:")
    print(f"Lấy được {len(df)} dòng.")
    if len(df) > 0:
        print(df.head())
        print("Tổng chi phí:", df["spend_usd"].sum())
    # Lọc riêng game Candy Crush Saga (Android) để đối soát
    ten_game = "Candy Crush Saga (Android)"
    df_mot_game = df[df["account_name"] == ten_game]

    print(f"\n ĐỐI SOÁT GAME: {ten_game}")
    print("Tổng Spend:", round(df_mot_game["spend_usd"].sum(), 2))
    print("Tổng Impressions:", df_mot_game["impressions"].sum())
    print("Tổng Clicks:", df_mot_game["clicks"].sum())
    print("Tổng Installs:", df_mot_game["installs"].sum())
    
    print("\nBẮT ĐẦU TEST TIKTOK")
    df_tiktok = lay_tiktok(start_date=yesterday, end_date=yesterday)
    print(f"Lấy được {len(df_tiktok)} dòng từ TikTok.")
    if len(df_tiktok) > 0:
        print(df_tiktok.head())
        print("Tổng chi phí TikTok:", df_tiktok["spend_usd"].sum())
    
    # --- ĐỐI SOÁT TIKTOK ---
    print("\n--- KIỂM TRA ĐỐI SOÁT TIKTOK ---")
    ten_game_tiktok = "Candy Crush Saga (Android)"
    df_tiktok_mot_game = df_tiktok[df_tiktok["account_name"] == ten_game_tiktok]

    print(f"Game: {ten_game_tiktok}")
    print("Tổng Spend:", round(df_tiktok_mot_game["spend_usd"].sum(), 2))
    print("Tổng Impressions:", df_tiktok_mot_game["impressions"].sum())
    print("Tổng Clicks:", df_tiktok_mot_game["clicks"].sum())
    print("Tổng Installs:", df_tiktok_mot_game["installs"].sum())    
    
    print("\n=== BẮT ĐẦU TEST GOOGLE ===")
    df_google = lay_google(start_date=yesterday, end_date=yesterday)
    print(f"Lấy được {len(df_google)} dòng từ Google.")
    if len(df_google) > 0:
        print(df_google.head())
        print("Tổng chi phí Google:", df_google["spend_usd"].sum())

    # --- ĐỐI SOÁT GOOGLE ---
    print("\n=== KIỂM TRA ĐỐI SOÁT GOOGLE ===")
    ten_game_google = "Candy Crush Saga (Android)" # Tên này phải khớp chính xác với tên trong df_google
    df_google_mot_game = df_google[df_google["account_name"] == ten_game_google]

    if len(df_google_mot_game) == 0:
        print(f"Không tìm thấy dữ liệu cho game: {ten_game_google}. Hãy kiểm tra lại tên game trong cột account_name.")
        print("Các tên game hiện có trong df_google:")
        print(df_google["account_name"].unique())
    else:
        print(f"Game: {ten_game_google}")
        print("Tổng Spend:", round(df_google_mot_game["spend_usd"].sum(), 2))
        print("Tổng Impressions:", df_google_mot_game["impressions"].sum())
        print("Tổng Clicks:", df_google_mot_game["clicks"].sum())
        print("Tổng Installs:", df_google_mot_game["installs"].sum())

        # In thử 3 dòng đầu để xem cột country_code trông như thế nào
        print("\n3 dòng đầu tiên của game này:")
        print(df_google_mot_game[["report_date", "campaign_name", "country_code", "spend_usd"]].head(3))