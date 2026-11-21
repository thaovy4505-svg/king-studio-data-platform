import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# Đọc token từ biến môi trường (đã được nạp từ file .env)
META_BASE = "https://graph.huydata.com/v21.0"
META_TOKEN = os.environ.get("META_ACCESS_TOKEN")

if not META_TOKEN:
    raise ValueError("Chưa cấu hình META_ACCESS_TOKEN trong file .env")

def meta_get(url, params):
    """
    Gửi request tới Meta API. Tự động retry nếu bị rate limit.
    """
    while True:
        try:
            response = requests.get(url, params=params, timeout=60)
            body = response.json()
            
            if "error" not in body:
                return body  # Thành công
            
            error_code = body["error"].get("code")
            if error_code == 17:  # 17 = Rate limit (Gọi quá nhanh)
                print(f"Meta: Bị rate limit. Chờ 60 giây rồi thử lại...")
                time.sleep(60)
                continue
            
            # Nếu là lỗi khác (sai token, sai tham số) thì báo lỗi luôn
            raise Exception(f"Meta báo lỗi: {body['error']}")
            
        except requests.exceptions.RequestException as e:
            print(f"Lỗi kết nối mạng: {e}. Chờ 10 giây thử lại...")
            time.sleep(10)

def lay_action(actions_list, ten_action):
    """
    Tìm trong danh sách actions phần tử có action_type = ten_action.
    Trả về số (float). Không có thì trả về 0.
    """
    if not actions_list:
        return 0.0
    
    for item in actions_list:
        if item.get("action_type") == ten_action:
            return float(item.get("value", 0))
    return 0.0

def get_meta_accounts():
    """Lấy danh sách 10 tài khoản quảng cáo"""
    url = f"{META_BASE}/me/adaccounts"
    params = {
        "access_token": META_TOKEN,
        "fields": "id,account_id,name",
        "limit": 500
    }
    body = meta_get(url, params)
    return body.get("data", [])

def lay_meta(start_date, end_date):
    """
    Lấy toàn bộ dữ liệu Meta từ start_date đến end_date.
    Trả về một pandas DataFrame.
    """
    accounts = get_meta_accounts()
    all_rows = []
    
    # Các cột cần lấy từ Meta
    fields = "account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,actions,action_values"

    for acc in accounts:
        print(f"Đang lấy dữ liệu Meta cho: {acc['name']}...")
        
        url = f"{META_BASE}/{acc['id']}/insights"
        params = {
            "access_token": META_TOKEN,
            "level": "ad",                # Lấy ở tầng Ad (nhỏ nhất)
            "fields": fields,
            "time_range": json.dumps({"since": str(start_date), "until": str(end_date)}),
            "time_increment": 1,          # Tách thành từng ngày
            "breakdowns": "country",      # Tách theo quốc gia
            "limit": 500                  # Lấy 500 dòng mỗi trang
        }

        # Vòng lặp phân trang
        while url:
            body = meta_get(url, params)
            
            for it in body.get("data", []):
                # Bóc tách lượt cài và lượt mua từ list actions
                actions = it.get("actions", [])
                action_values = it.get("action_values", [])
                
                installs = lay_action(actions, "mobile_app_install")
                purchases = lay_action(actions, "app_custom_event.fb_mobile_purchase")
                revenue = lay_action(action_values, "app_custom_event.fb_mobile_purchase")

                row = {
                    "report_date": it.get("date_start"),
                    "platform": "meta",
                    "account_id": str(acc["account_id"]),
                    "account_name": acc["name"],
                    "campaign_id": str(it.get("campaign_id", "")),
                    "campaign_name": it.get("campaign_name", ""),
                    "adset_id": str(it.get("adset_id", "")),
                    "adset_name": it.get("adset_name", ""),
                    "ad_id": str(it.get("ad_id", "")),
                    "ad_name": it.get("ad_name", ""),
                    "country_code": it.get("country", ""),
                    "spend_usd": float(it.get("spend", 0)),
                    "impressions": int(it.get("impressions", 0)),
                    "clicks": int(it.get("clicks", 0)),
                    "installs": int(installs),
                    "purchases": int(purchases),
                    "purchase_value_usd": float(revenue),
                    "extracted_at": datetime.now(timezone.utc)
                }
                all_rows.append(row)
            
            # Kiểm tra xem còn trang tiếp theo không
            paging = body.get("paging", {})
            url = paging.get("next")
            params = None # Nếu có link next, params đã nằm sẵn trong link

    print(f"Đã lấy xong Meta. Tổng cộng {len(all_rows)} dòng.")
    return pd.DataFrame(all_rows)