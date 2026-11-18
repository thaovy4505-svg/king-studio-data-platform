import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# Cấu hình TikTok
TIKTOK_BASE = "https://business-api.huydata.com/open_api/v1.3"
TIKTOK_TOKEN = os.environ.get("TIKTOK_ACCESS_TOKEN")

if not TIKTOK_TOKEN:
    raise ValueError("Chưa cấu hình TIKTOK_ACCESS_TOKEN trong file .env")

TIKTOK_HEADERS = {"Access-Token": TIKTOK_TOKEN}

def tiktok_get(path, params=None):
    """
    Gửi request tới TikTok API. 
    TikTok luôn trả về HTTP 200, nên phải check body["code"] == 0.
    """
    while True:
        try:
            # Lưu ý: TIKTOK_BASE + path. Path phải bắt đầu bằng dấu /
            response = requests.get(TIKTOK_BASE + path, headers=TIKTOK_HEADERS, params=params, timeout=60)
            body = response.json()
            
            if body.get("code") == 0:
                return body.get("data", {})
            
            if body.get("code") == 40100:  # 40100 = Rate limit
                print("TikTok: Bị rate limit. Chờ 60 giây rồi thử lại...")
                time.sleep(60)
                continue
                
            raise Exception(f"TikTok báo lỗi code={body.get('code')}: {body.get('message')}")
            
        except requests.exceptions.RequestException as e:
            print(f"Lỗi kết nối mạng: {e}. Chờ 10 giây thử lại...")
            time.sleep(10)

def get_tiktok_advertisers():
    """Lấy danh sách 10 tài khoản (advertiser)"""
    data = tiktok_get("/oauth2/advertiser/get/")
    return data.get("list", [])

def lay_tiktok(start_date, end_date):
    """
    Lấy toàn bộ dữ liệu TikTok từ start_date đến end_date.
    """
    advertisers = get_tiktok_advertisers()
    all_rows = []
    
    # Các tham số cấu hình cho báo cáo TikTok
    dimensions = json.dumps(["ad_id", "stat_time_day", "country_code"])
    metrics = json.dumps([
        "campaign_id", "campaign_name", "adgroup_id", "adgroup_name", "ad_name", 
        "spend", "impressions", "clicks", "conversion", "purchase", "total_purchase_value"
    ])

    for adv in advertisers:
        print(f"Đang lấy dữ liệu TikTok cho: {adv['advertiser_name']}...")
        
        page = 1
        while True:
            params = {
                "advertiser_id": adv["advertiser_id"],
                "report_type": "BASIC",
                "data_level": "AUCTION_AD",
                "dimensions": dimensions,
                "metrics": metrics,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "page": page,
                "page_size": 1000
            }
            
            data = tiktok_get("/report/integrated/get/", params)
            items = data.get("list", [])
            
            for it in items:
                d = it.get("dimensions", {})
                m = it.get("metrics", {})
                
                # TikTok trả ngày dạng "2026-11-13 00:00:00", cắt lấy 10 ký tự đầu
                report_date_str = d.get("stat_time_day", "")[:10]
                
                row = {
                    "report_date": report_date_str,
                    "platform": "tiktok",
                    "account_id": str(adv["advertiser_id"]),
                    "account_name": adv["advertiser_name"],
                    "campaign_id": str(m.get("campaign_id", "")),
                    "campaign_name": m.get("campaign_name", ""),
                    "adset_id": str(m.get("adgroup_id", "")),
                    "adset_name": m.get("adgroup_name", ""),
                    "ad_id": str(d.get("ad_id", "")),
                    "ad_name": m.get("ad_name", ""),
                    "country_code": d.get("country_code", ""),
                    "spend_usd": float(m.get("spend", 0)),
                    "impressions": int(m.get("impressions", 0)),
                    "clicks": int(m.get("clicks", 0)),
                    "installs": int(m.get("conversion", 0)),
                    "purchases": int(m.get("purchase", 0)),
                    "purchase_value_usd": float(m.get("total_purchase_value", 0)),
                    "extracted_at": datetime.now(timezone.utc)
                }
                all_rows.append(row)
            
            # Kiểm tra phân trang
            page_info = data.get("page_info", {})
            total_page = page_info.get("total_page", 1)
            if page >= total_page:
                break
            page += 1

    print(f"Đã lấy xong TikTok. Tổng cộng {len(all_rows)} dòng.")
    return pd.DataFrame(all_rows)