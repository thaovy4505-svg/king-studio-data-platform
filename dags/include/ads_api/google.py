import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# Cấu hình Google
GOOGLE_BASE = "https://googleads.huydata.com/v18"
GOOGLE_TOKEN = os.environ.get("GOOGLE_ACCESS_TOKEN")
GOOGLE_DEV = os.environ.get("GOOGLE_DEVELOPER_TOKEN")

if not GOOGLE_TOKEN or not GOOGLE_DEV:
    raise ValueError("Chưa cấu hình GOOGLE_ACCESS_TOKEN hoặc GOOGLE_DEVELOPER_TOKEN trong file .env")

# Header bắt buộc của Google Ads
GOOGLE_HEADERS = {
    "Authorization": f"Bearer {GOOGLE_TOKEN}",
    "developer-token": GOOGLE_DEV,
    "login-customer-id": "9990001111", 
    "Content-Type": "application/json",
}

def google_query(customer_id, gaql_query):
    """
    Hàm gửi câu lệnh GAQL (giống SQL) tới Google Ads API.
    """
    url = f"{GOOGLE_BASE}/customers/{customer_id}/googleAds:searchStream"
    
    while True:
        try:
            response = requests.post(
                url, 
                headers=GOOGLE_HEADERS, 
                json={"query": gaql_query}, 
                timeout=60
            )
            
            if response.status_code == 429: # Rate limit
                print("Google: Bị rate limit. Chờ 60s...")
                time.sleep(60)
                continue
                
            if response.status_code != 200:
                raise Exception(f"Google báo lỗi HTTP {response.status_code}: {response.text[:300]}")
                
            # Google trả về stream, gộp tất cả kết quả lại
            results = []
            for chunk in response.json():
                if "results" in chunk:
                    results.extend(chunk["results"])
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"Lỗi kết nối mạng: {e}. Chờ 10s...")
            time.sleep(10)

def get_google_customers():
    """Lấy danh sách 10 customer_id"""
    url = f"{GOOGLE_BASE}/customers:listAccessibleCustomers"
    response = requests.get(url, headers=GOOGLE_HEADERS, timeout=60)
    
    if response.status_code != 200:
        raise Exception(f"Lỗi lấy danh sách customer: {response.text}")
        
    resource_names = response.json().get("resourceNames", [])
    # Tên trả về dạng "customers/5423891542", ta chỉ lấy số
    return [name.split("/")[1] for name in resource_names]

def get_google_names(customers):
    """Lấy tên game từ customer_id"""
    names = {}
    for cid in customers:
        # Dùng GAQL để lấy tên customer
        query = "SELECT customer.id, customer.descriptive_name FROM customer"
        res = google_query(cid, query)
        if res:
            names[cid] = res[0].get("customer", {}).get("descriptiveName", f"Unknown_{cid}")
    return names

def lay_google(start_date, end_date):
    """
    Lấy toàn bộ dữ liệu Google từ start_date đến end_date.
    """
    customers = get_google_customers()
    print(f"Tìm thấy {len(customers)} tài khoản Google.")
    
    google_names = get_google_names(customers)
    all_rows = []

    for cid in customers:
        ten_game = google_names.get(cid, cid)
        print(f"Đang lấy dữ liệu Google cho: {ten_game}...")
        
        # Viết câu lệnh GAQL để lấy dữ liệu tầng Ad
        gaql = f"""
        SELECT 
            campaign.id, campaign.name, 
            ad_group.id, ad_group.name, 
            ad_group_ad.ad.id, ad_group_ad.ad.name, 
            segments.date, segments.geo_target_country, 
            metrics.cost_micros, metrics.impressions, metrics.clicks, 
            metrics.conversions, metrics.all_conversions, metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        
        results = google_query(cid, gaql)
        
        for it in results:
            m = it.get("metrics", {})
            c = it.get("campaign", {})
            ag = it.get("adGroup", {})
            ad = it.get("adGroupAd", {}).get("ad", {})
            seg = it.get("segments", {})
            
            # Xử lý đơn vị tiền tệ: cost_micros -> USD
            spend_micros = int(m.get("costMicros", 0))
            spend_usd = spend_micros / 1_000_000
            
            # Xử lý mã quốc gia (ví dụ: geoTargetConstants/2704)
            # Trong bài test này, giảng viên đã map sẵn, nên ta tạm lấy phần cuối hoặc để trống nếu không có
            country = seg.get("geoTargetCountry", "")
            # Nếu cần map chi tiết, bạn phải gọi thêm API geo_target_constant, 
            # nhưng trong phạm vi test, cứ lưu nguyên chuỗi hoặc cắt lấy số.
            
            row = {
                "report_date": seg.get("date"),
                "platform": "google",
                "account_id": str(cid),
                "account_name": ten_game,
                "campaign_id": str(c.get("id", "")),
                "campaign_name": c.get("name", ""),
                "adset_id": str(ag.get("id", "")),
                "adset_name": ag.get("name", ""),
                "ad_id": str(ad.get("id", "")),
                "ad_name": ad.get("name", ""),
                "country_code": country,
                "spend_usd": spend_usd,
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "installs": int(m.get("conversions", 0)),
                "purchases": int(m.get("allConversions", 0) - m.get("conversions", 0)),
                "purchase_value_usd": float(m.get("conversionsValue", 0)),
                "extracted_at": datetime.now(timezone.utc)
            }
            all_rows.append(row)

    print(f"Đã lấy xong Google. Tổng cộng {len(all_rows)} dòng.")
    return pd.DataFrame(all_rows)