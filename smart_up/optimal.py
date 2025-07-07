import requests
import pandas as pd
import json


DATA_URL = "https://smartup.online/b/anor/mxsx/mdeal/return$export"  # Update this with the new URL
JSON_FILE = ("smartup_return_export.json")

cookies = {
    '_lrt': '1750230987989',
    'AMP_8db086350f': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIzNTYyMzA4Mi0zYzRiLTQyZmEtOGNiZS01Mzg1ZTdkYzQxNjIlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzUwMjMwOTg5NjY3JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc1MDIzMDk4OTY3NCUyQyUyMmxhc3RFdmVudElkJTIyJTNBNiUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMSU3DA==',
    'biruni_device_id': 'EB45BB246B00670DFFE13902931B8B7D1E64D2CACC8BAFB46624D080CFED4A8D',
    'cw_conversation': 'eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VfaWQiOiJlNzcyMjc2OS0zYWY0LTQ5NTgtYjJmMy1lNGVjMmM4MzcyZGMiLCJpbmJveF9pZCI6MX0.ojZPTcjIY8SWQAfqPcpE4KLF3hgP8Xgy0ri3HPyZ0xQ',
    'JSESSIONID': 'sx_app2~D1B32A53D6DB1C503EB75CAF7C97CAD9',
}

def explore_json(data, prefix=""):
    """Recursively explore JSON to find lists for DataFrame conversion."""
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                yield from explore_json(value, f"{prefix}{key}.")
    elif isinstance(data, list) and data:
        yield prefix[:-1]  # Remove trailing dot

try:
    print("⬇️ 1. Ma'lumot yuklanmoqda...")
    response = requests.get(DATA_URL, cookies=cookies)
    response.raise_for_status()

    # Check content type
    content_type = response.headers.get('Content-Type', '').lower()
    print(f"📄 Content-Type: {content_type}")

    # Parse JSON response
    print("🔍 JSON pars qilinmoqda...")
    data = response.json()

    # Explore JSON structure to find potential lists
    list_keys = list(explore_json(data))
    print(f"🔎 Topilmagan ro'yxat kalitlari: {list_keys if list_keys else 'Hech qanday royxat topilmadi'}")

    # Try to create DataFrame from the JSON
    df = None
    if list_keys:
        # Try the first list key for normalization
        for key in list_keys:
            try:
                # Extract the nested list
                nested_data = data
                for part in key.split("."):
                    nested_data = nested_data[part]
                # Flatten the JSON with all parent fields
                df = pd.json_normalize(nested_data, errors="ignore")
                print(f"✅ '{key}' kalitidan DataFrame yaratildi")
                break
            except (KeyError, TypeError):
                continue
    else:
        # If no nested lists, try flat JSON
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            raise ValueError("JSON ma'lumotlari ro'yxat yoki lug'at shaklida emas")

    if df is not None and not df.empty:
        # Save DataFrame to JSON
        df.to_json(JSON_FILE, orient="records", indent=4, force_ascii=False)
        print(f"✅ JSON fayl saqlandi: {JSON_FILE}")
        # Print first few rows for verification
        print("📊 DataFrame namunasi (birinchi 2 qator):")
        print(df.head(2).to_string())
    else:
        raise ValueError("DataFrame yaratib bo'lmadi, ma'lumot bo'sh yoki noto'g'ri formatda")

except Exception as e:
    print(f"❌ Xatolik: {e}")
    # Save raw response for debugging
    with open("smartup_export.txt", "wb") as f:
        f.write(response.content)
    print("📜 Response smartup_export.txt ga saqlandi.")
    # Print JSON snippet for debugging
    try:
        print("📜 JSON namunasi (birinchi 500 belgigacha):")
        print(json.dumps(data, indent=2)[:500])
    except:
        print("📜 JSON ni chop etib bo'lmadi")
