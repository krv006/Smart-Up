import requests
import json

url = "https://smartup.online/b/anor/mxsx/mkw/balance$export"

params = {
    "filial_id": "9161160"
}

payload = {
    "warehouse_codes": [
        {
            "warehouse_code": "Toshkent sklad"
        }
    ],
    "filial_code": "01 Tashkent",
    "begin_date": "15.02.2025",
    "end_date": "15.02.2025"
}

username = "powerbi@epco"
password = "said_2021"

response = requests.post(
    url,
    params=params,
    auth=(username, password),
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload)
)

if response.status_code == 200:
    data = response.json()
    print("✅ Ma'lumot olindi")

    with open("final1.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("💾 Ma'lumot final.json fayliga saqlandi")
else:
    print("❌ Xato:", response.status_code, response.text)
