import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# Scope
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]

creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)

client = gspread.authorize(creds)

SHEET_ID = "1SVqA2Qp1848BAyoC39EbGrJax6hIqTsh"
sheet = client.open_by_key(SHEET_ID)

worksheet = sheet.get_worksheet(0)

df = pd.DataFrame(worksheet.get_all_records())

# Excelga saqlash
df.to_excel("output.xlsx", index=False)

print("✅ Ma’lumotlar output.xlsx fayliga yozildi!")
