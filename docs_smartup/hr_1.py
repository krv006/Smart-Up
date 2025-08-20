import hashlib
import io
import re

import gspread
import pandas as pd
import pyodbc
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SHEET_OR_FILE_ID = "1SVqA2Qp1848BAyoC39EbGrJax6hIqTsh"
SA_PATH = "credentials.json"

DB_CONN_STR = (
    "DRIVER={SQL Server};"
    "SERVER=WIN-LORQJU2719N;"
    "DATABASE=SmartUp;"
    "Trusted_Connection=yes;"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def clean_table_name(name: str) -> str:
    """SQL Server uchun xavfsiz jadval nomi yaratadi"""
    name = re.sub(r"\W+", "_", name)
    return name[:50]


def detect_sql_type(series: pd.Series) -> str:
    """Pandas ustun turiga qarab SQL turini belgilash"""
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(series):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)"


def get_sheets_data(file_id: str, creds) -> dict:
    """Google Sheets yoki Excel fayldan barcha sheetlarni DF sifatida qaytaradi"""
    drive = build("drive", "v3", credentials=creds)
    meta = drive.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    mime = meta["mimeType"]

    sheets_data = {}

    if mime == "application/vnd.google-apps.spreadsheet":
        client = gspread.authorize(creds)
        sh = client.open_by_key(file_id)
        for ws in sh.worksheets():
            df = pd.DataFrame(ws.get_all_records())
            sheets_data[ws.title] = df

    elif mime in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
    ):
        request = drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)

        xls = pd.ExcelFile(fh)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            sheets_data[sheet_name] = df

    else:
        raise RuntimeError("Noto‘g‘ri fayl turi!")

    return sheets_data


def write_to_sql(sheets_data: dict):
    """DataFrame’larni SQL Server ga yozish (dublikatlarsiz)"""
    conn = pyodbc.connect(DB_CONN_STR)
    cursor = conn.cursor()

    for sheet_name, df in sheets_data.items():
        if df.empty:
            print(f"⚠️ {sheet_name} bo‘sh, o‘tkazildi.")
            continue

        table_name = clean_table_name(sheet_name)
        print(f"📥 {table_name} jadvaliga tekshirilmoqda... ({len(df)} qator)")

        # Jadval yaratish (agar yo‘q bo‘lsa)
        cols = ", ".join([f"[{col}] {detect_sql_type(df[col])}" for col in df.columns])
        cols += ", [row_hash] CHAR(32)"  # dublikatni aniqlash uchun hash
        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NULL 
            CREATE TABLE {table_name} ({cols}, CONSTRAINT UQ_{table_name}_hash UNIQUE (row_hash))
        """)

        # Hash qo‘shish
        df["row_hash"] = df.astype(str).sum(axis=1).apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        inserted = 0
        placeholders = ", ".join(["?"] * len(df.columns))
        col_names = ",".join([f"[{c}]" for c in df.columns])
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        for _, row in df.iterrows():
            try:
                cursor.execute(insert_sql, tuple(None if pd.isna(x) else str(x) for x in row))
                inserted += 1
            except pyodbc.IntegrityError:
                # Dublikat row_hash bo‘lsa, tashlab ketamiz
                pass

        conn.commit()
        print(f"✅ {table_name} ga {inserted} yangi qator qo‘shildi ({len(df) - inserted} dublikat).")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    creds = Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
    sheets_data = get_sheets_data(SHEET_OR_FILE_ID, creds)
    write_to_sql(sheets_data)
    print("🎯 Barcha sheetlar SmartUp DB ga muvaffaqiyatli yuklandi!")
