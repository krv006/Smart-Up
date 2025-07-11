import pandas as pd
import os
from sqlalchemy import create_engine, text, event
import urllib
import numpy as np
import pyodbc
import time


def get_sql_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BIT'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'NVARCHAR(255)'


def test_connection(conn_str):
    try:
        print("🔌 Testing SQL Server connection...")
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        print(f"✅ Connected to SQL Server: {version[0]}")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {str(e)}")
        return False


def create_database_if_not_exists(conn_str, database_name):
    try:
        master_conn_str = conn_str.replace(f"DATABASE={database_name};", "DATABASE=master;")
        conn = pyodbc.connect(master_conn_str, timeout=10)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database_name}')
            CREATE DATABASE [{database_name}]
        """)
        print(f"✅ Database '{database_name}' is ready.")
        conn.close()
    except Exception as e:
        print(f"❌ Failed to create/check database: {str(e)}")
        raise


def upload_to_sql(excel_file):
    try:
        # 🔧 Connection string (pyodbc uchun)
        raw_conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Excel_data;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )

        # 🔐 SQLAlchemy uchun URL format
        quoted_conn_str = urllib.parse.quote_plus(raw_conn_str)

        # 📂 Create database if not exists
        create_database_if_not_exists(raw_conn_str, "Excel_data")

        # ✅ Test connection
        if not test_connection(raw_conn_str):
            raise Exception("❌ Could not connect to SQL Server")

        # ⚙️ SQLAlchemy engine
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}", fast_executemany=True)

        # ⚡️ Enable fast_executemany before inserts
        @event.listens_for(engine, "before_cursor_execute")
        def set_fast_executemany(conn, cursor, statement, parameters, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        # 📖 Excel file ochish
        xlsx = pd.ExcelFile(excel_file)
        print(f"📑 {len(xlsx.sheet_names)} ta sheet topildi")

        for sheet_name in xlsx.sheet_names:
            start_time = time.time()
            print(f"\n📄 Processing sheet: {sheet_name}")

            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            print(f"🔍 {len(df)} ta row o‘qildi")

            df['month'] = 'iyul'

            table_name = 'T_' + ''.join(c if c.isalnum() or c == '_' else '_' for c in sheet_name)

            dtype_dict = {col: get_sql_dtype(df[col].dtype) for col in df.columns}
            df = df.replace({np.nan: None})

            with engine.connect() as conn:
                drop_query = text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]")
                conn.execute(drop_query)

                columns_sql = ", ".join([f"[{col}] {dtype}" for col, dtype in dtype_dict.items()])
                create_query = text(f"CREATE TABLE [{table_name}] ({columns_sql})")
                print(f"🧱 Creating table: {table_name}")
                conn.execute(create_query)

            # ⚡️ Insert with fast_executemany
            with engine.begin() as connection:
                print(f"⬆️ Inserting data into: {table_name}")
                df.to_sql(
                    table_name,
                    con=connection,
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )

            print(f"✅ Sheet '{sheet_name}' uploaded in {time.time() - start_time:.2f} sec")

        print("\n🎉 All sheets uploaded successfully to 'Excel_data' database!")

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise


if __name__ == "__main__":
    excel_file = 'Region planlari Iyul BI uchun.xlsx'
    upload_to_sql(excel_file)

# import pandas as pd
# import os
# from sqlalchemy import create_engine, text
# import urllib
# import numpy as np
# import pyodbc
# import time
#
#
# def get_sql_dtype(dtype):
#     if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
#         return 'FLOAT'
#     elif pd.api.types.is_bool_dtype(dtype):
#         return 'BIT'
#     elif pd.api.types.is_datetime64_any_dtype(dtype):
#         return 'DATETIME'
#     else:
#         return 'NVARCHAR(255)'
#
#
# def test_connection(conn_str):
#     try:
#         print("🔌 Testing SQL Server connection...")
#         conn = pyodbc.connect(conn_str, timeout=10)
#         cursor = conn.cursor()
#         cursor.execute("SELECT @@VERSION")
#         version = cursor.fetchone()
#         print(f"✅ Connected to SQL Server: {version[0]}")
#         conn.close()
#         return True
#     except Exception as e:
#         print(f"❌ Connection test failed: {str(e)}")
#         return False
#
#
# def create_database_if_not_exists(conn_str, database_name):
#     try:
#         master_conn_str = conn_str.replace(f"DATABASE={database_name};", "DATABASE=master;")
#         conn = pyodbc.connect(master_conn_str, timeout=10)
#         conn.autocommit = True
#         cursor = conn.cursor()
#
#         cursor.execute(f"""
#             IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database_name}')
#             CREATE DATABASE [{database_name}]
#         """)
#         print(f"✅ Database '{database_name}' checked/created successfully")
#         conn.close()
#     except Exception as e:
#         print(f"❌ Failed to create/check database: {str(e)}")
#         raise
#
#
# def upload_to_sql(excel_file):
#     try:
#         # 🔧 Connection string (pyodbc uchun to‘g‘ridan-to‘g‘ri)
#         raw_conn_str = (
#             "DRIVER={ODBC Driver 17 for SQL Server};"
#             "SERVER=localhost;"
#             "DATABASE=Excel_data;"
#             "Trusted_Connection=yes;"
#             "TrustServerCertificate=yes;"
#         )
#
#         # 🔐 SQLAlchemy uchun URL formatda quote qilish
#         encoded_conn_str = urllib.parse.quote_plus(raw_conn_str)
#
#         # 📂 Create database if not exists
#         create_database_if_not_exists(raw_conn_str, "Excel_data")
#
#         # ✅ Test connection
#         if not test_connection(raw_conn_str):
#             raise Exception("❌ Could not connect to SQL Server")
#
#         # ⚙️ SQLAlchemy engine
#         engine = create_engine(f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}", connect_args={'connect_timeout': 10})
#
#         # 📖 Excel file ochish
#         xlsx = pd.ExcelFile(excel_file)
#         print(f"📑 {len(xlsx.sheet_names)} ta sheet topildi")
#
#         for sheet_name in xlsx.sheet_names:
#             start_time = time.time()
#             print(f"\n📄 Processing sheet: {sheet_name}")
#
#             df = pd.read_excel(xlsx, sheet_name=sheet_name)
#             print(f"🔍 {len(df)} ta row o‘qildi")
#
#             df['month'] = 'iyul'
#
#             table_name = 'T_' + ''.join(c if c.isalnum() or c == '_' else '_' for c in sheet_name)
#
#             dtype_dict = {col: get_sql_dtype(df[col].dtype) for col in df.columns}
#             df = df.replace({np.nan: None})
#
#             with engine.connect() as conn:
#                 drop_query = text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]")
#                 conn.execute(drop_query)
#
#                 columns_sql = ", ".join([f"[{col}] {dtype}" for col, dtype in dtype_dict.items()])
#                 create_query = text(f"CREATE TABLE [{table_name}] ({columns_sql})")
#                 print(f"🧱 Creating table: {table_name}")
#                 conn.execute(create_query)
#
#                 print(f"⬆️ Inserting data into: {table_name}")
#                 df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
#
#                 conn.commit()
#                 print(f"✅ Sheet '{sheet_name}' uploaded in {time.time() - start_time:.2f} sec")
#
#         print("\n🎉 All sheets uploaded successfully to 'Excel_data' database!")
#
#     except Exception as e:
#         print(f"\n❌ Error: {str(e)}")
#         raise
#
#
# if __name__ == "__main__":
#     excel_file = 'Region planlari Iyul BI uchun.xlsx'
#     upload_to_sql(excel_file)
