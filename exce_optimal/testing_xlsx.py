import pandas as pd
from sqlalchemy import create_engine
import urllib
import numpy as np


def get_sql_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BIT'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'NVARCHAR(255)'


def upload_to_sql(excel_file):
    try:
        print("🔌 Подключение к SQL Server...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=TAKEDA;"
            "DATABASE=Excel_data;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        xlsx = pd.ExcelFile(excel_file)
        for sheet_name in xlsx.sheet_names:
            print(f"Processing sheet: {sheet_name}")
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            df['month'] = 'iyul'
            table_name = sheet_name.replace(' ', '_').replace('-', '_').replace('.', '_')
            dtype_dict = {}
            for column in df.columns:
                dtype_dict[column] = get_sql_dtype(df[column].dtype)
            df = df.replace({np.nan: None})
            with engine.connect() as conn:
                conn.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
                columns_sql = ", ".join([f"[{col}] {dtype}" for col, dtype in dtype_dict.items()])
                create_table_sql = f"CREATE TABLE {table_name} ({columns_sql})"
                conn.execute(create_table_sql)
                df.to_sql(table_name, engine, if_exists='append', index=False)

                print(f"Sheet '{sheet_name}' has been uploaded to table '{table_name}'")

        print(f"All sheets have been uploaded to the database: Excel_data")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    excel_file = 'your_file.xlsx'
    upload_to_sql(excel_file)