# -*- coding: utf-8 -*-
import hashlib
import json
import platform
import re
import sys
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
import traceback

import pyodbc
import requests

print(sys.getdefaultencoding())

# ====== KONFIG ======
URL = "https://smartup.online/b/anor/mxsx/mkw/balance$export"
USERNAME = "powerbi@epco"
PASSWORD = "said_2021"
DATE_FORMAT = "%d.%m.%Y"

SQL_SERVER = "WIN-LORQJU2719N"
SQL_DATABASE = "SmartUp"
SQL_TRUSTED = "Yes"  # Windows auth

BEGIN_DATE_STR = "01.01.2025"
FILIAL_WAREHOUSE_JSON = "filial_warehouse.json"
PRODUCT_CONDITION_JSON = "product_conditions.json"

FACT_TABLE = "dbo.FactBalance"
GROUP_TABLE = "dbo.BalanceGroup"
CONDITION_TABLE = "dbo.BalanceCondition"
COLLATION = "Cyrillic_General_CI_AS"

INCREMENTAL_BUFFER_DAYS = 3

_WS_CHARS = "\u00A0\u202F\u2007"
_WS_TABLE = str.maketrans({c: " " for c in _WS_CHARS})


# ====== UTIL ======
def today_samarkand_date():
    return (datetime.utcnow() + timedelta(hours=5)).date()


def daterange(start_date: datetime, end_date: datetime, step_days: int = 30):
    current = start_date
    while current <= end_date:
        next_date = min(current + timedelta(days=step_days - 1), end_date)
        yield current, next_date
        current = next_date + timedelta(days=1)


def _pick_driver():
    drivers = [d.strip() for d in pyodbc.drivers()]
    for name in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]:
        if name in drivers:
            return name, drivers
    return None, drivers


def connect_sql():
    driver, all_drivers = _pick_driver()
    if not driver:
        arch = platform.architecture()[0]
        raise RuntimeError(
            f"ODBC drayver topilmadi. Python: {arch}. O'rnatilganlar: {all_drivers}\n"
            "Iltimos, 'ODBC Driver 17/18 for SQL Server' ni o'rnating."
        )
    print(f"➡️  Using ODBC driver: {{{driver}}}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection={SQL_TRUSTED};"
        "Encrypt=No;"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
    conn.setencoding(encoding="utf-16le")
    return conn


def _clean_str(s: str) -> str:
    return s.translate(_WS_TABLE).strip()


def to_date(val):
    if val is None:
        return None
    s = _clean_str(str(val))
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s[:19]).date()
    except Exception:
        return None


def to_float_decimal(val):
    """
    Return Decimal or None. Cleans common garbage like NBSP, commas, '—', 'N/A' etc.
    Use Decimal (not float) to avoid SQL conversion quirks.
    """
    if val is None:
        return None
    s = _clean_str(str(val))
    if s == "" or s.lower() in {"null", "nan", "n/a"} or s in {"-", "—"}:
        return None
    # remove spaces, replace comma with dot
    s = s.replace(" ", "").replace(",", ".")
    # keep only digits, dot, minus
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in {"", "-", ".", "-.", ".-"}:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        # try fallback with float->str
        try:
            return Decimal(str(float(s)))
        except Exception:
            return None


def safe_int(val):
    if val is None:
        return None
    s = _clean_str(str(val))
    if s == "" or s.lower() in {"null", "nan"} or s in {"-", "—"}:
        return None
    s = s.replace(" ", "")
    s = re.sub(r"[^0-9\-]", "", s)
    if s in {"", "-"}:
        return None
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except Exception:
            return None


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_balance_id(warehouse_id, product_id, batch_number, balance_date) -> str:
    if isinstance(balance_date, datetime):
        date_str = balance_date.date().isoformat()
    elif hasattr(balance_date, "isoformat"):
        date_str = balance_date.isoformat()
    else:
        date_str = str(balance_date or "")
    parts = [
        str(warehouse_id or ""),
        str(product_id or ""),
        str(batch_number or ""),
        date_str or ""
    ]
    return sha256("|".join(parts))


# Normalize a fact row before inserting into temp table
def normalize_fact_row(row):
    # expected order:
    # (balance_id, inv_kind, bal_date, warehouse_id, warehouse_code, product_code,
    #  product_barcode, product_id, card_code, expiry_date, serial_number, batch_number,
    #  quantity, measure_code, input_price, filial_id, filial_code)
    lst = list(row)
    # balance_date -> date or None
    if lst[2] is not None and not isinstance(lst[2], (date, datetime)):
        lst[2] = to_date(lst[2])
    # warehouse_id -> int or None
    lst[3] = safe_int(lst[3])
    # expiry_date -> date or None (index 9)
    if lst[9] is not None and not isinstance(lst[9], (date, datetime)):
        lst[9] = to_date(lst[9])
    # quantity -> Decimal or None (index 12)
    lst[12] = to_float_decimal(lst[12])
    # input_price -> Decimal or None (index 14)
    lst[14] = to_float_decimal(lst[14])
    # filial_id -> int or None (index 15)
    lst[15] = safe_int(lst[15])
    return tuple(lst)


# ====== DB Objects ======
def ensure_tables(cursor):
    cursor.execute(f"""
IF OBJECT_ID('{FACT_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {FACT_TABLE} (
        balance_id      CHAR(64)     NOT NULL PRIMARY KEY,
        inventory_kind  VARCHAR(50)   NULL,
        balance_date    DATE          NULL,
        warehouse_id    INT           NULL,
        warehouse_code  NVARCHAR(200) COLLATE {COLLATION} NULL,
        product_code    NVARCHAR(100) COLLATE {COLLATION} NULL,
        product_barcode NVARCHAR(100) COLLATE {COLLATION} NULL,
        product_id      NVARCHAR(50)  COLLATE {COLLATION} NULL,
        card_code       NVARCHAR(100) COLLATE {COLLATION} NULL,
        expiry_date     DATE          NULL,
        serial_number   NVARCHAR(100) COLLATE {COLLATION} NULL,
        batch_number    NVARCHAR(100) COLLATE {COLLATION} NULL,
        quantity        DECIMAL(18,4) NULL,
        measure_code    NVARCHAR(50)  COLLATE {COLLATION} NULL,
        input_price     DECIMAL(18,4) NULL,
        filial_id       INT           NULL,
        filial_code     NVARCHAR(100) COLLATE {COLLATION} NULL
    );
END
""")

    cursor.execute(f"""
IF OBJECT_ID('{GROUP_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {GROUP_TABLE} (
        balance_id  CHAR(64)      NOT NULL,
        group_code  NVARCHAR(100) COLLATE {COLLATION} NOT NULL,
        type_code   NVARCHAR(200) COLLATE {COLLATION} NULL,
        CONSTRAINT PK_BalanceGroup PRIMARY KEY (balance_id, group_code),
        CONSTRAINT FK_BalanceGroup_FactBalance
            FOREIGN KEY (balance_id) REFERENCES {FACT_TABLE}(balance_id)
    );
END
""")

    cursor.execute(f"""
IF OBJECT_ID('{CONDITION_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {CONDITION_TABLE} (
        balance_id        CHAR(64)      NOT NULL,
        product_condition NVARCHAR(50)  COLLATE {COLLATION} NOT NULL,
        CONSTRAINT PK_BalanceCondition PRIMARY KEY (balance_id, product_condition),
        CONSTRAINT FK_BalanceCondition_FactBalance
            FOREIGN KEY (balance_id) REFERENCES {FACT_TABLE}(balance_id)
    );
END
""")


def ensure_loadstate_table(cursor):
    cursor.execute("""
IF OBJECT_ID('dbo.LoadState_Balance','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoadState_Balance
  (
      scope_key         nvarchar(200) NOT NULL PRIMARY KEY, -- e.g. "filial=1|warehouse=65478|cond=T"
      last_balance_date date          NULL,
      last_run_utc      datetime2     NULL,
      last_rowcount     int           NULL
  );
END
""")


def make_scope_key(filial_id, warehouse_id, condition=None) -> str:
    if condition:
        return f"filial={filial_id}|warehouse={warehouse_id}|cond={condition}"
    return f"filial={filial_id}|warehouse={warehouse_id}"


def get_scope_state(cursor, scope_key: str):
    cursor.execute("SELECT last_balance_date FROM dbo.LoadState_Balance WHERE scope_key = ?", scope_key)
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_scope_state(cursor, scope_key: str, last_balance_date, rowcount: int):
    cursor.execute("""
    MERGE dbo.LoadState_Balance AS T
    USING (SELECT ? AS scope_key) AS S
       ON T.scope_key = S.scope_key
    WHEN MATCHED THEN UPDATE SET
        last_balance_date = CASE WHEN (? IS NULL OR ? > ISNULL(T.last_balance_date, '1900-01-01')) THEN ? ELSE T.last_balance_date END,
        last_run_utc      = SYSUTCDATETIME(),
        last_rowcount     = ?
    WHEN NOT MATCHED THEN
       INSERT (scope_key, last_balance_date, last_run_utc, last_rowcount)
       VALUES (S.scope_key, ?, SYSUTCDATETIME(), ?);
    """, scope_key, last_balance_date, last_balance_date, last_balance_date, rowcount,
                   last_balance_date, rowcount)


# ====== API → ROWS (single filial/warehouse at a time) ======
def fetch_balance_chunks(cursor, filial_entry, product_conditions, user_begin_date: datetime, user_end_date: datetime):
    filial_id = filial_entry.get("filial_id")
    filial_code = filial_entry.get("filial_code")
    warehouse_id = filial_entry.get("warehouse_id")
    warehouse_code = filial_entry.get("warehouse_code")

    session = requests.Session()
    fact_rows, group_rows, condition_rows = [], [], []
    seen_balance_ids, seen_group_pairs, seen_cond_pairs = set(), set(), set()
    total_items = 0

    for cond in product_conditions:
        scope_key = make_scope_key(filial_id, warehouse_id, cond)
        state_last = get_scope_state(cursor, scope_key)
        effective_begin = user_begin_date
        if state_last:
            eff = state_last - timedelta(days=INCREMENTAL_BUFFER_DAYS)
            if eff > effective_begin:
                effective_begin = eff
        effective_end = user_end_date
        if effective_begin > effective_end:
            print(f"↪️ Skip scope {scope_key}: effective_begin>{effective_end}")
            continue

        scope_max_balance_date = None
        scope_added_f = scope_added_g = scope_added_c = 0

        for start, finish in daterange(effective_begin, effective_end, step_days=30):
            payload = {
                "warehouse_codes": [{"warehouse_code": warehouse_code}],
                "filial_code": filial_code,
                "begin_date": start.strftime(DATE_FORMAT),
                "end_date": finish.strftime(DATE_FORMAT),
                "product_conditions": [cond]
            }
            try:
                resp = session.post(
                    URL,
                    params={"filial_id": filial_id},
                    auth=(USERNAME, PASSWORD),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    timeout=120,
                )
                resp.encoding = "utf-8"
                resp.raise_for_status()
                data = resp.json()
                balance = data.get("balance", []) or []
                total_items += len(balance)

                added_f = added_g = added_c = 0
                for item in balance:
                    bal_date = to_date(item.get("date"))
                    balance_id = make_balance_id(warehouse_id, item.get("product_id"), item.get("batch_number"), bal_date)

                    if balance_id not in seen_balance_ids:
                        fact_rows.append((
                            balance_id, item.get("inventory_kind"), bal_date,
                            safe_int(warehouse_id), warehouse_code, item.get("product_code"),
                            item.get("product_barcode"), item.get("product_id"), item.get("card_code"),
                            to_date(item.get("expiry_date")), item.get("serial_number"), item.get("batch_number"),
                            to_float_decimal(item.get("quantity")), item.get("measure_code"), to_float_decimal(item.get("input_price")),
                            safe_int(filial_id), filial_code
                        ))
                        seen_balance_ids.add(balance_id)
                        added_f += 1
                        if bal_date and (scope_max_balance_date is None or bal_date > scope_max_balance_date):
                            scope_max_balance_date = bal_date

                    groups = item.get("groups") or [{"group_code": None, "type_code": None}]
                    for g in groups:
                        gc, tc = g.get("group_code"), g.get("type_code")
                        if (balance_id, gc) not in seen_group_pairs:
                            group_rows.append((balance_id, gc, tc))
                            seen_group_pairs.add((balance_id, gc))
                            added_g += 1

                    if (balance_id, cond) not in seen_cond_pairs:
                        condition_rows.append((balance_id, cond))
                        seen_cond_pairs.add((balance_id, cond))
                        added_c += 1

                scope_added_f += added_f
                scope_added_g += added_g
                scope_added_c += added_c

                print(f"✅ {start.strftime(DATE_FORMAT)}-{finish.strftime(DATE_FORMAT)} | {scope_key} | items:{len(balance)} → +F:{added_f}, +G:{added_g}, +C:{added_c}")

            except Exception as e:
                print(f"⚠️ API xatosi | {scope_key} | {start.strftime(DATE_FORMAT)}-{finish.strftime(DATE_FORMAT)} | {e}")
                traceback.print_exc()

        if scope_max_balance_date:
            upsert_scope_state(cursor, scope_key, scope_max_balance_date, scope_added_f)

    print(f"Σ API items: {total_items} | fact:{len(fact_rows)} | group:{len(group_rows)} | cond:{len(condition_rows)}")
    return fact_rows, group_rows, condition_rows


# ====== MAIN ======
def main():
    # read configs
    with open(FILIAL_WAREHOUSE_JSON, "r", encoding="utf-8") as f:
        filial_warehouse_list = json.load(f)

    with open(PRODUCT_CONDITION_JSON, "r", encoding="utf-8") as f:
        cond_json = json.load(f)
    product_conditions = [p for entry in cond_json for p in entry.get("product_conditions", [])]

    begin_date = datetime.strptime(BEGIN_DATE_STR, DATE_FORMAT)
    end_date = datetime.strptime(today_samarkand_date().strftime(DATE_FORMAT), DATE_FORMAT)

    conn = connect_sql()
    cursor = conn.cursor()
    ensure_tables(cursor)
    ensure_loadstate_table(cursor)
    conn.commit()
    print("✅ SQL tayyor")

    # error rows collector
    error_rows = []

    for filial_entry in filial_warehouse_list:
        print(f"\n--- Processing filial {filial_entry.get('filial_id')} / warehouse {filial_entry.get('warehouse_id')} ---")
        fact_rows, group_rows, condition_rows = fetch_balance_chunks(cursor, filial_entry, product_conditions, begin_date, end_date)
        if not (fact_rows or group_rows or condition_rows):
            print("No rows for this filial/warehouse.")
            continue

        # create temp tables
        cursor.execute(f"""
IF OBJECT_ID('tempdb..#TmpFact') IS NOT NULL DROP TABLE #TmpFact;
CREATE TABLE #TmpFact(
    balance_id CHAR(64), inventory_kind VARCHAR(50), balance_date DATE,
    warehouse_id INT, warehouse_code NVARCHAR(200) COLLATE {COLLATION},
    product_code NVARCHAR(100) COLLATE {COLLATION}, product_barcode NVARCHAR(100) COLLATE {COLLATION},
    product_id NVARCHAR(50) COLLATE {COLLATION}, card_code NVARCHAR(100) COLLATE {COLLATION},
    expiry_date DATE, serial_number NVARCHAR(100) COLLATE {COLLATION}, batch_number NVARCHAR(100) COLLATE {COLLATION},
    quantity DECIMAL(18,4), measure_code NVARCHAR(50) COLLATE {COLLATION}, input_price DECIMAL(18,4),
    filial_id INT, filial_code NVARCHAR(100) COLLATE {COLLATION}
);
IF OBJECT_ID('tempdb..#TmpGroup') IS NOT NULL DROP TABLE #TmpGroup;
CREATE TABLE #TmpGroup(balance_id CHAR(64), group_code NVARCHAR(100) COLLATE {COLLATION}, type_code NVARCHAR(200) COLLATE {COLLATION});
IF OBJECT_ID('tempdb..#TmpCond') IS NOT NULL DROP TABLE #TmpCond;
CREATE TABLE #TmpCond(balance_id CHAR(64), product_condition NVARCHAR(50) COLLATE {COLLATION});
""")

        # normalize rows
        norm_fact_rows = [normalize_fact_row(r) for r in fact_rows]
        norm_group_rows = [(
            # ensure group_code/type are strings or None
            r[0],
            (r[1] if r[1] is not None else None),
            (r[2] if r[2] is not None else None)
        ) for r in group_rows]
        norm_cond_rows = [(r[0], r[1]) for r in condition_rows]

        # bulk insert with fallback
        insert_fact_sql = "INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        insert_group_sql = "INSERT INTO #TmpGroup VALUES (?,?,?)"
        insert_cond_sql = "INSERT INTO #TmpCond VALUES (?,?)"

        # FACT
        try:
            cursor.fast_executemany = True
            if norm_fact_rows:
                cursor.executemany(insert_fact_sql, norm_fact_rows)
        except pyodbc.Error as e:
            print(f"⚠️ fast_executemany muammo (Fact): {e}. Per-row fallback boshlanmoqda.")
            cursor.fast_executemany = False
            # per-row fallback and collect bad rows
            for r in norm_fact_rows:
                try:
                    cursor.execute(insert_fact_sql, r)
                except Exception as ex:
                    # log problematic row
                    print("❌ Fact row failed, logging and skipping. Error:", ex)
                    error_rows.append({"type": "fact", "row": r, "error": str(ex)})
                    # try sanitize numeric fields to None and retry
                    r2 = list(r)
                    r2[12] = None  # quantity
                    r2[14] = None  # input_price
                    try:
                        cursor.execute(insert_fact_sql, tuple(r2))
                    except Exception as ex2:
                        print("  still failed after sanitize:", ex2)
                        error_rows.append({"type": "fact_sanitized", "row": tuple(r2), "error": str(ex2)})
                        continue

        # GROUP
        try:
            if norm_group_rows:
                cursor.executemany(insert_group_sql, norm_group_rows)
        except pyodbc.Error as e:
            print(f"⚠️ fast_executemany muammo (Group): {e}. Per-row fallback.")
            for r in norm_group_rows:
                try:
                    cursor.execute(insert_group_sql, r)
                except Exception as ex:
                    print("❌ Group row failed, logging and skipping. Error:", ex)
                    error_rows.append({"type": "group", "row": r, "error": str(ex)})
                    continue

        # COND
        try:
            if norm_cond_rows:
                cursor.executemany(insert_cond_sql, norm_cond_rows)
        except pyodbc.Error as e:
            print(f"⚠️ fast_executemany muammo (Cond): {e}. Per-row fallback.")
            for r in norm_cond_rows:
                try:
                    cursor.execute(insert_cond_sql, r)
                except Exception as ex:
                    print("❌ Cond row failed, logging and skipping. Error:", ex)
                    error_rows.append({"type": "cond", "row": r, "error": str(ex)})
                    continue

        # MERGE into main tables
        try:
            cursor.execute(f"""
MERGE {FACT_TABLE} AS T
USING (SELECT DISTINCT * FROM #TmpFact) AS S
ON (T.balance_id = S.balance_id)
WHEN MATCHED THEN UPDATE SET
    inventory_kind = S.inventory_kind, balance_date = S.balance_date, warehouse_id = S.warehouse_id,
    warehouse_code = S.warehouse_code, product_code = S.product_code, product_barcode = S.product_barcode,
    product_id = S.product_id, card_code = S.card_code, expiry_date = S.expiry_date, serial_number = S.serial_number,
    batch_number = S.batch_number, quantity = S.quantity, measure_code = S.measure_code,
    input_price = S.input_price, filial_id = S.filial_id, filial_code = S.filial_code
WHEN NOT MATCHED THEN
    INSERT (balance_id, inventory_kind, balance_date, warehouse_id, warehouse_code, product_code, product_barcode,
            product_id, card_code, expiry_date, serial_number, batch_number, quantity, measure_code, input_price,
            filial_id, filial_code)
    VALUES (S.balance_id, S.inventory_kind, S.balance_date, S.warehouse_id, S.warehouse_code, S.product_code, S.product_barcode,
            S.product_id, S.card_code, S.expiry_date, S.serial_number, S.batch_number, S.quantity, S.measure_code, S.input_price,
            S.filial_id, S.filial_code);
""")
            cursor.execute(f"""
MERGE {GROUP_TABLE} AS T
USING (SELECT DISTINCT balance_id, ISNULL(group_code, N'__NULL__') AS group_code, type_code FROM #TmpGroup) AS S
ON (T.balance_id = S.balance_id AND T.group_code = S.group_code)
WHEN MATCHED THEN UPDATE SET type_code = S.type_code
WHEN NOT MATCHED THEN INSERT (balance_id, group_code, type_code) VALUES (S.balance_id, S.group_code, S.type_code);
""")
            cursor.execute(f"""
MERGE {CONDITION_TABLE} AS T
USING (SELECT DISTINCT balance_id, product_condition FROM #TmpCond) AS S
ON (T.balance_id = S.balance_id AND T.product_condition = S.product_condition)
WHEN NOT MATCHED THEN INSERT (balance_id, product_condition) VALUES (S.balance_id, S.product_condition);
""")
        except Exception as ex:
            print("❌ MERGE step failed:", ex)
            traceback.print_exc()

        # drop temp tables & commit
        cursor.execute("DROP TABLE #TmpFact; DROP TABLE #TmpGroup; DROP TABLE #TmpCond;")
        conn.commit()
        print(f"💾 Yuklandi | Fact:{len(fact_rows)} | Group:{len(group_rows)} | Cond:{len(condition_rows)}")

    # save errors if any
    if error_rows:
        with open("smartup_insert_errors.json", "w", encoding="utf-8") as ef:
            json.dump(error_rows, ef, ensure_ascii=False, default=str, indent=2)
        print(f"⚠️ Ba'zi satrlar qo'shilmadi. smartup_insert_errors.json faylga yozildi ({len(error_rows)} satr).")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
