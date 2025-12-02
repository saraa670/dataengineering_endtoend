# bronze_to_silver_scd2.py
import pandas as pd
import re
from sqlalchemy import create_engine, text
from datetime import datetime

# -----------------------------
#  DATABASE CONNECTION
# -----------------------------
DB_USER = "postgres"
DB_PASS = "sa11%4022"        # your password "sa11@22"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "adventureworks"

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)


# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------

def safe_read_table(table_name):
    """
    Try reading table both unquoted and quoted (case-sensitive)
    Return dataframe OR None
    """
    queries = [
        f"SELECT * FROM {table_name}",
        f'SELECT * FROM "{table_name}"'
    ]

    for q in queries:
        try:
            df = pd.read_sql(q, engine)
            print(f"SUCCESS: Read bronze table → {table_name}")
            return df
        except Exception:
            continue

    print(f"FAILED TO READ TABLE: {table_name}")
    return None


def detect_business_key(columns):
    """
    Detect primary key column automatically.
    Example: productCategoryId, customerId, productId, employeeId
    """
    for c in columns:
        if c.lower().endswith("id"):
            return c
    return None


# -------------------------------------------------------
#  MAIN PROCESS
# -------------------------------------------------------

with engine.connect() as conn:
    bronze_tables = conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'bronze_api_%'
    """)).fetchall()

bronze_tables = [t[0] for t in bronze_tables]

print("\n=== BRONZE TABLES DETECTED ===")
for t in bronze_tables:
    print(" -", t)
print()


# -------------------------------------------------------
# PROCESS EACH BRONZE TABLE
# -------------------------------------------------------

for bronze_table in bronze_tables:

    print(f"\n\n==========================")
    print(f"PROCESSING: {bronze_table}")
    print("==========================")

    df = safe_read_table(bronze_table)
    if df is None or df.empty:
        print("SKIPPED (empty or unreadable)")
        continue

    # business key detection
    business_key = detect_business_key(df.columns)
    if business_key is None:
        print(f"No business key found for {bronze_table} — skipping")
        continue

    # create silver table name
    silver_table = bronze_table.replace("bronze_api_", "silver_dim_")

    print(f"Detected business key: {business_key}")
    print(f"Silver table → {silver_table}")

    # ---------------------------------------
    # CHECK IF SILVER TABLE EXISTS
    # ---------------------------------------
    with engine.connect() as conn:
        exists = conn.execute(
            text(f"SELECT to_regclass('{silver_table}')")
        ).scalar()

    # =======================================
    # IF SILVER DOES NOT EXIST → CREATE INITIAL COPY
    # =======================================
    if exists is None:
        print("Creating silver table for first time...")

        df["effective_date"] = pd.Timestamp.now()
        df["expiry_date"] = pd.NaT
        df["is_current"] = True

        df.to_sql(silver_table, engine, index=False)
        print(f"✔ Created silver table with {len(df)} rows")
        continue

    # =======================================
    # IF EXISTS → APPLY SCD TYPE 2
    # =======================================
    silver_df = pd.read_sql(
        f'SELECT * FROM "{silver_table}" WHERE is_current = TRUE',
        engine
    )

    # which columns to compare
    ignore_cols = {
        business_key, "effective_date", "expiry_date",
        "is_current", "load_timestamp", "ingestion_date"
    }
    tracked_cols = [c for c in df.columns if c not in ignore_cols]

    now = pd.Timestamp.now()

    inserts_updates = []

    for _, row in df.iterrows():
        key_val = row[business_key]

        existing = silver_df[silver_df[business_key] == key_val]

        if existing.empty:
            inserts_updates.append(("insert", row))
        else:
            existing = existing.iloc[0]
            changed = False

            for col in tracked_cols:
                new_val = row[col]
                old_val = existing.get(col)

                if pd.isna(new_val) and pd.isna(old_val):
                    continue
                if new_val != old_val:
                    changed = True
                    break

            if changed:
                inserts_updates.append(("update", row))

    # =======================================
    # APPLY UPDATES
    # =======================================
    with engine.begin() as conn:
        for action, row in inserts_updates:

            key_val = row[business_key]

            if action == "update":
                conn.execute(text(f"""
                    UPDATE "{silver_table}"
                    SET is_current = FALSE,
                        expiry_date = :now
                    WHERE "{business_key}" = :key
                    AND is_current = TRUE
                """), {"now": now, "key": key_val})

            # insert new row
            new_row = row.to_frame().T
            new_row["effective_date"] = now
            new_row["expiry_date"] = None
            new_row["is_current"] = True
            new_row.to_sql(silver_table, conn.engine, index=False, if_exists="append")

    print(f"✔ SCD2 applied. Rows changed: {len(inserts_updates)}")


print("\n\n=============================")
print("      SCD2 PROCESS DONE      ")
print("=============================\n")
