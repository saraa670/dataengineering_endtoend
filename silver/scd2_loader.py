# scd2_loader.py
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

DB_USER = "postgres"
DB_PASS = "sa11%4022"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "adventureworks"

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, future=True)


def scd2_load(bronze_table, silver_table, business_key):
    print(f"\n=== Running SCD2 for {bronze_table} → {silver_table} ===")

    # Load bronze table
    bronze_df = pd.read_sql(f'SELECT * FROM "{bronze_table}"', engine)

    if bronze_df.empty:
        print(" Bronze table is empty — skipping")
        return

    # Check if silver table exists
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT to_regclass('{silver_table}')")
        ).scalar()

    # If silver doesn't exist → create initial snapshot
    if result is None:
        print("⏳ Creating initial Silver dimension…")

        bronze_df["effective_date"] = pd.Timestamp.now()
        bronze_df["expiry_date"] = pd.NaT
        bronze_df["is_current"] = True

        bronze_df.to_sql(silver_table, engine, index=False)
        print(" Silver table initialized.")
        return

    # Load current silver data
    silver_df = pd.read_sql(
        f'SELECT * FROM "{silver_table}" WHERE "is_current" = TRUE',
        engine
    )

    # List of columns to compare
    ignore_cols = {
        business_key, "effective_date", "expiry_date", "is_current",
        "load_timestamp", "ingestion_date"
    }
    tracked_cols = [c for c in bronze_df.columns if c not in ignore_cols]

    now = datetime.now()

    with engine.begin() as conn:
        for _, row in bronze_df.iterrows():
            key_value = row[business_key]

            # Get existing row for this key
            existing = silver_df[silver_df[business_key] == key_value]

            # Case 1: New key → insert as NEW dimension record
            if existing.empty:
                new = row.to_frame().T
                new["effective_date"] = now
                new["expiry_date"] = None
                new["is_current"] = True
                new.to_sql(silver_table, conn, if_exists="append", index=False)
                continue

            existing = existing.iloc[0]

            changed = False
            for col in tracked_cols:
                old = existing.get(col)
                new = row.get(col)

                if pd.isna(old) and pd.isna(new):
                    continue
                if old != new:
                    changed = True
                    break

            if changed:
                # EXPIRE existing
                conn.execute(
                    text(f'''
                        UPDATE "{silver_table}"
                        SET "is_current" = FALSE, "expiry_date" = :now
                        WHERE "{business_key}" = :key AND "is_current" = TRUE
                    '''),
                    {"now": now, "key": key_value}
                )

                # INSERT new updated record
                new_row = row.to_frame().T
                new_row["effective_date"] = now
                new_row["expiry_date"] = None
                new_row["is_current"] = True
                new_row.to_sql(silver_table, conn, if_exists="append", index=False)

    print("SCD2 applied successfully!")
