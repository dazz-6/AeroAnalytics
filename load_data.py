import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

print("🔥 Script Started")

# =============================================================================
# 1. LOAD ENV
# =============================================================================
load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "aeroshield")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
OUTPUT_DIR  = os.getenv("OUTPUT_DIR", "./outputs")

# =============================================================================
# 2. CONNECT DB
# =============================================================================
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()
print("✅ Connected to PostgreSQL")

try:

    # =========================================================================
    # 3. LOAD SMALL TABLES
    # =========================================================================
    # Each table only loads the exact columns defined in the SQL schema.
    # Extra columns produced by the pipeline (norm_disruption, delay_penalty
    # etc.) are dropped before COPYing so PostgreSQL never sees them.
    # =========================================================================
    print("\n🚀 Loading small tables...\n")

    # Truncate all small tables first to avoid duplicate key errors on re-runs
    for t in ["airport_summary","route_summary","carrier_summary",
              "monthly_summary","delay_cause_summary"]:
        cur.execute(f"TRUNCATE TABLE {t} CASCADE;")
    conn.commit()
    print("🗑️  All small tables truncated.\n")

    # For each table: (csv filename, postgres table, exact columns to keep, renames)
    small_tables = [
        (
            "airport_summary.csv",
            "airport_summary",
            # Exact columns from CREATE TABLE airport_summary in queries.sql
            ["origin","total_flights","disrupted_flights","avg_dep_delay",
             "avg_arr_delay","cancellation_rate","diversion_rate",
             "disruption_rate","afi"],
            {},
        ),
        (
            "route_summary.csv",
            "route_summary",
            # Exact columns from CREATE TABLE route_summary in queries.sql
            ["route","total_flights","disrupted_flights","avg_dep_delay",
             "avg_arr_delay","avg_distance","disruption_rate","rrs"],
            {},
        ),
        (
            "carrier_summary.csv",
            "carrier_summary",
            # Exact columns from CREATE TABLE carrier_summary in queries.sql
            ["carrier","total_flights","disrupted_flights","avg_dep_delay",
             "avg_arr_delay","cancellations","diversions",
             "disruption_rate","cancellation_rate"],
            {"op_unique_carrier": "carrier"},
        ),
        (
            "monthly_summary.csv",
            "monthly_summary",
            # Exact columns from CREATE TABLE monthly_summary in queries.sql
            ["year","month","total_flights","disrupted_flights",
             "avg_dep_delay","cancellations","disruption_rate"],
            {},
        ),
        (
            "delay_cause_summary.csv",
            "delay_cause_summary",
            # Exact columns from CREATE TABLE delay_cause_summary in queries.sql
            ["cause","total_minutes","avg_minutes","flight_count","pct_of_total"],
            {},
        ),
    ]

    for fname, table, keep_cols, renames in small_tables:
        try:
            fpath = os.path.join(OUTPUT_DIR, fname)
            if not os.path.exists(fpath):
                print(f"⚠️  Skipped (file not found): {fname}")
                continue

            df = pd.read_csv(fpath)

            # Lowercase all columns
            df.columns = df.columns.str.lower().str.strip()

            # Apply renames (e.g. op_unique_carrier → carrier)
            if renames:
                df.rename(columns=renames, inplace=True)

            # Keep ONLY the columns that exist in the SQL table
            available = [c for c in keep_cols if c in df.columns]
            missing   = [c for c in keep_cols if c not in df.columns]
            if missing:
                print(f"⚠️  {fname}: columns not found in CSV (skipped): {missing}")
            df = df[available]

            # Write normalised temp CSV
            tmp_path = fpath + ".tmp"
            df.to_csv(tmp_path, index=False, na_rep="")

            columns = ", ".join(df.columns)
            with open(tmp_path, "r") as f:
                cur.copy_expert(
                    f"COPY {table} ({columns}) FROM STDIN WITH CSV HEADER", f
                )

            conn.commit()
            os.remove(tmp_path)
            print(f"✅ Loaded {table}  ({len(df):,} rows)")

        except Exception as e:
            conn.rollback()
            print(f"❌ Error loading {table}: {e}")

    # =========================================================================
    # 4. PREPARE FLIGHTS DATA
    # =========================================================================
    print("\n🧹 Preparing flights_cleaned...\n")

    file_path = os.path.join(OUTPUT_DIR, "flights_cleaned.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"flights_cleaned.csv not found in {OUTPUT_DIR}. "
            "Run aeroshield_pipeline.py first."
        )

    df = pd.read_csv(file_path, nrows=200_000, low_memory=False)

    # Step 1: lowercase all columns
    df.columns = df.columns.str.lower().str.strip()

    # Step 2: rename to match SQL schema
    df.rename(columns={
        "op_unique_carrier": "carrier",
        "op_carrier_fl_num": "flight_num",
    }, inplace=True)

    # Step 3: fix date
    df["fl_date"] = pd.to_datetime(df["fl_date"], errors="coerce")

    # Step 4: recalculate day_of_week (1=Mon, 7=Sun)
    df["day_of_week"] = df["fl_date"].dt.dayofweek + 1

    # Step 5: fix SMALLINT columns — SQL cannot accept float strings like "1726.0"
    #   Convert to numeric, round, then fill NaN with 0, cast to int-compatible
    smallint_cols = [
        "year","month","day_of_week","crs_dep_time","dep_time",
        "crs_arr_time","arr_time","dep_del15","arr_del15",
        "cancelled","diverted","disrupted","is_weekend",
        "dep_hour","arr_hour",
    ]
    for col in smallint_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")

    # Step 6: coerce NUMERIC columns (allow NaN → written as empty = SQL NULL)
    numeric_cols = [
        "dep_delay","arr_delay","taxi_out","taxi_in","air_time",
        "distance","carrier_delay","weather_delay","nas_delay",
        "security_delay","late_aircraft_delay",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Step 7: select only columns defined in the SQL schema
    final_cols = [
        "year","month","day_of_week","fl_date",
        "carrier","tail_num","flight_num",
        "origin","dest","route",
        "crs_dep_time","dep_time","dep_delay","dep_del15",
        "taxi_out","taxi_in",
        "crs_arr_time","arr_time","arr_delay","arr_del15",
        "cancelled","diverted","air_time","distance",
        "carrier_delay","weather_delay","nas_delay",
        "security_delay","late_aircraft_delay",
        "disrupted","is_weekend","dep_hour","arr_hour","time_block",
    ]

    available_cols = [c for c in final_cols if c in df.columns]
    missing_cols   = [c for c in final_cols if c not in df.columns]
    if missing_cols:
        print(f"⚠️  Columns missing from CSV (skipped): {missing_cols}")

    df = df[available_cols]

    fixed_path = os.path.join(OUTPUT_DIR, "flights_cleaned_fixed.csv")
    df.to_csv(fixed_path, index=False, na_rep="")
    print(f"✅ Prepared {len(df):,} rows  →  flights_cleaned_fixed.csv")

    # =========================================================================
    # 5. LOAD FLIGHTS USING COPY
    # =========================================================================
    print("\n🚀 Loading flights_cleaned...\n")

    cur.execute("TRUNCATE TABLE flights_cleaned CASCADE;")

    with open(fixed_path, "r") as f:
        cur.copy_expert(
            f"COPY flights_cleaned ({', '.join(available_cols)}) FROM STDIN WITH CSV HEADER",
            f,
        )

    conn.commit()
    print("🎉 ALL DATA LOADED SUCCESSFULLY 🚀")

except Exception as e:
    conn.rollback()
    print(f"\n💥 Fatal error: {e}")
    raise

finally:
    cur.close()
    conn.close()
    print("🔒 Database connection closed.")
