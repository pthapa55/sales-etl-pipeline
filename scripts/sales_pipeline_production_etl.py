import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------
csv_path = r"C:\Users\Prakash\Documents\Python_Automation_Project\data\Sales.csv"
log_path = r"C:\Users\Prakash\Documents\Python_Automation_Project\pipeline_log.txt"

server = r"DESKTOP-1SMPQQM\SQLEXPRESS"
database = "Python_ETL_Pipeline"

pipeline_name = "Sales_Production_ETL"

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

try:
    print("🚀 Production ETL Started")

    # -------------------------------
    # 1. READ CSV
    # -------------------------------
    df = pd.read_csv(csv_path)

    if df.empty:
        raise ValueError("CSV is empty")

    if "OrderID" not in df.columns:
        raise ValueError("OrderID column missing")

    # Fix datatype
    df["OrderID"] = pd.to_numeric(df["OrderID"], errors="coerce")

    rows_processed = len(df)

    print(f"CSV Rows: {rows_processed}")

    # -------------------------------
    # 2. LOAD INTO STAGING TABLE
    # -------------------------------
    df.to_sql(
        "Sales_Staging",
        engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi"
    )

    print("Loaded into staging table")

    # -------------------------------
# 3. MERGE INTO FINAL TABLE
# -------------------------------
    with open(r"sql\merge_sales.sql", "r") as f:
        merge_sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    print("Merge completed (UPSERT done)")

    # -------------------------------
    # 4. VERIFY FINAL TABLE
    # -------------------------------
    total_rows = pd.read_sql(
        "SELECT COUNT(*) AS cnt FROM dbo.Sales",
        engine
    )["cnt"][0]

    print(f"Total rows in Sales: {total_rows}")

    # -------------------------------
    # 5. CLEAN STAGING TABLE
    # -------------------------------
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dbo.Sales_Staging"))

    # -------------------------------
    # 6. LOG SUCCESS (SQL TABLE)
    # -------------------------------
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dbo.ETL_Log 
            (Run_Time, Pipeline_Name, Status, Rows_Processed, Message)
            VALUES (GETDATE(), :name, 'SUCCESS', :rows, 'ETL completed successfully')
        """), {"name": pipeline_name, "rows": rows_processed})

    # -------------------------------
    # 7. LOG SUCCESS (TEXT FILE)
    # -------------------------------
    with open(log_path, "a") as f:
        f.write(f"{datetime.now()} - SUCCESS\n")

    print("✅ PRODUCTION ETL SUCCESSFUL")

except Exception as e:

    # -------------------------------
    # LOG FAILURE (SQL TABLE)
    # -------------------------------
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dbo.ETL_Log 
            (Run_Time, Pipeline_Name, Status, Rows_Processed, Message)
            VALUES (GETDATE(), :name, 'FAILED', 0, :msg)
        """), {"name": pipeline_name, "msg": str(e)})

    # -------------------------------
    # LOG FAILURE (TEXT FILE)
    # -------------------------------
    with open(log_path, "a") as f:
        f.write(f"{datetime.now()} - ERROR: {str(e)}\n")

    print("❌ PIPELINE FAILED:", e)