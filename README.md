# 📊 Sales Data Pipeline & Power BI Dashboard

An end-to-end automated data engineering project that extracts raw sales data from a CSV file, transforms and loads it into SQL Server using a Python ETL pipeline, and visualizes it through an interactive Power BI Sales Dashboard.

* * *

## 🚀 Project Overview

This project demonstrates a complete automated data pipeline workflow:

**CSV File (Source)** → **Python ETL** → **SQL Server** → **Power BI Dashboard**

Raw sales data is ingested from a CSV file, cleaned and transformed using Python, loaded into a SQL Server database using a MERGE (Upsert) strategy, and visualized in a Power BI dashboard published to Power BI Service. The entire pipeline is automated using SQL Server Agent.

* * *

## 🛠️ Tools & Technologies

| Tool | Purpose |
| --- | --- |
| Python 3.x | ETL pipeline (extract, transform, load) |
| Pandas | Data cleaning and transformation |
| SQLAlchemy | SQL Server connection |
| SQL Server Developer Edition | Data storage and querying |
| SQL Server Agent | Pipeline automation and scheduling |
| Power BI Desktop | Dashboard creation |
| Power BI Service | Dashboard publishing and sharing |
| GitHub | Version control and portfolio |

* * *

## 📁 Project Structure

    📦 project-root
     ┣ 📂 data
     ┃ ┗ 📄 Sales.csv                          # Raw source CSV file
     ┣ 📂 scripts
     ┃ ┗ 📄 sales_pipeline_production_etl.py   # Python ETL script
     ┣ 📂 sql
     ┃ ┗ 📄 merge_sales.sql                    # SQL MERGE statement
     ┣ 📂 powerbi
     ┃ ┗ 📄 Sales_Dashboard.pbix               # Power BI dashboard file
     ┣ 📄 README.md

* * *

## ⚙️ How It Works

### Step 1 — Extract

* Raw sales data is read from `Sales.csv` using Python and Pandas
* Data is validated — checks for empty file and required columns

### Step 2 — Transform

* Data types are cleaned and corrected (e.g. OrderID converted to numeric)
* Invalid values are handled gracefully

### Step 3 — Load (Staging)

* Transformed data is loaded into `dbo.Sales_Staging` table in SQL Server
* Staging table is wiped and reloaded fresh on every run

### Step 4 — Merge (Upsert)

* A SQL MERGE statement syncs staging data into the final `dbo.Sales` table
* Existing records are **updated**, new records are **inserted**
* This ensures no duplicates and always up-to-date data

### Step 5 — Verify & Clean

* Row count is verified after merge
* Staging table is truncated after successful load

### Step 6 — Log

* Every pipeline run is logged to `dbo.ETL_Log` table and a text file
* Logs include run time, status (SUCCESS/FAILED), rows processed, and error messages

### Step 7 — Automate

* SQL Server Agent schedules the pipeline to run automatically every day
* No manual intervention required

* * *

## 📊 Dashboard Preview



* * *

## 🔗 Power BI Dashboard

👉 [https://app.powerbi.com/groups/5fcfb0e3-938b-4e2b-841d-870cc8e40c31/reports/8e509dce-0d6c-4662-abe6-70295591a47f/6526f4bed6607ca49589?experience=power-bi&clientSideAuth=0](YOUR_POWER_BI_LINK_HERE)

* * *

## 🏃 How to Run the ETL Pipeline

### Prerequisites

* Python 3.x installed
* SQL Server Developer Edition
* Required Python libraries (see below)

### Installation

    # Clone the repository
    git clone https://github.com/pthapa55/sales-etl-pipeline.git
    
    # Navigate to the project folder
    cd sales-etl-pipeline
    
    # Install dependencies
    pip install pandas sqlalchemy pyodbc

### Run the ETL Script

    cd scripts
    python sales_pipeline_production_etl.py

* * *

## 📋 ETL Log Table Structure

| Column | Description |
| --- | --- |
| RunID | Unique run identifier |
| Run_Time | Timestamp of pipeline run |
| Pipeline_Name | Name of the pipeline |
| Status | SUCCESS or FAILED |
| Rows_Processed | Number of rows loaded |
| Message | Success or error message |

* * *

## 🔄 Automation

The pipeline is fully automated using **SQL Server Agent**:

| Setting | Value |
| --- | --- |
| Schedule | Daily |
| Start Time | 8:00 AM |
| On Success | Quit with success |
| On Failure | Logged to ETL_Log table |

In a production/company environment this would be handled by **Azure Data Factory** or **Apache Airflow**.

* * *

## 👤 Author

**PRAKASH THAPA**

* LinkedIn: [Prakash Thapa - Kiewit | LinkedIn](https://www.linkedin.com/in/prakash-thapa-22aa91140/)]
* GitHub: [[GitHub - pthapa55/sales-etl-pipeline: Production ETL pipeline using Python, SQL Server, and Power BI · GitHub](https://github.com/pthapa55/sales-etl-pipeline)]
