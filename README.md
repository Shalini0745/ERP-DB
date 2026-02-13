# Attendance ETL Pipeline using PySpark & PostgreSQL

## 📌 Project Overview
This project implements an end-to-end ETL pipeline for processing attendance data
using PySpark and PostgreSQL. The pipeline reads structured CSV data, applies
transformations, and loads the results into staging and data warehouse tables.

The solution simulates a real-world analytics workflow commonly used in ERP /
data engineering systems.

---

## 🛠️ Technology Stack
- PySpark
- PostgreSQL
- JDBC Connectivity
- Python
- Hadoop (local setup)

---

## 📂 Project Structure

attendance-project/
│
├── data/
│   └── attendance.csv
│
├── drivers/
│   └── postgresql-42.7.8.jar
│
├── src/
│   ├── load_stage_data.py
│   └── etl_stage_to_dw.py
│
├── README.md
└── .gitignore

---

## ⚙️ ETL Workflow

### 1️⃣ Stage Load (`load_stage_data.py`)
This script performs the initial ingestion of attendance records.

### Key Operations:
✔ Configure Hadoop & Spark environment  
✔ Define structured schema for CSV  
✔ Load attendance dataset into Spark DataFrame  
✔ Apply datatype conversions  
✔ Load data into PostgreSQL staging table  

### Output:
Database: `stage_db`  
Table: `attendance_project`

---

### 2️⃣ Stage → Data Warehouse (`etl_stage_to_dw.py`)
This script performs analytical transformations and warehouse loading.

### Key Operations:
✔ Read attendance dataset  
✔ Apply datatype conversions  
✔ Load Stage Table (`attendance_stage`)  
✔ Apply business transformation rules  
✔ Load Data Warehouse Table (`attendance_dw`)

### Example Transformation:
Temperature alerts are derived using rule-based logic:

- If `Temperature_Status = High` → `Temp_Alert = YES`
- Otherwise → `Temp_Alert = NO`

---

## 🗂️ Dataset Description
The attendance dataset includes:

- Student identity attributes
- Attendance metadata
- Check-in timestamps
- Temperature measurements
- Verification details
- Correction / source tracking fields

---

##  How to Run

### Step 1 — Start PostgreSQL
Ensure PostgreSQL is running and database is created

### Step 2 — Verify JDBC Driver
Place PostgreSQL JDBC driver inside

### Step 3 — Run Stage Load

### Step 4 — Run ETL Pipeline



##  Learning Outcomes
This project demonstrates practical concepts in:

✔ Data Engineering  
✔ ETL Pipeline Design  
✔ Schema Definition  
✔ Spark Transformations  
✔ JDBC Integration  
✔ Staging vs Data Warehouse Layers  

---

##  Future Enhancements
- Incremental data loads
- Data validation checks
- Fact & Dimension modelling
- Performance optimization
- BI dashboard integration (Power BI)

---

# Shalini Janarthanan  
B.Tech – Artificial Intelligence & Data Science

