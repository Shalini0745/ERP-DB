import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.functions import col, when

# ---------------------------
# Environment setup
# ---------------------------
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"

# ---------------------------
# Spark session
# ---------------------------
spark = SparkSession.builder \
    .appName("Attendance ETL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark session started successfully!")

# ---------------------------
# PostgreSQL connection details
# ---------------------------
postgres_url = "jdbc:postgresql://localhost:5432/stage_db"  # replace with your DB
postgres_properties = {
    "user": "postgres",
    "password": "sha@17",
    "driver": "org.postgresql.Driver"
}

# ---------------------------
# Stage CSV file path
# ---------------------------
stage_file_path = "C:/Users/SHALINI/OneDrive/Desktop/attendance project/attendance.csv"

# ---------------------------
# Define schema for CSV
# ---------------------------
schema = StructType([
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("ID", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Attendance_Group", StringType(), True),
    StructField("Date", StringType(), True),  # Will convert to DateType later
    StructField("Week", StringType(), True),
    StructField("Check_In_Time", StringType(), True),  # Will convert to TimestampType
    StructField("Skin_Surface_Temperature", StringType(), True),
    StructField("Temperature_Status", StringType(), True),
    StructField("Card_Swiping_Type", StringType(), True),
    StructField("Verification_Method", StringType(), True),
    StructField("Attendance_Check_Point", StringType(), True),
    StructField("Custom_Name", StringType(), True),
    StructField("Data_Source", StringType(), True),
    StructField("Correction_Type", StringType(), True),
    StructField("Note", StringType(), True)
])

# ---------------------------
# Load stage CSV data
# ---------------------------
df_stage = spark.read.csv(stage_file_path, header=True, schema=schema)

# Convert Date and Timestamp columns
df_stage = df_stage.withColumn("Date", col("Date").cast(DateType()))
df_stage = df_stage.withColumn("Check_In_Time", col("Check_In_Time").cast(TimestampType()))

print(f"📥 Stage data loaded — {df_stage.count()} records")

# ---------------------------
# Write to PostgreSQL stage table
# ---------------------------
df_stage.write.jdbc(
    url=postgres_url,
    table="attendance_stage",  # public schema table
    mode="overwrite",  # use "append" if you don't want to overwrite
    properties=postgres_properties
)
print("✅ Stage data written to PostgreSQL successfully!")

# ---------------------------
# Data Warehouse transformations
# ---------------------------
# Example: Simple transformation - mark "High" temperature as alert
df_dw = df_stage.withColumn(
    "Temp_Alert",
    when(col("Temperature_Status") == "High", "YES").otherwise("NO")
)

# ---------------------------
# Write to PostgreSQL DW table
# ---------------------------
df_dw.write.jdbc(
    url=postgres_url,
    table="attendance_dw",  # public schema table
    mode="overwrite",
    properties=postgres_properties
)
print("✅ DW data written to PostgreSQL successfully!")

# ---------------------------
# Stop Spark session
# ---------------------------
spark.stop()
print("🚀 ETL job completed!")


