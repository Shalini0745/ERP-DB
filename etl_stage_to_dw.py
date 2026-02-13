import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.functions import col, when


def run_etl_pipeline():

    # ---------------------------
    # Configure Hadoop environment
    # ---------------------------
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"

    # ---------------------------
    # Create Spark session
    # ---------------------------
    spark = (
        SparkSession.builder
        .appName("Attendance ETL Pipeline")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started successfully")

    # ---------------------------
    # PostgreSQL connection config
    # ---------------------------
    postgres_url = "jdbc:postgresql://localhost:5432/stage_db"

    postgres_properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # ---------------------------
    # Source CSV file path
    # ---------------------------
    stage_file_path = "C:/project/data/attendance.csv"

    # ---------------------------
    # Define schema for dataset
    # ---------------------------
    schema = StructType([
        StructField("First_Name", StringType(), True),
        StructField("Last_Name", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Attendance_Group", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Week", StringType(), True),
        StructField("Check_In_Time", StringType(), True),
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
    # Load CSV into Spark DataFrame
    # ---------------------------
    df_stage = spark.read.csv(stage_file_path, header=True, schema=schema)

    # Apply datatype conversions
    df_stage = df_stage.withColumn("Date", col("Date").cast(DateType()))
    df_stage = df_stage.withColumn("Check_In_Time", col("Check_In_Time").cast(TimestampType()))

    record_count = df_stage.count()
    print(f"📥 Stage data loaded — {record_count} records")

    # ---------------------------
    # Write Stage Table
    # ---------------------------
    df_stage.write.jdbc(
        url=postgres_url,
        table="attendance_stage",
        mode="overwrite",
        properties=postgres_properties
    )

    print(" Stage table loaded successfully")

    # ---------------------------
    # Data Warehouse Transformations
    # ---------------------------
    df_dw = df_stage.withColumn(
        "Temp_Alert",
        when(col("Temperature_Status") == "High", "YES").otherwise("NO")
    )

    # ---------------------------
    # Write Data Warehouse Table
    # ---------------------------
    df_dw.write.jdbc(
        url=postgres_url,
        table="attendance_dw",
        mode="overwrite",
        properties=postgres_properties
    )

    print(" Data Warehouse table loaded successfully")

    # ---------------------------
    # Stop Spark session
    # ---------------------------
    spark.stop()
    print(" ETL pipeline completed successfully")


if __name__ == "__main__":
    run_etl_pipeline()
