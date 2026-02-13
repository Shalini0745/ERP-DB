import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType


def load_stage_data():

    # ---------------------------
    # File & JDBC Configuration
    # ---------------------------
    CSV_FILE_PATH = "C:/project/data/Attendance.csv"
    JDBC_JAR_PATH = "file:///C:/project/drivers/postgresql-42.7.8.jar"
    TARGET_TABLE = "attendance_stage"

    # ---------------------------
    # Define CSV Schema
    # ---------------------------
    COLUMNS = [
        "First Name", "Last Name", "ID", "Department", "Attendance Group",
        "Date", "Week", "Check-In Time", "Skin-Surface Temperature",
        "Temperature Status", "Card Swiping Type", "Verification Method",
        "Attendance Check Point", "Custom Name", "Data Source",
        "Correction Type", "Note"
    ]

    csv_schema = StructType([StructField(c, StringType(), True) for c in COLUMNS])
    csv_schema = csv_schema.add(
        StructField("extra_trailing_comma_field", StringType(), True)
    )

    # ---------------------------
    # Windows Spark Environment
    # ---------------------------
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"
    os.environ['JAVA_HOME'] = "C:\\Program Files\\Java\\jdk-17"
    os.environ['SPARK_HOME'] = "C:\\Spark\\Spark3"

    # ---------------------------
    # Create Spark Session
    # ---------------------------
    spark = (
        SparkSession.builder
        .appName("RobustStageDataLoader")
        .config("spark.jars", JDBC_JAR_PATH)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    try:
        print(" Spark session started")

        # ---------------------------
        # Read Raw File via RDD
        # ---------------------------
        rdd_raw = spark.sparkContext.textFile(CSV_FILE_PATH)

        # Skip metadata & header rows
        rdd_data = (
            rdd_raw.zipWithIndex()
            .filter(lambda x: x[1] >= 5)
            .map(lambda x: x[0])
        )

        # ---------------------------
        # Load into DataFrame
        # ---------------------------
        df_raw = (
            spark.read
            .schema(csv_schema)
            .option("header", "false")
            .csv(rdd_data)
        )

        # Remove repeated headers if present
        df_filtered = df_raw.filter(col("ID") != "ID")

        # ---------------------------
        # Transformations & Casting
        # ---------------------------
        df_final = (
            df_filtered.select(
                col("First Name").alias("first_name"),
                col("Last Name").alias("last_name"),
                col("ID").alias("student_id"),
                col("Department").alias("department"),
                col("Attendance Group").alias("attendance_group"),

                to_date(col("Date"), "yyyy-MM-dd").alias("attendance_date"),
                col("Week").alias("day_of_week"),

                to_timestamp(
                    concat_ws(" ", col("Date"), col("Check-In Time")),
                    "yyyy-MM-dd HH:mm"
                ).alias("check_in_time"),

                col("Skin-Surface Temperature").alias("skin_surface_temp"),
                col("Temperature Status").alias("temp_status"),
                col("Card Swiping Type").alias("card_swiping_type"),
                col("Verification Method").alias("verification_method"),
                col("Attendance Check Point").alias("attendance_check_point"),
                col("Custom Name").alias("custom_name"),
                col("Data Source").alias("data_source"),
                col("Correction Type").alias("correction_type_id"),
                col("Note").alias("note")
            )
            .filter(col("student_id").isNotNull())
        )

        record_count = df_final.count()
        print(f" Processed Records: {record_count}")

        # ---------------------------
        # PostgreSQL Write
        # ---------------------------
        postgres_props = {
            "user": "postgres",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }

        url = "jdbc:postgresql://localhost:5432/stage_db"

        df_final.write \
            .mode("overwrite") \
            .jdbc(url=url, table=TARGET_TABLE, properties=postgres_props)

        print(f"Data written to PostgreSQL → stage_db.{TARGET_TABLE}")

    except Exception as e:
        print(f"ERROR: {str(e)}")

    finally:
        spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    load_stage_data()
