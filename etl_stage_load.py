import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col


def load_stage_data():

    # Configure Hadoop environment
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"

    # Path to PostgreSQL JDBC driver
    postgres_jar = "C:\\project\\drivers\\postgresql-42.7.8.jar"

    # Create Spark session with JDBC support
    spark = (
        SparkSession.builder
        .appName("LoadAttendanceStage")
        .config("spark.jars", postgres_jar)
        .getOrCreate()
    )

    # Define schema for attendance dataset
    schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("attendance_group", StringType(), True),
        StructField("attendance_date", StringType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("check_in_time", StringType(), True),
        StructField("skin_surface_temp", StringType(), True),
        StructField("temp_status", StringType(), True),
        StructField("card_swiping_type", StringType(), True),
        StructField("verification_method", StringType(), True),
        StructField("attendance_check_point", StringType(), True),
        StructField("custom_name", StringType(), True),
        StructField("data_source", StringType(), True),
        StructField("correction_type", StringType(), True),
        StructField("note", StringType(), True),
    ])

    # Load CSV data into Spark DataFrame
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load("C:\\project\\data\\attendance.csv")
    )

    # Apply datatype conversions
    df = df.withColumn("attendance_date", col("attendance_date").cast("date"))
    df = df.withColumn("check_in_time", col("check_in_time").cast(TimestampType()))

    # Write transformed data to PostgreSQL staging table
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/stage_db") \
        .option("dbtable", "attendance_project") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Stage Load Completed Successfully")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    load_stage_data()
