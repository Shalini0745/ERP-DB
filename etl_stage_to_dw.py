import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col


def create_spark_session():
    """
    Creates Spark session with PostgreSQL JDBC driver
    """

    postgres_jar = "drivers/postgresql-42.7.8.jar"

    spark = (
        SparkSession.builder
        .appName("LoadAttendanceStage")
        .config("spark.jars", postgres_jar)
        .getOrCreate()
    )

    return spark


def define_schema():
    """
    Defines schema for Attendance CSV
    """

    return StructType([
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


def load_csv_data(spark, schema):
    """
    Loads CSV from data folder
    """

    file_path = "data/attendance.csv"

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(file_path)
    )

    return df


def transform_data(df):
    """
    Performs datatype conversions
    """

    df = df.withColumn("attendance_date", col("attendance_date").cast("date"))
    df = df.withColumn("check_in_time", col("check_in_time").cast(TimestampType()))

    return df


def write_to_postgres(df):
    """
    Writes DataFrame to PostgreSQL Stage DB
    """

    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")

    if not pg_user or not pg_password:
        raise Exception(" PostgreSQL credentials not set in environment variables")

    (
        df.write.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/stage_db")
        .option("dbtable", "attendance_project")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def load_stage_data():
    """
    Main ETL Pipeline
    """

    print(" Starting Stage Load Pipeline...")

    spark = create_spark_session()
    schema = define_schema()

    df = load_csv_data(spark, schema)
    print(" CSV Loaded Successfully")

    df = transform_data(df)
    print("Data Transformations Applied")

    write_to_postgres(df)
    print(" Stage Load Completed Successfully")

    spark.stop()


if __name__ == "__main__":
    load_stage_data()
