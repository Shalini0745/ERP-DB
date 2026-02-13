import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    when,
    lit,
    current_date
)


def load_dw_data():

    # ---------------------------
    # JDBC & Database Configuration
    # ---------------------------
    JDBC_JAR_PATH = "file:///C:/project/drivers/postgresql-42.7.8.jar"

    SOURCE_DB_NAME = "stage_db"
    SOURCE_TABLE = "attendance_stage"
    SOURCE_URL = f"jdbc:postgresql://localhost:5432/{SOURCE_DB_NAME}"

    TARGET_DB_NAME = "dw_db"
    TARGET_TABLE = "attendance_student"
    TARGET_URL = f"jdbc:postgresql://localhost:5432/{TARGET_DB_NAME}"

    postgres_props = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # ---------------------------
    # Windows Spark Environment Setup
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
        .appName("DWDataLoader")
        .config("spark.jars", JDBC_JAR_PATH)
        .getOrCreate()
    )

    try:
        print(f" Reading data from {SOURCE_DB_NAME}.{SOURCE_TABLE}")

        # ---------------------------
        # Read Stage Table
        # ---------------------------
        df_stage = spark.read.jdbc(
            url=SOURCE_URL,
            table=SOURCE_TABLE,
            properties=postgres_props
        )

        print(" Stage data loaded successfully")

        # ---------------------------
        # Apply Data Warehouse Transformations
        # ---------------------------
        df_final = (
            df_stage
            .withColumn("id", monotonically_increasing_id().cast("bigint"))
            .select(
                col("id"),

                # Placeholder surrogate key (dimension not yet implemented)
                lit(None).cast("bigint").alias("id_student"),

                col("student_id").cast("string").alias("student_id"),
                col("attendance_group"),
                col("attendance_date").cast("date"),
                col("day_of_week"),
                col("check_in_time"),
                col("skin_surface_temp"),
                col("temp_status"),
                col("card_swiping_type"),
                col("verification_method"),
                col("attendance_check_point"),
                col("custom_name"),
                col("data_source"),

                # Clean correction_type_id before casting
                when(col("correction_type_id") == "-", lit(None))
                .otherwise(col("correction_type_id"))
                .cast("int")
                .alias("correction_type_id"),

                col("note"),

                # Audit / ETL metadata columns
                lit(101).cast("bigint").alias("load_batch_id"),
                current_date().alias("process_date"),
                current_date().alias("load_dt"),
                current_date().alias("updt_dt")
            )
        )

        final_count = df_final.count()
        print(f" Records prepared for DW: {final_count}")

        # ---------------------------
        # Write Data Warehouse Table
        # ---------------------------
        df_final.write \
            .mode("overwrite") \
            .jdbc(url=TARGET_URL, table=TARGET_TABLE, properties=postgres_props)

        print(f" SUCCESS: Data loaded into {TARGET_DB_NAME}.{TARGET_TABLE}")

    except Exception as e:
        print(f" ERROR: DW Load Failed → {str(e)}")

    finally:
        spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    load_dw_data()
