from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType
import logging

# 1. Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverLayerJob")

# 2. Spark Session (Fixed for 20M records & LongType)
spark = (SparkSession.builder
    .appName("RetailSales-BronzeToSilver")
    .config("spark.jars", "/home/airflow/spark_jars/hadoop-aws-3.3.4.jar,/home/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # Casting fix for large data
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate())

try:
    # 3. Read Bronze Data
    logger.info("Reading data from Bronze Layer...")
    bronze_path = "s3a://warehouse/bronze/retail_sales_bronze"
    df = spark.read.parquet(bronze_path)
    
    initial_count = df.count()
    logger.info(f"Bronze Records Loaded: {initial_count}")

    # 4. Data Cleaning & Quarantine Logic
    # Bad Records Condition
    bad_records_condition = (
        (col("quantity") <= 0) | 
        (col("customer_age") < 15) | 
        (col("customer_age") > 100) |
        (col("unit_price") <= 0)
    )

    # Split Data: Silver (Good) vs Quarantine (Bad)
    bad_df = df.filter(bad_records_condition)
    silver_df = df.filter(~bad_records_condition)

    # Standardize Good Data
    silver_df = silver_df.withColumn(
        "gender",
        when(upper(trim(col("gender"))).isin("MALE", "M"), "M")
        .when(upper(trim(col("gender"))).isin("FEMALE", "F"), "F")
        .otherwise("Unknown")
    ).dropDuplicates(["transaction_id"])
    
    logger.info(f"Cleaning Complete. Valid: {silver_df.count()}, Bad: {bad_df.count()}")

    # 5. Write Silver Data (Good Records)
    silver_output = "s3a://warehouse/silver/retail_sales_clean"
    (silver_df.write
        .mode("overwrite")
        .parquet(silver_output))

    # 6. Write Quarantine Data (Bad Records) - WAPAS CHALU KAR DIYA
    bad_output = "s3a://warehouse/quarantine/bad_records"
    logger.info(f"Writing Bad Records to: {bad_output}")
    
    # Coalesce(1) use kiya hai taaki ek hi CSV file bane dekhne ke liye
    (bad_df.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(bad_output))

    logger.info("✅ Silver Layer & Quarantine process finished successfully!")

except Exception as e:
    logger.error(f"❌ Silver Layer Failed: {str(e)}")
    raise
finally:
    spark.stop()