from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType
import logging

# 1. Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeLayerJob")

# 2. Schema Definition (Using LongType for IDs)
# 2. Schema Definition (Updated for LongType columns)
raw_schema = StructType([
    StructField("transaction_id", LongType(), True), 
    StructField("order_date", StringType(), True),
    StructField("ship_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_age", LongType(), True),    # Pehle fix kiya tha
    StructField("gender", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", LongType(), True),        # <--- AB YAHAN FIX KAREIN (IntegerType se LongType)
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("ingestion_date", StringType(), True)
])

# 3. Spark Session with Critical Fixes for Cast Error
# 3. Spark Session with Correct Password and Region
spark = (SparkSession.builder
    .appName("RetailSales-RawToBronze")
    .config("spark.jars", "/home/airflow/spark_jars/hadoop-aws-3.3.4.jar,/home/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123") # Aapka password update kar diya gaya hai
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.region", "us-east-1") # Ye line SdkClientException ko theek karegi
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())

try:
    # 4. Reading Data
    raw_path = "s3a://warehouse/raw/**" 
    logger.info(f"Reading Raw Partitioned Parquet from: {raw_path}")
    
    # Schema aur Merge option dono use karein
    bronze_df = spark.read.schema(raw_schema).option("mergeSchema", "true").parquet(raw_path)
    
    total_count = bronze_df.count()
    logger.info(f"✅ Success! Read {total_count} records.")

    # 5. Writing to Bronze
    output_path = "s3a://warehouse/bronze/retail_sales_bronze"
    (bronze_df.write
        .mode("overwrite")
        .parquet(output_path))

    logger.info("✅ Bronze layer finished successfully!")

except Exception as e:
    logger.error(f"❌ Bronze process failed: {str(e)}")
    raise
finally:
    spark.stop()