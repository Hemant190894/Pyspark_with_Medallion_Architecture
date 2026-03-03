from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round as spark_round
import logging

# ------------------------------------
# 1. Logging Setup
# ------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldLayerJob")

# ------------------------------------
# 2. Spark Session (OPTIMIZED FOR 9M RECORDS)
# ------------------------------------
spark = (SparkSession.builder
    .appName("RetailSales-SilverToGold")
    .config("spark.jars", "/home/airflow/spark_jars/hadoop-aws-3.3.4.jar,/home/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # --- MEMORY & PERFORMANCE FIXES ---
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    # FIX: Partitions ko 8 se badha kar 50 kijiye taaki chunks chhote ho jayein
    .config("spark.sql.shuffle.partitions", "50") 
    # FIX: Memory management for heavy aggregations
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.2")
    # FIX: Enable Spill to disk taaki RAM full hone par crash na ho
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate())

silver_df = None 

try:
    # 3. Read Silver Layer
    logger.info("Reading Cleaned Data from Silver Layer...")
    silver_path = "s3a://warehouse/silver/retail_sales_clean"
    silver_df = spark.read.parquet(silver_path)
    
    # 4. Feature Engineering
    silver_df = silver_df.withColumn(
        "total_amount",
        spark_round(col("quantity") * col("unit_price") * (1 - col("discount_pct") / 100), 2)
    )

    # Note: Cache ko hata diya hai kyunki 9M records RAM full kar rahe hain. 
    # Spark Plan itna optimized hai ki wo bina cache ke bhi fast chalega.

    # 5. Aggregations & Writing to Gold
    
    # 1️⃣ Daily Sales Metrics
    logger.info("Generating Daily Sales Metrics...")
    daily_sales = (silver_df.groupBy("order_date").agg(
        spark_round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_orders"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value")
    ))
    daily_sales.write.mode("overwrite").parquet("s3a://warehouse/gold/daily_sales_metrics")

    # 2️⃣ Product Performance Metrics
    logger.info("Generating Product Performance Metrics...")
    product_perf = (silver_df.groupBy("product_category").agg(
        spark_round(sum("total_amount"), 2).alias("category_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("transaction_id").alias("order_count")
    ))
    product_perf.write.mode("overwrite").parquet("s3a://warehouse/gold/product_performance")

    # 3️⃣ City Revenue Metrics
    logger.info("Generating City Revenue Metrics...")
    city_rev = (silver_df.groupBy("city", "state").agg(
        spark_round(sum("total_amount"), 2).alias("city_revenue"),
        count("transaction_id").alias("order_count"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value")
    ))
    city_rev.write.mode("overwrite").parquet("s3a://warehouse/gold/city_revenue")

    logger.info("✅ Gold layer completed successfully!")

except Exception as e:
    logger.error(f"❌ Error in Gold Layer: {str(e)}")
    raise 
finally:
    spark.stop()