# Retail Data Medallion Architecture Pipeline 🚀
Project is divided into 3 main layers (Medallion Architecture):

## 1. Bronze (Ingestion): 
Raw Parquet data is read from MinIO and archived.
SCHEMA ENFORCEMENT: Standardized schema is enforced, specifically ensuring quantity and customer_age use standard LongType. Merged standard partitioned Parquet inputs into a unified output.

## 2. Silver (Cleaned): 
Data types fix (customer_age & quantity to LongType) and null handling.
QUARANTINE BRANCH: Records where quantity <= 0 or customer_age is invalid (e.g., < 15) are separately filtered. This invalid data is quarantined into standard consolidated Bad Records CSV for immediate standard analysis. Clean data standardized gender values and unique transactions.

## 3. Gold (Analytics): 
Aggregation of 9M+ records (Daily Sales & City Revenue).

### Tech Stack
Orchestration: Apache Airflow 2.7.1

Processing: PySpark 3.5.1

Storage: MinIO (S3 Compatible Data Lake)

### Architecture Diagram
Pipeline orchestration (Triggered every 10 minutes) and data flow can be visualized in the standardized diagram below:

![Retail Data Medallion Architecture](https://github.com/Hemant190894/Pyspark_with_Medallion_Architecture/blob/main/assets/Pyspark_with_Medallion_Architecture.png)

### Advanced Optimization Callout
The Gold layer processes 9 Million records. Caching standard large datasets can crash standard standard standard standard standard limited standard standard Docker RAM (User Logs Memory OOM). Therefore, this script uses specific optimizations:

NO CACHE PRESSURE: Data is purely processed without caching. Arrow Optimization enabled.

HIGH-PERFORMANCE SHUFFLE: Spark shuffle.partitions is increased from standard 8 to standard 50. This breaks standard complex aggregation standard standard smaller tasks, keeping standard RAM consumption safe.

### System Configuration
Solution is containarized and highly optimized for standard standard standard limits:

Docker Resources: 15 GB RAM, 10 CPUs (Allocated through Docker Desktop settings).

operating timezone: Logs standardized to Asia/Kolkata (IST).
