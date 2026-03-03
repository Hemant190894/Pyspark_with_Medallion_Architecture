# Retail Data Medallion Architecture Pipeline 🚀

Project ko 3 main layers mein divide kiya gaya hai (Medallion Architecture):
1. **Bronze (Ingestion)**: Raw Parquet data ko MinIO se read karke archive kiya jata hai.
2. **Silver (Cleaned)**: Data types fix (customer_age & quantity to LongType) aur null handling.
3. **Gold (Analytics)**: 9M+ records ka aggregation (Daily Sales & City Revenue).

### Tech Stack
* **Orchestration**: Apache Airflow 2.7.1
* **Processing**: PySpark 3.5.1
* **Storage**: MinIO (S3 Compatible)