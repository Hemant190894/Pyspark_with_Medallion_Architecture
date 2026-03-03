import datetime
import pandas as pd
import numpy as np
import os
import configparser
import time
import boto3
from botocore.client import Config
from concurrent.futures import ProcessPoolExecutor

# ---------------------------------------------------------
# 1. Path Setup
# ---------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.ini")

def generate_data_chunk(chunk_size):
    """
    Core data generation logic for parallel workers.
    """
    np.random.seed() 
    
    categories = {"Fashion": (20, 500), "Grocery": (1, 50), "Furniture": (50, 1500), "Sports": (10, 800)}
    cities = [("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"), ("Phoenix", "AZ")]
    payment_types = ["Card", "UPI", "COD", "Crypto", None]
    genders = ["M", "F", "Male", "Female", None]
    order_statuses = ["Delivered", "Cancelled", "Returned"]
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2026, 1, 1)

    records = []
    for _ in range(chunk_size):
        o_date = start_date + datetime.timedelta(days=np.random.randint(0, (end_date - start_date).days))
        category = np.random.choice(list(categories.keys()))
        price_min, price_max = categories[category]
        city, state = cities[np.random.randint(0, len(cities))]
        
        records.append([
            np.random.randint(1, 100_000_000),
            o_date.strftime("%Y-%m-%d"),
            (o_date + datetime.timedelta(days=np.random.randint(-3, 10))).strftime("%Y-%m-%d"),
            f"CUST{np.random.randint(1, 200_000)}",
            np.random.randint(10, 100),
            np.random.choice(genders),
            f"PROD{np.random.randint(1, 50_000)}",
            category,
            np.random.randint(1, 10),
            round(np.random.uniform(price_min, price_max), 2),
            round(np.random.uniform(0, 50), 2),
            city, state,
            np.random.choice(payment_types),
            np.random.choice(order_statuses),
            datetime.datetime.now().strftime("%Y-%m-%d")
        ])
    return records

def upload_to_minio(file_path, minio_config, object_name):
    """
    Uploads a file to MinIO.
    """
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=minio_config['endpoint'],
            aws_access_key_id=minio_config['access_key'],
            aws_secret_access_key=minio_config['secret_key'],
            config=Config(signature_version='s3v4')
        )
        bucket = minio_config['bucket_name']
        s3.upload_file(file_path, bucket, object_name)
        return True
    except Exception as e:
        print(f"❌ MinIO Upload Failed for {object_name}: {e}")
        return False

if __name__ == "__main__":
    start_time = time.time()
    
    # 2. Load Config
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    num_records = int(config["DATA"]["num_records"]) # 10,000,000
    chunk_size = int(config["DATA"]["chunk_size"])   # 100,000
    batch_size = 5  # Ek baar mein sirf 5 files parallel banengi RAM bachane ke liye

    minio_settings = {
        'endpoint': config["MINIO"]["endpoint"],
        'access_key': config["MINIO"]["access_key"],
        'secret_key': config["MINIO"]["secret_key"],
        'bucket_name': config["MINIO"]["bucket_name"]
    }

    columns = ["transaction_id", "order_date", "ship_date", "customer_id", "customer_age",
               "gender", "product_id", "product_category", "quantity", "unit_price",
               "discount_pct", "city", "state", "payment_type", "order_status", "ingestion_date"]

    raw_data_base_path = config["DATA"]["raw_data_path"]
    output_dir = os.path.dirname(raw_data_base_path)
    run_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    print(f"🚀 Starting generation: {num_records} records in 100 files...")

    num_chunks = num_records // chunk_size

    try:
        # Loop over batches
        for i in range(0, num_chunks, batch_size):
            current_batch_count = min(batch_size, num_chunks - i)
            print(f"📦 Batch {(i//batch_size)+1}: Generating files {i+1} to {i+current_batch_count}...")

            with ProcessPoolExecutor(max_workers=4) as executor:
                # Parallel generation of current batch
                batch_results = list(executor.map(generate_data_chunk, [chunk_size] * current_batch_count))
                
                for idx, records in enumerate(batch_results, start=i+1):
                    df_chunk = pd.DataFrame(records, columns=columns)
                    
                    filename = f"retail_chunk_{idx}.parquet"
                    local_path = os.path.join(output_dir, filename)
                    df_chunk.to_parquet(local_path, index=False)
                    
                    minio_path = f"raw/{run_timestamp}/{filename}"
                    if upload_to_minio(local_path, minio_settings, minio_path):
                        os.remove(local_path)
                    
                    # Clean up to free memory
                    del records
                    del df_chunk

    except Exception as e:
        print(f"💥 Critical Error: {e}")
        raise

    print(f"🏁 Total Time: {time.time() - start_time:.2f} seconds.")