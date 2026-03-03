from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ---------------------------------------------------------
# 1. Path Configuration
# ---------------------------------------------------------
SCRIPT_PATH = "/opt/airflow/scripts"

default_args = {
    'owner': 'hemant',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 21), #
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'retail_medallion_pipeline',
    default_args=default_args,
    description='Automated Medallion Architecture Pipeline: Raw to Gold',
    # schedule_interval=timedelta(minutes=5),  # Har 5 minute ke liye
    schedule_interval="*/10 * * * *",        # Har 10 minute ke liye Cron format (Zyaada stable)
    catchup=False,                           # Pichle runs skip karne ke liye zaroori
    max_active_runs=1,                       # Ek waqt mein ek hi pipeline chalegi load kam karne ke liye
    tags=['pyspark', 'medallion']
) as dag:

    # Task 1: Generate Raw Data
    generate_data = BashOperator(
        task_id='generate_data',
        bash_command=f'python3 {SCRIPT_PATH}/generate_raw_data.py',
        cwd=SCRIPT_PATH 
    )

    # Task 2: Bronze Layer
    run_bronze = BashOperator(
        task_id='process_bronze_layer',
        bash_command=f'python3 {SCRIPT_PATH}/retail_bronze.py',
        cwd=SCRIPT_PATH
    )

    # Task 3: Silver Layer
    run_silver = BashOperator(
        task_id='process_silver_layer',
        bash_command=f'python3 {SCRIPT_PATH}/retail_silver.py',
        cwd=SCRIPT_PATH
    )

    # Task 4: Gold Layer
    run_gold = BashOperator(
        task_id='process_gold_layer',
        bash_command=f'python3 {SCRIPT_PATH}/retail_gold.py',
        cwd=SCRIPT_PATH
    )

    # Pipeline Flow
    generate_data >> run_bronze >> run_silver >> run_gold