from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.utils.edgemodifier import Label
from airflow import Dataset
from datetime import timedelta
import pandas as pd
import os
import logging

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
TEMP_DATA_DIR = os.getenv("TEMP_DATA_DIR", "/opt/airflow/temp")
REVIEWS_CSV_NAME = os.getenv("REVIEWS_CSV_NAME", "tiktok_google_play_reviews")

raw_csv_path = os.path.join(DATA_DIR, f"{REVIEWS_CSV_NAME}.csv")
temp_csv_path_1 = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_1.csv")
temp_csv_path_2 = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_2.csv")
temp_csv_path_3 = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_3.csv")
final_csv_path = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_final.csv")

FINAL_CSV_DATASET = Dataset(f"file://{final_csv_path}")

EXPTECTED_CSV_COLUMNS = ["reviewId","userName","userImage","content","score","thumbsUpCount","reviewCreatedVersion","at","replyContent","repliedAt"]

default_args = {
    "owner":"???",
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

dag = DAG(
    "transform_csv",
    default_args = default_args,
    schedule="@daily",
    catchup=False
)

wait_for_file = FileSensor(
    task_id = "wait_for_file",
    filepath = raw_csv_path,
    poke_interval = 30,
    timeout = 600,
    fs_conn_id = "fs_default", 
    dag = dag
)


def task_branch(**kwargs):
        df = pd.read_csv(raw_csv_path)
        if len(df) == 0:
            logging.warning("File was completely empty. You can see logs for empty files in logs/file_empty.log")
            return "log_empty_file"
        return "transform_df.filter_rows"
        

branch = BranchPythonOperator(
    task_id = "branch_task",
    python_callable = task_branch,
    dag = dag
)

log_empty_file = BashOperator(
    task_id = "log_empty_file",
    bash_command = 'echo "$(date) - The csv file was empty" >> /opt/airflow/logs/file_empty.log',
    dag = dag
)

def filter_valid_rows():
    df = pd.read_csv(raw_csv_path, header=0)
    if list(df.columns) != EXPTECTED_CSV_COLUMNS:
        raise ValueError("Csv columns mismatch")
    
    invalid_numeric_rows = df[['score', 'thumbsUpCount']].apply(pd.to_numeric, errors='coerce').isna().any(axis=1)
    invalid_datetime_rows = pd.to_datetime(df["at"], errors='coerce').isna()
    
    final_invalid_rows = invalid_numeric_rows | invalid_datetime_rows
    
    invalid_count = final_invalid_rows.sum()
    valid_count = len(df) - invalid_count
    logging.info(f"Valid rows: {valid_count}, invalid rows: {invalid_count}")
    
    df = df[~final_invalid_rows]
    
    os.makedirs("/opt/airflow/temp", exist_ok=True)
    df.to_csv(temp_csv_path_1, index = False)
    logging.info("Rows were filtered")
    
def clean_null_values():
    df = pd.read_csv(temp_csv_path_1, header=0)
    df = df.fillna("-")
    df = df.replace("null", "-")
    df.to_csv(temp_csv_path_2, index = False)
    logging.info("Null values were cleaned")
    

def sort_by_dates():
    df = pd.read_csv(temp_csv_path_2, header=0)
    # "at" = "created_date"
    df = df.sort_values("at", ascending= False)
    df.to_csv(temp_csv_path_3, index = False)
    logging.info("Data was sorted by dates")
    

def clean_content_column():
    df = pd.read_csv(temp_csv_path_3, header=0)
    df["content"] = df["content"].str.replace(r"[^a-zA-z0-9!,.?: ]", " ", regex = True)
    df.to_csv(final_csv_path, index = False)
    logging.info("Content column was cleaned")
    
@task_group(group_id="transform_df", dag = dag)
def transform_df():
    filter_rows = PythonOperator(
    task_id = "filter_rows",
    python_callable = filter_valid_rows,
    dag = dag
    )
    
    clean_nulls = PythonOperator(
    task_id = "clean_nulls",
    python_callable = clean_null_values,
    dag = dag
    )
    
    sort_df = PythonOperator(
    task_id = "sort_df",
    python_callable = sort_by_dates,
    dag = dag
    )
    
    clean_content = PythonOperator(
    task_id = "clean_content",
    python_callable = clean_content_column,
    outlets = [FINAL_CSV_DATASET],
    dag = dag
    )
    
    filter_rows >> clean_nulls >> sort_df >> clean_content
    
transform_group = transform_df()
    
wait_for_file >> branch >> Label("The file is empty") >>log_empty_file 
branch >> Label("The file is NON-empty") >> transform_group