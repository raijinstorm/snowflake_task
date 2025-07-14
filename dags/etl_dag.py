from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import os
import logging

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
CSV_FILE_NAME = os.getenv("CSV_FILE_NAME", "airline_data.csv") 
#testing
#----
CSV_FILE_NAME = "airline_data_initial_load.csv"
#----
FULL_FILE_PATH = os.path.join(DATA_DIR, CSV_FILE_NAME)

#User's default internal stage
SNOWFLAKE_INTERNAL_STAGE = "@~"


with DAG(
    "etl_snowflake",
) as dag:
    test_connection = SQLExecuteQueryOperator(
        task_id = "test_connection",
        conn_id = "snowflake_default",
        sql= "SELECT 1"
    )
    
    #PUT file to stage
    put_into_local_stage = SQLExecuteQueryOperator(
        task_id = "put_into_local_stage",
        conn_id = "snowflake_default",
        sql = f"""
            PUT 'file://{FULL_FILE_PATH}' {SNOWFLAKE_INTERNAL_STAGE}/airline_data.csv
            AUTO_COMPRESS = FALSE;
        """
    )
    
    # COPY from stage to table
    copy_data_to_raw_table = SQLExecuteQueryOperator(
        task_id = "copy_data_to_raw_table",
        conn_id = "snowflake_default",
        sql = f"""
            COPY INTO raw_stage.passengers_wide
            FROM {SNOWFLAKE_INTERNAL_STAGE}/airline_data.csv
            FILE_FORMAT = (
                TYPE = CSV,
                SKIP_HEADER = 1,
                NULL_IF = ('NULL', '')
            )
            ON_ERROR = 'CONTINUE';
        """
    )
    
    
    # #REMOVE file from stage
    # remove_file_from_stage = SQLExecuteQueryOperator(
    #     task_id = "remove_file_from_stage",
    #     conn_id = "snowflake_default",
    #     sql = f"REMOVE {SNOWFLAKE_INTERNAL_STAGE}/airline_data.csv;"
    # )
    
    test_connection >> put_into_local_stage >> copy_data_to_raw_table 


