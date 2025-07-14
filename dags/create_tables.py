from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

with DAG(
    "create_objects"
) as dag:
    test_connection = SQLExecuteQueryOperator(
        task_id = "test_connection",
        conn_id = "snowflake_default",
        sql= "SELECT 1" 
    )
    
    create_raw_stage_table = SQLExecuteQueryOperator(
        task_id = "create_raw_stage_table",
        conn_id = "snowflake_default",
        sql = """
            CREATE TABLE IF NOT EXISTS  raw_stage.passengers_wide (
                index_col TEXT,
                passenger_id TEXT,
                first_name TEXT, 
                last_name TEXT,
                gender TEXT,
                age TEXT,
                nationality TEXT,
                airport_name TEXT,
                airport_country_code TEXT,
                country_name TEXT,
                airport_continent TEXT,
                continents TEXT,
                departure_date TEXT,
                arrival_airport TEXT,
                pilot_name TEXT,
                flight_status TEXT,
                ticket_type TEXT,
                passenger_status TEXT
            )
        """
    )
    
    create_raw_table_stream = SQLExecuteQueryOperator(
        task_id = "create_raw_table_stream",
        conn_id = "snowflake_default",
        sql = """
            CREATE STREAM IF NOT EXISTS raw_stage.passengers_wide_stream
            ON TABLE raw_stage.passengers_wide
            APPEND_ONLY = TRUE
            SHOW_INITIAL_ROWS = FALSE;
        """
    )
    
    create_core_stage_table = SQLExecuteQueryOperator(
        task_id = "create_core_stage_table",
        conn_id = "snowflake_default",
        sql = """
            CREATE TABLE IF NOT EXISTS  core_stage.passengers_cleaned (
                index_col NUMBER(10, 0) PRIMARY KEY,
                passenger_id VARCHAR(6) NOT NULL,
                first_name VARCHAR(255) NOT NULL, 
                last_name VARCHAR(255) NOT NULL,
                gender VARCHAR(10) ,
                age NUMBER(4, 0),
                nationality VARCHAR(255),
                airport_name VARCHAR(255),
                airport_country_code VARCHAR(3) NOT NULL,
                country_name VARCHAR(255),
                airport_continent VARCHAR(3) NOT NULL ,
                continents VARCHAR(255),
                departure_date DATE NOT NULL,
                arrival_airport VARCHAR(3) NOT NULL,
                pilot_name VARCHAR(255),
                flight_status VARCHAR(10),
                ticket_type VARCHAR(10),
                passenger_status VARCHAR(10)
            )
        """
    )
    
    create_core_table_stream = SQLExecuteQueryOperator(
        task_id = "create_core_table_stream",
        conn_id = "snowflake_default",
        sql = """
            CREATE STREAM IF NOT EXISTS core_stage.passengers_cleaned_stream
            ON TABLE core_stage.passengers_cleaned
        """
    )
    
    test_connection >> create_raw_stage_table >> create_raw_table_stream
    test_connection >> create_core_stage_table >> create_core_table_stream