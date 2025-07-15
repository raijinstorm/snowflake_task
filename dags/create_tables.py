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
    
    create_star_schema_tables = SQLExecuteQueryOperator(
        task_id = "create_star_schema_tables",
        conn_id = "snowflake_default",
        sql = """
            USE SCHEMA mart_stage;

            CREATE TABLE IF NOT EXISTS dim_passenger (
            passenger_sk    NUMBER(38,0)  IDENTITY PRIMARY KEY,
            passenger_id    VARCHAR       NOT NULL, 
            first_name      VARCHAR       NOT NULL,
            last_name       VARCHAR       NOT NULL,
            gender          VARCHAR       NOT NULL,
            age             NUMBER        NOT NULL,
            nationality     VARCHAR       NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_flight (
            flight_sk       NUMBER(38,0)  IDENTITY PRIMARY KEY,
            flight_id       VARCHAR       NOT NULL,
            pilot_name      VARCHAR       NOT NULL,
            flight_status   VARCHAR       NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_airport (
            airport_sk       NUMBER(38,0) IDENTITY PRIMARY KEY,
            airport_code     VARCHAR      NOT NULL,
            airport_name     VARCHAR      NOT NULL,
            country_code     VARCHAR      NOT NULL,
            country_name     VARCHAR      NOT NULL,
            continent_code   VARCHAR      NOT NULL,
            continent_name   VARCHAR      NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_date (
            date_sk        NUMBER(38,0)  IDENTITY PRIMARY KEY,
            date           DATE          NOT NULL,
            day_of_week    NUMBER        NOT NULL,
            month          NUMBER        NOT NULL,
            year           NUMBER        NOT NULL,
            quarter        NUMBER        NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fact_passenger_flight (
            id                VARCHAR    PRIMARY KEY,  
            passenger_sk      NUMBER(38,0) REFERENCES dim_passenger(passenger_sk),
            flight_sk         NUMBER(38,0) REFERENCES dim_flight(flight_sk),
            airport_sk        NUMBER(38,0) REFERENCES dim_airport(airport_sk),
            date_sk           NUMBER(38,0) REFERENCES dim_date(date_sk),
            passenger_status  VARCHAR,
            ticket_type       VARCHAR
            );
        """
    )
    
    create_audit_table = SQLExecuteQueryOperator(
        task_id = "create_audit_table",
        conn_id = "snowflake_default",
        sql = """
            CREATE TABLE IF NOT EXISTS mart_stage.etl_audit_log (
                log_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
                task_id           VARCHAR(255) NOT NULL,
                target_table      VARCHAR(255) NOT NULL,
                operation_type    VARCHAR(50) NOT NULL, 
                rows_affected     NUMBER(38,0) NOT NULL,
                query_id          VARCHAR(255),
                log_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """
    )
    
    test_connection >> create_raw_stage_table >> create_raw_table_stream
    test_connection >> create_core_stage_table >> create_core_table_stream
    test_connection >> create_star_schema_tables
    test_connection >> create_audit_table