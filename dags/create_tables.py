from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

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
    
    create_staging_table_for_stream = SQLExecuteQueryOperator(
        task_id="create_staging_table_for_stream",
        conn_id="snowflake_default",
        sql="""
            CREATE TABLE IF NOT EXISTS core_stage.temp_stream_table (
                index_col NUMBER(38,0),
                passenger_id VARCHAR(16777216),
                first_name VARCHAR(16777216),
                last_name VARCHAR(16777216),
                gender VARCHAR(16777216),
                age NUMBER(38,0),
                nationality VARCHAR(16777216),
                airport_name VARCHAR(16777216),
                airport_country_code VARCHAR(16777216),
                country_name VARCHAR(16777216),
                airport_continent VARCHAR(16777216),
                continents VARCHAR(16777216),
                departure_date DATE,
                arrival_airport VARCHAR(16777216),
                pilot_name VARCHAR(16777216),
                flight_status VARCHAR(16777216),
                ticket_type VARCHAR(16777216),
                passenger_status VARCHAR(16777216),
                metadata$action VARCHAR(16777216),
                metadata$isupdate BOOLEAN,
                metadata$row_id VARCHAR(16777216)
            );
        """
    )
    
    with TaskGroup(group_id="create_star_schema_tables") as create_star_schema_tables_group:
        create_dim_passenger = SQLExecuteQueryOperator(
            task_id="create_dim_passenger",
            conn_id="snowflake_default",
            sql="""
                CREATE TABLE IF NOT EXISTS mart_stage.dim_passenger (
                    passenger_sk    NUMBER(38,0)  IDENTITY PRIMARY KEY,
                    passenger_id    VARCHAR       NOT NULL, 
                    first_name      VARCHAR       NOT NULL,
                    last_name       VARCHAR       NOT NULL,
                    gender          VARCHAR       NOT NULL,
                    age             NUMBER        NOT NULL,
                    nationality     VARCHAR       NOT NULL
                );
            """
        )

        create_dim_flight = SQLExecuteQueryOperator(
            task_id="create_dim_flight",
            conn_id="snowflake_default",
            sql="""
                CREATE TABLE IF NOT EXISTS mart_stage.dim_flight (
                    flight_sk       NUMBER(38,0)  IDENTITY PRIMARY KEY,
                    flight_id       VARCHAR       NOT NULL,
                    pilot_name      VARCHAR       NOT NULL,
                    flight_status   VARCHAR       NOT NULL
                );
            """
        )

        create_dim_airport = SQLExecuteQueryOperator(
            task_id="create_dim_airport",
            conn_id="snowflake_default",
            sql="""
                CREATE TABLE IF NOT EXISTS mart_stage.dim_airport (
                    airport_sk      NUMBER(38,0) IDENTITY PRIMARY KEY,
                    airport_code    VARCHAR      NOT NULL,
                    airport_name    VARCHAR      NOT NULL,
                    country_code    VARCHAR      NOT NULL,
                    country_name    VARCHAR      NOT NULL,
                    continent_code  VARCHAR      NOT NULL,
                    continent_name  VARCHAR      NOT NULL
                );
            """
        )

        create_dim_date = SQLExecuteQueryOperator(
            task_id="create_dim_date",
            conn_id="snowflake_default",
            sql="""
                CREATE TABLE IF NOT EXISTS mart_stage.dim_date (
                    date_sk         NUMBER(38,0)  IDENTITY PRIMARY KEY,
                    date            DATE          NOT NULL,
                    day_of_week     NUMBER        NOT NULL,
                    month           NUMBER        NOT NULL,
                    year            NUMBER        NOT NULL,
                    quarter         NUMBER        NOT NULL
                );
            """
        )

        create_fact_passenger_flight = SQLExecuteQueryOperator(
            task_id="create_fact_passenger_flight",
            conn_id="snowflake_default",
            sql="""
                CREATE TABLE IF NOT EXISTS mart_stage.fact_passenger_flight (
                    id              VARCHAR    PRIMARY KEY, 
                    passenger_sk    NUMBER(38,0) REFERENCES mart_stage.dim_passenger(passenger_sk),
                    flight_sk       NUMBER(38,0) REFERENCES mart_stage.dim_flight(flight_sk),
                    airport_sk      NUMBER(38,0) REFERENCES mart_stage.dim_airport(airport_sk),
                    date_sk         NUMBER(38,0) REFERENCES mart_stage.dim_date(date_sk),
                    passenger_status VARCHAR,
                    ticket_type      VARCHAR
                );
            """
        )

        [create_dim_passenger, create_dim_flight, create_dim_airport, create_dim_date] >> create_fact_passenger_flight
    
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
    test_connection >> create_star_schema_tables_group
    test_connection >> create_audit_table