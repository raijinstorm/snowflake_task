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
FULL_FILE_PATH = os.path.join(DATA_DIR, CSV_FILE_NAME)

#User's default internal stage
SNOWFLAKE_INTERNAL_STAGE = "@~"

#
CSV_FILE_NAME="airline_data.csv"
#

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
            PUT 'file://{FULL_FILE_PATH}' {SNOWFLAKE_INTERNAL_STAGE}/{CSV_FILE_NAME}
            AUTO_COMPRESS = FALSE;
        """
    )
    
    # COPY from stage to table
    copy_data_to_raw_table = SQLExecuteQueryOperator(
        task_id = "copy_data_to_raw_table",
        conn_id = "snowflake_default",
        sql = f"""
            COPY INTO raw_stage.passengers_wide
            FROM {SNOWFLAKE_INTERNAL_STAGE}/{CSV_FILE_NAME}
            FILE_FORMAT = (
                TYPE = CSV,
                SKIP_HEADER = 1,
                NULL_IF = ('NULL', ''),
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
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
    
    insert_into_core_table = SQLExecuteQueryOperator(
        task_id = "insert_into_core_table",
        conn_id = "snowflake_default",
        sql = f"""
            MERGE INTO core_stage.passengers_cleaned AS target
            USING (
                SELECT
                    index_col::NUMBER(10,0) AS index_col, 
                    passenger_id::VARCHAR(6) AS passenger_id,
                    first_name::VARCHAR(255) AS first_name,
                    last_name::VARCHAR(255) AS last_name,
                    gender::VARCHAR(20) AS gender,
                    age::NUMBER(4,0) AS age,
                    nationality::VARCHAR(50) AS nationality,
                    airport_name::VARCHAR(255) AS airport_name,
                    airport_country_code::VARCHAR(3) AS airport_country_code,
                    country_name::VARCHAR(100) AS country_name,
                    airport_continent::VARCHAR(50) AS airport_continent,
                    continents::VARCHAR(50) AS continents,
                    departure_date::DATE AS departure_date,
                    arrival_airport::VARCHAR(3) AS arrival_airport,
                    pilot_name::VARCHAR(255) AS pilot_name,
                    flight_status::VARCHAR(50) AS flight_status,
                    ticket_type::VARCHAR(10) AS ticket_type,
                    passenger_status::VARCHAR(50) AS passenger_status,
                    METADATA$ACTION AS action_type,
                    METADATA$ISUPDATE AS is_update
                FROM raw_stage.passengers_wide_stream
                WHERE METADATA$ACTION IS NOT NULL
            ) as source 
            ON target.index_col = source.index_col
            WHEN MATCHED AND source.action_type = 'DELETE' THEN DELETE
            WHEN MATCHED AND source.action_type = 'INSERT' and source.is_update = TRUE THEN UPDATE SET
                target.index_col = source.index_col,
                target.first_name = source.first_name,
                target.last_name = source.last_name,
                target.gender = source.gender,
                target.age = source.age,
                target.nationality = source.nationality,
                target.airport_name = source.airport_name,
                target.airport_country_code = source.airport_country_code,
                target.country_name = source.country_name,
                target.airport_continent = source.airport_continent,
                target.continents = source.continents,
                target.departure_date = source.departure_date,
                target.arrival_airport = source.arrival_airport,
                target.pilot_name = source.pilot_name,
                target.flight_status = source.flight_status,
                target.ticket_type = source.ticket_type,
                target.passenger_status = source.passenger_status
            WHEN NOT MATCHED THEN INSERT ( 
                index_col, passenger_id, first_name, last_name, gender, age, nationality,
                airport_name, airport_country_code, country_name, airport_continent,
                continents, departure_date, arrival_airport, pilot_name, flight_status,
                ticket_type, passenger_status
            )
            VALUES (
                source.index_col, source.passenger_id, source.first_name, source.last_name, source.gender, source.age, source.nationality,
                source.airport_name, source.airport_country_code, source.country_name, source.airport_continent,
                source.continents, source.departure_date, source.arrival_airport, source.pilot_name, source.flight_status,
                source.ticket_type, source.passenger_status
            ); 
            
            -- Logs for MERGE operation
            INSERT INTO mart_stage.etl_audit_log (
                task_id, target_table, operation_type, rows_affected, query_id
            )
            SELECT
                'insert_into_core_table',
                'core_stage.passengers_cleaned', 
                'MERGE', 
                "number of rows inserted" + "number of rows updated" + "number of rows deleted", 
                LAST_QUERY_ID()
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        """
    )
    
    materialize_stream_to_temp_table  = SQLExecuteQueryOperator(
        task_id = "materialize_stream_to_temp_table",
        conn_id = "snowflake_default",
        sql = """
            CREATE OR REPLACE TRANSIENT TABLE core_stage.temp_stream_table AS 
            SELECT *
            FROM core_stage.passengers_cleaned_stream
            WHERE METADATA$ACTION IS NOT NULL;
        """
    )
    
    load_dim_passenger = SQLExecuteQueryOperator(
        task_id = "load_dim_passenger",
        conn_id = "snowflake_default",
        sql = """
            MERGE INTO mart_stage.dim_passenger AS target
            USING (
                SELECT
                    passenger_id,
                    first_name,
                    last_name,
                    gender,
                    age,
                    nationality,
                    METADATA$ACTION AS action_type,
                    METADATA$ISUPDATE AS is_update
                FROM core_stage.temp_stream_table
                WHERE METADATA$ACTION IS NOT NULL
                QUALIFY ROW_NUMBER() OVER(PARTITION BY passenger_id ORDER BY departure_date DESC) = 1
            ) AS source
            ON target.passenger_id = source.passenger_id 
            WHEN MATCHED AND source.action_type = 'DELETE' THEN DELETE 
            WHEN MATCHED AND source.action_type = 'UPDATE' THEN UPDATE SET 
                target.first_name = source.first_name,
                target.last_name = source.last_name,
                target.gender = source.gender,
                target.age = source.age,
                target.nationality = source.nationality
            WHEN NOT MATCHED AND source.action_type = 'INSERT' THEN INSERT ( 
                passenger_id, first_name, last_name, gender, age, nationality
            ) VALUES (
                source.passenger_id, source.first_name, source.last_name, source.gender, source.age, source.nationality
            );
            
            --logs
            INSERT INTO mart_stage.etl_audit_log (
                task_id, target_table, operation_type, rows_affected, query_id
            )
            SELECT
                'load_dim_passenger', 
                'mart_stage.dim_passenger', 
                'MERGE',
                SUM(
                    "number of rows inserted" + "number of rows updated" + "number of rows deleted"
                ),
                LAST_QUERY_ID()
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        """
    )
    
    load_dim_airport = SQLExecuteQueryOperator(
        task_id = "load_dim_airport",
        conn_id = "snowflake_default",
        sql = """
            MERGE INTO mart_stage.dim_airport AS target
            USING (
                SELECT
                    arrival_airport as airport_code,
                    airport_name,
                    airport_country_code as country_code,
                    country_name,
                    airport_continent as continent_code,
                    continents as continent_name,
                    METADATA$ACTION AS action_type,
                    METADATA$ISUPDATE AS is_update
                FROM core_stage.temp_stream_table
                WHERE METADATA$ACTION IS NOT NULL
                QUALIFY ROW_NUMBER() OVER(PARTITION BY arrival_airport ORDER BY departure_date DESC) = 1
            ) AS source
            ON target.airport_code = source.airport_code 
            WHEN MATCHED AND source.action_type = 'DELETE' THEN DELETE 
            WHEN MATCHED AND source.action_type = 'UPDATE' THEN UPDATE SET 
                target.airport_name = source.airport_name,
                target.country_code = source.country_code,
                target.country_name = source.country_name,
                target.continent_code = source.continent_code,
                target.continent_name = source.continent_name
            WHEN NOT MATCHED AND source.action_type = 'INSERT' THEN INSERT ( 
                airport_code, airport_name, country_code, country_name, continent_code, continent_name
            ) VALUES (
                source.airport_code, source.airport_name, source.country_code, source.country_name, source.continent_code, source.continent_name
            );
            
            --logs
            INSERT INTO mart_stage.etl_audit_log (
                task_id, target_table, operation_type, rows_affected, query_id
            )
            SELECT
                'load_dim_airport', 
                'mart_stage.dim_airport', 
                'MERGE',
                SUM(
                    "number of rows inserted" + "number of rows updated" + "number of rows deleted"
                ),
                LAST_QUERY_ID()
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            
        """
    )
    
    load_dim_flight = SQLExecuteQueryOperator(
        task_id="load_dim_flight",
        conn_id="snowflake_default",
        sql="""
                MERGE INTO mart_stage.dim_flight AS target
                USING (
                    SELECT
                        -- Use the same hashing method as the fact table
                        MD5(
                            pilot_name || flight_status || 
                            TO_CHAR(departure_date, 'YYYY-MM-DD') || arrival_airport
                        ) AS flight_id,
                        pilot_name,
                        flight_status,
                        METADATA$ACTION AS action_type,
                        METADATA$ISUPDATE AS is_update
                    FROM core_stage.temp_stream_table
                    WHERE METADATA$ACTION IS NOT NULL
                    -- Deduplicate the source records to get unique flights
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY flight_id ORDER BY departure_date) = 1
                ) AS source
                ON target.flight_id = source.flight_id 
                WHEN MATCHED AND source.action_type = 'DELETE' THEN DELETE 
                WHEN MATCHED AND source.action_type = 'UPDATE' THEN UPDATE SET  
                    target.pilot_name = source.pilot_name,
                    target.flight_status = source.flight_status
                WHEN NOT MATCHED AND source.action_type = 'INSERT' THEN INSERT ( 
                    flight_id, pilot_name, flight_status
                ) VALUES (
                    source.flight_id, source.pilot_name, source.flight_status
                );
                
                --logs
                INSERT INTO mart_stage.etl_audit_log (
                    task_id, target_table, operation_type, rows_affected, query_id
                )
                SELECT
                    'load_dim_flight', 
                    'mart_stage.dim_flight', 
                    'MERGE',
                    SUM(
                        "number of rows inserted" + "number of rows updated" + "number of rows deleted"
                    ),
                    LAST_QUERY_ID()
                FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            """
        )
    
    load_dim_date = SQLExecuteQueryOperator(
        task_id = "load_dim_date",
        conn_id = "snowflake_default",
        sql = """
            MERGE INTO mart_stage.dim_date AS target
            USING (
                SELECT
                    departure_date as date,
                    DAYOFWEEK(departure_date) as day_of_week,
                    MONTH(departure_date) as month,
                    YEAR(departure_date) as year,
                    QUARTER(departure_date) as quarter,
                    METADATA$ACTION AS action_type,
                    METADATA$ISUPDATE AS is_update
                FROM core_stage.temp_stream_table
                WHERE METADATA$ACTION IS NOT NULL
                QUALIFY ROW_NUMBER() OVER(PARTITION BY date ORDER BY departure_date DESC) = 1
            ) AS source
            ON target.date  = source.date  
            WHEN MATCHED AND source.action_type = 'DELETE' THEN DELETE 
            WHEN MATCHED AND source.action_type = 'UPDATE' THEN UPDATE SET 
                target.day_of_week = source.day_of_week,
                target.month = source.month,
                target.year = source.year,
                target.quarter = source.quarter
            WHEN NOT MATCHED AND source.action_type = 'INSERT' THEN INSERT ( 
                date, day_of_week, month, year, quarter
            ) VALUES (
                source.date, source.day_of_week, source.month, source.year, source.quarter
            );
            
            --logs
            INSERT INTO mart_stage.etl_audit_log (
                task_id, target_table, operation_type, rows_affected, query_id
            )
            SELECT
                'load_dim_date', 
                'mart_stage.dim_date', 
                'MERGE',
                SUM(
                    "number of rows inserted" + "number of rows updated" + "number of rows deleted"
                ),
                LAST_QUERY_ID()
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        """
    )
    
    load_fact_flight_passenger = SQLExecuteQueryOperator(
        task_id = "load_fact_flight_passenger",
        conn_id = "snowflake_default",
        sql="""
            MERGE INTO mart_stage.fact_passenger_flight AS target
            USING (
            SELECT
                MD5(
                src.passenger_id || '|' ||
                dim_flight.flight_sk || '|' ||
                TO_CHAR(src.departure_date, 'YYYYMMDD')
                ) AS id,
                dim_passenger.passenger_sk,
                dim_flight.flight_sk,
                dim_airport.airport_sk,
                dim_date.date_sk,
                src.passenger_status,
                src.ticket_type,
                METADATA$ACTION AS action_type
            FROM core_stage.temp_stream_table AS src
            LEFT JOIN mart_stage.dim_passenger AS dim_passenger
                ON src.passenger_id = dim_passenger.passenger_id
            LEFT JOIN mart_stage.dim_flight AS dim_flight
                ON MD5(
                    src.pilot_name || src.flight_status ||
                    TO_CHAR(src.departure_date,'YYYY-MM-DD') ||
                    src.arrival_airport
                ) = dim_flight.flight_id
            LEFT JOIN mart_stage.dim_airport AS dim_airport
                ON src.arrival_airport = dim_airport.airport_code
            LEFT JOIN mart_stage.dim_date AS dim_date
                ON src.departure_date = dim_date.date
            WHERE src.METADATA$ACTION = 'INSERT'
            ) AS source
            ON target.id = source.id

            WHEN NOT MATCHED THEN
            INSERT (
                id, passenger_sk, flight_sk, airport_sk, date_sk, passenger_status, ticket_type
            )
            VALUES (
                source.id, source.passenger_sk, source.flight_sk, source.airport_sk, source.date_sk, source.passenger_status, source.ticket_type
            );
      
            --logs
            INSERT INTO mart_stage.etl_audit_log (
                task_id, target_table, operation_type, rows_affected, query_id
            )
            SELECT
                'load_fact_flight_passenger', 
                'mart_stage.fact_passenger_flight', 
                'MERGE',
                "number of rows inserted",
                LAST_QUERY_ID()
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    """
    )
    
    test_connection >> put_into_local_stage >> copy_data_to_raw_table >> insert_into_core_table
    insert_into_core_table >> materialize_stream_to_temp_table >>[load_dim_passenger , load_dim_flight, load_dim_date, load_dim_airport] 
    [load_dim_passenger , load_dim_flight, load_dim_airport, load_dim_date]  >> load_fact_flight_passenger

