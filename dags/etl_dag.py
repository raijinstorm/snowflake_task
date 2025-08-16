from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import os
import logging

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
CSV_FILE_NAME = os.getenv("CSV_FILE_NAME", "airline_data.csv") 
FULL_FILE_PATH = os.path.join(DATA_DIR, CSV_FILE_NAME)

#User's default internal stage
SNOWFLAKE_INTERNAL_STAGE = "@~"

def check_stream_for_new_data():
    hook = SnowflakeHook(snowflake_conn_id = "snowflake_default")
    conn = hook.get_conn()
    curr = conn.cursor()
    
    curr.execute("SELECT COUNT(*) FROM core_stage.passengers_cleaned_stream;")
    row_count = curr.fetchone()[0]
    
    curr.close()
    conn.close()
    
    return row_count > 0
    
     
def log_snowflake_query_to_audit(task_id, target_table, operation_type, sql):
    """Execute a SQL statement in Snowflake and log metadata to an audit table."""
    start_time = datetime.now()
    end_time = start_time
    rows_affected = 0
    query_id = None
    error_message = None
    
    hook = SnowflakeHook(snowflake_conn_id = "snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        query_id = cursor.sfqid
        end_time = datetime.now()
        
        result_scan_sql = f"""
            SELECT
                "number of rows inserted",
                "number of rows updated",
                "number of rows deleted"
            FROM TABLE(RESULT_SCAN('{query_id}'));
        """
    
        result_scan_cursor = hook.run(result_scan_sql, autocommit=True, handler=lambda cur: cur.fetchone())
        
        if result_scan_cursor: 

            inserted = result_scan_cursor[0] if result_scan_cursor[0] is not None else 0
            updated = result_scan_cursor[1] if result_scan_cursor[1] is not None else 0
            deleted = result_scan_cursor[2] if result_scan_cursor[2] is not None else 0
            rows_affected = inserted + updated + deleted
        else:
            logging.warning(f"No result found from RESULT_SCAN for query_id: {query_id}")

    except Exception as e:
        logging.error(f"Error in task {task_id}: {e}")
        error_message = str(e)
        rows_affected = 0
        end_time = datetime.now()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

        execution_time = (end_time - start_time).total_seconds()

        sql_error_message = ("'" + error_message.replace("'", "''") + "'") if error_message else 'NULL'

        audit_sql = f"""
            INSERT INTO mart_stage.etl_audit_log (
                task_id, target_table, operation_type, rows_affected, query_id,
                start_timestamp, end_timestamp, execution_time, error_message
            )
            VALUES (
                '{task_id}',
                '{target_table}',
                '{operation_type}',
                {rows_affected}, -- Use the calculated rows_affected
                {("'" + query_id + "'") if query_id else 'NULL'},
                '{start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}',
                '{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')}',
                {execution_time},
                {sql_error_message}
            );
        """
        try:
            hook.run(audit_sql, autocommit=True)
            logging.info(f"Successfully inserted audit log for task {task_id}.")
        except Exception as audit_e:
            logging.error(f"Failed to insert audit log for task {task_id}: {audit_e}")
            
                     
with DAG(
    "etl_snowflake",
) as dag:
    wait_for_file = FileSensor(
        task_id = "wait_for_file",
        filepath = FULL_FILE_PATH,
        poke_interval = 30,
        timeout = 600,
        fs_conn_id = "fs_default", 
        dag = dag
    )
    
    #PUT file to stage
    put_into_local_stage = SQLExecuteQueryOperator(
        task_id = "put_into_local_stage",
        conn_id = "snowflake_default",
        sql = f"""
            PUT 'file://{FULL_FILE_PATH}' {SNOWFLAKE_INTERNAL_STAGE}/uploads/{CSV_FILE_NAME}
            AUTO_COMPRESS = FALSE;
        """
    )
    
    # COPY from stage to table
    copy_data_to_raw_table = SQLExecuteQueryOperator(
        task_id = "copy_data_to_raw_table",
        conn_id = "snowflake_default",
        sql = f"""
            COPY INTO raw_stage.passengers_wide
            FROM {SNOWFLAKE_INTERNAL_STAGE}/uploads/{CSV_FILE_NAME}
            FILE_FORMAT = (
                TYPE = CSV,
                SKIP_HEADER = 1,
                NULL_IF = ('NULL', ''),
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            )
            ON_ERROR = 'CONTINUE';
        """
    )
    
    log_copy_info = SQLExecuteQueryOperator(
        task_id = "log_copy_info",
        conn_id = "snowflake_default",
        sql = """
            INSERT INTO raw_stage.copy_info_table (
                file_name,
                file_size,
                last_load_time,
                status,
                rows_parsed ,
                rows_loaded,
                error_rows,
                first_error_message,
                first_error_line_number 
            )
            SELECT
                file_name,
                file_size,
                last_load_time,
                status,
                row_parsed AS rows_parsed ,
                row_count AS rows_loaded,
                error_count as error_rows,
                first_error_message,
                first_error_line_number  
            FROM
                TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'RAW_STAGE.PASSENGERS_WIDE', START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())));
        """
    )
    
    insert_into_core_table = PythonOperator(
        task_id = "insert_into_core_table",
        python_callable = log_snowflake_query_to_audit,
        op_kwargs = {
            "task_id": "insert_into_core_table",
            "target_table": "core_stage.passengers_cleaned",
            "operation_type": "MERGE",
        "sql" : f"""
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
            
        """
        }
    )
    
    check_stream = ShortCircuitOperator(
        task_id = "check_stream",
        python_callable = check_stream_for_new_data
    )
    
    materialize_stream_data  = SQLExecuteQueryOperator(
        task_id = "materialize_stream_data",
        conn_id = "snowflake_default",
        sql = """
            TRUNCATE TABLE core_stage.temp_stream_table;
            INSERT INTO core_stage.temp_stream_table
                SELECT * FROM core_stage.passengers_cleaned_stream;
        """
    )
    
    load_dim_passenger = PythonOperator(
        task_id = "load_dim_passenger",
        python_callable = log_snowflake_query_to_audit,
        op_kwargs = {
            "task_id": "load_dim_passenger",
            "target_table": "mart_stage.dim_passenger",
            "operation_type": "MERGE",
            "sql" : """
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
                
                
            """
        }
    )
    
    load_dim_airport = PythonOperator(
        task_id = "load_dim_airport",
        python_callable = log_snowflake_query_to_audit,
        op_kwargs = {
            "task_id": "load_dim_airport",
            "target_table": "mart_stage.dim_airport",
            "operation_type": "MERGE",
            "sql" : """
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
                
                
                
            """
        }
    )
    
    load_dim_flight = PythonOperator(
        task_id="load_dim_flight",
        python_callable = log_snowflake_query_to_audit,
        op_kwargs = {
            "task_id": "load_dim_flight",
            "target_table": "mart_stage.dim_flight",
            "operation_type": "MERGE",
            "sql":"""
                    MERGE INTO mart_stage.dim_flight AS target
                    USING (
                        SELECT
                            -- Use the same hashing method as the fact table
                            MD5(
                                TRIM(LOWER(pilot_name)) || flight_status || 
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
                    
                    
                """
            }
        )
    
    load_dim_date = PythonOperator(
        task_id = "load_dim_date",
        python_callable = log_snowflake_query_to_audit,
        op_kwargs = {
            "task_id": "load_dim_date",
            "target_table": "mart_stage.dim_date",
            "operation_type": "MERGE",
            "sql" : """
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
                
            """
        }
    )
    
    load_fact_flight_passenger = PythonOperator(
        task_id = "load_fact_flight_passenger",
        python_callable = log_snowflake_query_to_audit,
        op_kwargs = {
            "task_id": "load_fact_flight_passenger",
            "target_table": "mart_stage.fact_passenger_flight",
            "operation_type": "MERGE",
            "sql":"""
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
                        TRIM(LOWER(src.pilot_name))  || src.flight_status ||
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
                WHEN MATCHED AND source.action_type = 'DELETE' THEN DELETE 
                WHEN MATCHED AND source.action_type = 'UPDATE' THEN UPDATE SET 
                    target.passenger_sk = source.passenger_sk,
                    target.flight_sk = source.flight_sk,
                    target.airport_sk = source.airport_sk,
                    target.date_sk = source.date_sk,
                    target.passenger_status = source.passenger_status,
                    target.ticket_type = source.ticket_type
                WHEN NOT MATCHED AND source.action_type = 'INSERT' THEN INSERT ( 
                    id, passenger_sk, flight_sk, airport_sk, date_sk, passenger_status, ticket_type
                ) VALUES (
                    source.id, source.passenger_sk, source.flight_sk, source.airport_sk, source.date_sk, source.passenger_status, source.ticket_type
                );
                
        """
        }
    )
    
    wait_for_file >> put_into_local_stage >> copy_data_to_raw_table
    copy_data_to_raw_table >> [log_copy_info, insert_into_core_table]
    insert_into_core_table >> check_stream >> materialize_stream_data >>[load_dim_passenger , load_dim_flight, load_dim_date, load_dim_airport] 
    [load_dim_passenger , load_dim_flight, load_dim_airport, load_dim_date]  >> load_fact_flight_passenger

