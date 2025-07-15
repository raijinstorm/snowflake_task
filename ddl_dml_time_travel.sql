-- DDL
CREATE TABLE raw_stage.raw_table_from_past 
CLONE raw_stage.PASSENGERS_WIDE  AT (TIMESTAMP => '2025-07-15 10:00:00'::TIMESTAMP);

DROP TABLE raw_stage.raw_table_from_past;

UNDROP TABLE raw_stage.raw_table_from_past;


-- DML
SELECT *
FROM raw_stage.PASSENGERS_WIDE
AT (TIMESTAMP => '2025-07-15 10:00:00'::TIMESTAMP);

SELECT *
FROM mart_stage.fact_passenger_flight
BEFORE (STATEMENT => '01bdb4e9-0305-fdf5-000d-027300035602');

