CREATE SECURE VIEW flights_airports_secure AS 
SELECT id, passenger_sk, flight_sk, date_sk, airport_name, airport_code, country_name, continent_name
FROM fact_passenger_flight as fact
JOIN  dim_airport as arp
    ON fact.airport_sk = arp.airport_sk
LIMIT 100;

CREATE ROW ACCESS POLICY mart_stage.continent_europe_policy
AS (continent_name VARCHAR) RETURNS BOOLEAN ->
    continent_name = 'Europe';

ALTER VIEW mart_stage.flights_airports_secure
ADD ROW ACCESS POLICY mart_stage.continent_europe_policy ON (continent_name);

