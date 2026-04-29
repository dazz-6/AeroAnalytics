
-- =============================================================================
-- AeroShield: Airline Disruption Intelligence Platform
-- CLEAN VERSION (ERROR-FREE)
-- =============================================================================

-- =============================================================================
-- PART A — TABLE SCHEMAS
-- =============================================================================

CREATE TABLE IF NOT EXISTS flights_cleaned (
    flight_id           SERIAL PRIMARY KEY,
    year                SMALLINT,
    month               SMALLINT,
    day_of_week         SMALLINT,
    fl_date             DATE,
    op_unique_carrier             VARCHAR(10),
    tail_num            VARCHAR(10),
    flight_num          VARCHAR(10),
    origin              CHAR(3),
    dest                CHAR(3),
    route               VARCHAR(8),
    crs_dep_time        SMALLINT,
    dep_time            SMALLINT,
    dep_delay           NUMERIC(6,1),
    dep_del15           SMALLINT,
    taxi_out            NUMERIC(5,1),
    taxi_in             NUMERIC(5,1),
    crs_arr_time        SMALLINT,
    arr_time            SMALLINT,
    arr_delay           NUMERIC(6,1),
    arr_del15           SMALLINT,
    cancelled           SMALLINT,
    diverted            SMALLINT,
    air_time            NUMERIC(6,1),
    distance            NUMERIC(7,1),
    carrier_delay       NUMERIC(6,1),
    weather_delay       NUMERIC(6,1),
    nas_delay           NUMERIC(6,1),
    security_delay      NUMERIC(6,1),
    late_aircraft_delay NUMERIC(6,1),
    disrupted           SMALLINT,
    is_weekend          SMALLINT,
    dep_hour            SMALLINT,
    arr_hour            SMALLINT,
    time_block          VARCHAR(12)
);


CREATE TABLE IF NOT EXISTS airport_summary (
    origin CHAR(3) PRIMARY KEY,
    total_flights INTEGER,
    disrupted_flights INTEGER,
    avg_dep_delay NUMERIC(6,2),
    avg_arr_delay NUMERIC(6,2),
    cancellation_rate NUMERIC(5,4),
    diversion_rate NUMERIC(5,4),
    disruption_rate NUMERIC(5,4),
    afi NUMERIC(5,4)
);


CREATE TABLE IF NOT EXISTS route_summary (
    route VARCHAR(8) PRIMARY KEY,
    total_flights INTEGER,
    disrupted_flights INTEGER,
    avg_dep_delay NUMERIC(6,2),
    avg_arr_delay NUMERIC(6,2),
    avg_distance NUMERIC(7,2),
    disruption_rate NUMERIC(5,4),
    rrs NUMERIC(5,2)
);


CREATE TABLE IF NOT EXISTS carrier_summary (
    op_unique_carrier VARCHAR(10) PRIMARY KEY,
    total_flights INTEGER,
    disrupted_flights INTEGER,
    avg_dep_delay NUMERIC(6,2),
    avg_arr_delay NUMERIC(6,2),
    cancellations INTEGER,
    diversions INTEGER,
    disruption_rate NUMERIC(5,4),
    cancellation_rate NUMERIC(5,4)
);


CREATE TABLE IF NOT EXISTS monthly_summary (
    year SMALLINT,
    month SMALLINT,
    total_flights INTEGER,
    disrupted_flights INTEGER,
    avg_dep_delay NUMERIC(6,2),
    cancellations INTEGER,
    disruption_rate NUMERIC(5,4),
    PRIMARY KEY (year, month)
);


CREATE TABLE IF NOT EXISTS delay_cause_summary (
    cause VARCHAR(30) PRIMARY KEY,
    total_minutes NUMERIC(12,1),
    avg_minutes NUMERIC(6,2),
    flight_count INTEGER,
    pct_of_total NUMERIC(5,2)
);


-- =============================================================================
-- PART B — INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_flights_origin    ON flights_cleaned (origin);
CREATE INDEX IF NOT EXISTS idx_flights_dest      ON flights_cleaned (dest);
CREATE INDEX IF NOT EXISTS idx_flights_route     ON flights_cleaned (route);
CREATE INDEX IF NOT EXISTS idx_flights_carrier   ON flights_cleaned (op_unique_carrier);
CREATE INDEX IF NOT EXISTS idx_flights_month     ON flights_cleaned (year, month);
CREATE INDEX IF NOT EXISTS idx_flights_disrupted ON flights_cleaned (disrupted);
CREATE INDEX IF NOT EXISTS idx_flights_date      ON flights_cleaned (fl_date);


-- =============================================================================
-- PART C — VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW vw_top10_disrupted_airports AS
SELECT
    origin AS airport,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 1) AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1) AS avg_dep_delay_min,
    ROUND((cancellation_rate * 100)::numeric, 2) AS cancel_pct,
    ROUND(afi::numeric, 4) AS fragility_index
FROM airport_summary
ORDER BY disruption_pct DESC
LIMIT 10;


CREATE OR REPLACE VIEW vw_top10_unstable_routes AS
SELECT
    route,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 1) AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1) AS avg_dep_delay_min,
    ROUND(rrs::numeric, 1) AS reliability_score
FROM route_summary
WHERE total_flights >= 50
ORDER BY disruption_pct DESC
LIMIT 10;


CREATE OR REPLACE VIEW vw_airline_disruption AS
SELECT
    op_unique_carrier,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 1) AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1) AS avg_dep_delay_min,
    ROUND(avg_arr_delay::numeric, 1) AS avg_arr_delay_min,
    cancellations,
    ROUND((cancellation_rate * 100)::numeric, 2) AS cancel_pct,
    diversions
FROM carrier_summary
ORDER BY disruption_pct DESC;


CREATE OR REPLACE VIEW vw_monthly_trend AS
SELECT
    year,
    month,
    TO_CHAR(TO_DATE(month::TEXT, 'MM'), 'Mon') AS month_name,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 2) AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1) AS avg_dep_delay_min,
    cancellations
FROM monthly_summary
ORDER BY year, month;


CREATE OR REPLACE VIEW vw_delay_root_causes AS
SELECT
    cause,
    ROUND((total_minutes / 60.0)::numeric, 0) AS total_hours,
    total_minutes,
    avg_minutes,
    flight_count,
    ROUND(pct_of_total::numeric,1) AS pct_of_total_delay
FROM delay_cause_summary
ORDER BY total_minutes DESC;


CREATE OR REPLACE VIEW vw_top_cancel_airports AS
SELECT
    origin AS airport,
    total_flights,
    ROUND((cancellation_rate * 100)::numeric, 2) AS cancel_pct,
    ROUND((disruption_rate * 100)::numeric, 1) AS disruption_pct,
    ROUND(afi::numeric, 4) AS fragility_index
FROM airport_summary
WHERE total_flights >= 100
ORDER BY cancel_pct DESC
LIMIT 15;


CREATE OR REPLACE VIEW vw_top_arrival_delay_airports AS
SELECT
    origin AS airport,
    total_flights,
    ROUND(avg_arr_delay::numeric, 1) AS avg_arr_delay_min,
    ROUND((disruption_rate * 100)::numeric, 1) AS disruption_pct,
    ROUND(afi::numeric, 4) AS fragility_index
FROM airport_summary
WHERE total_flights >= 100
ORDER BY avg_arr_delay DESC
LIMIT 15;


-- =============================================================================
-- PART D — QUICK TEST QUERY
-- =============================================================================

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';





SELECT column_name
FROM information_schema.columns
WHERE table_name = 'flights_cleaned';