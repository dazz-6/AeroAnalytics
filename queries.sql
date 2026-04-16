-- =============================================================================
-- AeroShield: Airline Disruption Intelligence Platform
-- LAYER 2 — SQL Schema + Analytical Queries
-- Compatible with: PostgreSQL 14+ / SQLite 3.35+ / SQL Server 2019+
-- =============================================================================
-- Workflow:
--   Python exports CSVs → Load into DB → Run queries → Connect to Power BI
-- PART A — TABLE SCHEMAS
-- (Create these, then bulk-load from Python-exported CSVs)
-- =============================================================================

-- ── flights_cleaned ──────────────────────────────────────────────────────────
-- Primary fact table: one row per flight, loaded from Python output
CREATE TABLE IF NOT EXISTS flights_cleaned (
    flight_id           SERIAL PRIMARY KEY,
    year                SMALLINT,
    month               SMALLINT,
    day_of_week         SMALLINT,
    fl_date             DATE,
    carrier             VARCHAR(10),       -- OP_UNIQUE_CARRIER
    tail_num            VARCHAR(10),
    flight_num          VARCHAR(10),       -- OP_CARRIER_FL_NUM
    origin              CHAR(3),
    dest                CHAR(3),
    route               VARCHAR(8),        -- engineered: 'ORIGIN-DEST'
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
    disrupted           SMALLINT,          -- engineered: 0/1
    is_weekend          SMALLINT,          -- engineered: 0/1
    dep_hour            SMALLINT,          -- engineered: 0-23
    arr_hour            SMALLINT,          -- engineered: 0-23
    time_block          VARCHAR(12)        -- engineered: Night/Morning/etc.
);

-- ── airport_summary ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS airport_summary (
    origin              CHAR(3) PRIMARY KEY,
    total_flights       INTEGER,
    disrupted_flights   INTEGER,
    avg_dep_delay       NUMERIC(6,2),
    avg_arr_delay       NUMERIC(6,2),
    cancellation_rate   NUMERIC(5,4),
    diversion_rate      NUMERIC(5,4),
    disruption_rate     NUMERIC(5,4),
    afi                 NUMERIC(5,4)        -- Airport Fragility Index
);

-- ── route_summary ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS route_summary (
    route               VARCHAR(8) PRIMARY KEY,
    total_flights       INTEGER,
    disrupted_flights   INTEGER,
    avg_dep_delay       NUMERIC(6,2),
    avg_arr_delay       NUMERIC(6,2),
    avg_distance        NUMERIC(7,2),
    disruption_rate     NUMERIC(5,4),
    rrs                 NUMERIC(5,2)        -- Route Reliability Score (0-100)
);

-- ── carrier_summary ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS carrier_summary (
    carrier             VARCHAR(10) PRIMARY KEY,
    total_flights       INTEGER,
    disrupted_flights   INTEGER,
    avg_dep_delay       NUMERIC(6,2),
    avg_arr_delay       NUMERIC(6,2),
    cancellations       INTEGER,
    diversions          INTEGER,
    disruption_rate     NUMERIC(5,4),
    cancellation_rate   NUMERIC(5,4)
);

-- ── monthly_summary ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS monthly_summary (
    year                SMALLINT,
    month               SMALLINT,
    total_flights       INTEGER,
    disrupted_flights   INTEGER,
    avg_dep_delay       NUMERIC(6,2),
    cancellations       INTEGER,
    disruption_rate     NUMERIC(5,4),
    PRIMARY KEY (year, month)
);

-- ── delay_cause_summary ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS delay_cause_summary (
    cause               VARCHAR(30) PRIMARY KEY,
    total_minutes       NUMERIC(12,1),
    avg_minutes         NUMERIC(6,2),
    flight_count        INTEGER,
    pct_of_total        NUMERIC(5,2)
);


-- =============================================================================
-- PART B — INDEXES (performance for large datasets)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_flights_origin    ON flights_cleaned (origin);
CREATE INDEX IF NOT EXISTS idx_flights_dest      ON flights_cleaned (dest);
CREATE INDEX IF NOT EXISTS idx_flights_route     ON flights_cleaned (route);
CREATE INDEX IF NOT EXISTS idx_flights_carrier   ON flights_cleaned (carrier);
CREATE INDEX IF NOT EXISTS idx_flights_month     ON flights_cleaned (year, month);
CREATE INDEX IF NOT EXISTS idx_flights_disrupted ON flights_cleaned (disrupted);
CREATE INDEX IF NOT EXISTS idx_flights_date      ON flights_cleaned (fl_date);


-- =============================================================================
-- PART C — ANALYTICAL VIEWS (Power BI can connect directly to these)
-- =============================================================================

-- ── V1: Top 10 Most Disrupted Airports ───────────────────────────────────────
CREATE OR REPLACE VIEW vw_top10_disrupted_airports AS
SELECT
    origin                              AS airport,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 1)     AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1)             AS avg_dep_delay_min,
    ROUND((cancellation_rate * 100)::numeric,  2)   AS cancel_pct,
    ROUND(afi::numeric, 4)                       AS fragility_index
FROM airport_summary
ORDER BY disruption_pct DESC
LIMIT 10;



-- ── V2: Top 10 Unstable Routes ───────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_top10_unstable_routes AS
SELECT
    route,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 1)     AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1)             AS avg_dep_delay_min,
    ROUND(rrs::numeric, 1)                       AS reliability_score
FROM route_summary
WHERE total_flights >= 50                -- filter out low-volume noise
ORDER BY disruption_pct DESC
LIMIT 10;


-- ── V3: Airline-Wise Disruption Rate ─────────────────────────────────────────
CREATE OR REPLACE VIEW vw_airline_disruption AS
SELECT
    carrier,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 1)    AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1)             AS avg_dep_delay_min,
    ROUND(avg_arr_delay::numeric, 1)             AS avg_arr_delay_min,
    cancellations,
    ROUND((cancellation_rate * 100)::numeric, 2)   AS cancel_pct,
    diversions
FROM carrier_summary
ORDER BY disruption_pct DESC;


-- ── V4: Monthly Disruption Trend ─────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_monthly_trend AS
SELECT
    year,
    month,
    TO_CHAR(TO_DATE(month::TEXT, 'MM'), 'Mon') AS month_name,
    total_flights,
    disrupted_flights,
    ROUND((disruption_rate * 100)::numeric, 2) AS disruption_pct,
    ROUND(avg_dep_delay::numeric, 1)           AS avg_dep_delay_min,
    cancellations
FROM monthly_summary
ORDER BY year, month;

-- ── V5: Delay Root Cause Breakdown ───────────────────────────────────────────
CREATE OR REPLACE VIEW vw_delay_root_causes AS
SELECT
    cause,
    ROUND(total_minutes / 60.0, 0)      AS total_hours,
    total_minutes,
    avg_minutes,
    flight_count,
    ROUND(pct_of_total::numeric,1)              AS pct_of_total_delay
FROM delay_cause_summary
ORDER BY total_minutes DESC;


-- ── V6: Top Airports by Cancellation Rate ────────────────────────────────────
CREATE OR REPLACE VIEW vw_top_cancel_airports AS
SELECT
    origin AS airport,
    total_flights,
    ROUND((cancellation_rate * 100)::numeric, 2) AS cancel_pct,
    ROUND((disruption_rate * 100)::numeric, 1)   AS disruption_pct,
    ROUND(afi::numeric,  4)                      AS fragility_index
FROM airport_summary
WHERE total_flights >= 100
ORDER BY cancel_pct DESC
LIMIT 15;


-- ── V7: Top Airports by Average Arrival Delay ────────────────────────────────
CREATE OR REPLACE VIEW vw_top_arr_delay_airports AS
SELECT
    origin AS airport,
    total_flights,
    ROUND(avg_arr_delay::numeric, 1)             AS avg_arr_delay_min,
    ROUND(avg_dep_delay::numeric, 1)             AS avg_dep_delay_min,
    ROUND((disruption_rate * 100)::numeric, 1)   AS disruption_pct,
    ROUND(afi::numeric, 4)                       AS fragility_index
FROM airport_summary
WHERE total_flights >= 100
ORDER BY avg_arr_delay_min DESC
LIMIT 15;


   

-- =============================================================================
-- PART D — AD-HOC ANALYTICAL QUERIES
-- (Use in reporting / Power BI custom queries / stakeholder requests)
-- =============================================================================

-- ── Q1: Daily disruption count (last 30 days) ─────────────────────────────────
SELECT
    fl_date,
    COUNT(*)                           AS total_flights,
    SUM(disrupted)                     AS disrupted_flights,
    ROUND(AVG(disrupted) * 100, 1)     AS disruption_pct,
    ROUND(AVG(dep_delay), 1)           AS avg_dep_delay
FROM flights_cleaned
WHERE fl_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY fl_date
ORDER BY fl_date;


-- ── Q2: Route instability with carrier breakdown ──────────────────────────────
SELECT
    f.route,
    f.carrier,
    COUNT(*)                            AS flights,
    ROUND(AVG(f.disrupted) * 100, 1)    AS disruption_pct,
    ROUND(AVG(f.dep_delay), 1)          AS avg_dep_delay,
    ROUND(AVG(f.arr_delay), 1)          AS avg_arr_delay
FROM flights_cleaned f
GROUP BY f.route, f.carrier
HAVING COUNT(*) >= 30
ORDER BY disruption_pct DESC
LIMIT 20;


-- ── Q3: Time-block disruption heatmap data ────────────────────────────────────
SELECT
    time_block,
    day_of_week,
    COUNT(*)                            AS flights,
    ROUND(AVG(disrupted) * 100, 1)      AS disruption_pct
FROM flights_cleaned
GROUP BY time_block, day_of_week
ORDER BY day_of_week, time_block;


-- ── Q4: Weekend vs weekday comparison ────────────────────────────────────────
SELECT
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END  AS day_type,
    COUNT(*)                            AS total_flights,
    ROUND(AVG(disrupted) * 100, 1)      AS disruption_pct,
    ROUND(AVG(dep_delay), 1)            AS avg_dep_delay,
    ROUND(AVG(arr_delay), 1)            AS avg_arr_delay
FROM flights_cleaned
GROUP BY is_weekend;


-- ── Q5: Origin-Destination delay matrix (top 20 routes) ─────────────────────
SELECT
    origin,
    dest,
    COUNT(*)                            AS flights,
    ROUND(SUM(dep_delay) / NULLIF(COUNT(*), 0), 1)  AS avg_dep_delay,
    ROUND(AVG(disrupted) * 100, 1)      AS disruption_pct
FROM flights_cleaned
GROUP BY origin, dest
HAVING COUNT(*) >= 50
ORDER BY disruption_pct DESC
LIMIT 20;


-- ── Q6: KPI Summary Card values (for Power BI Executive Page) ────────────────
SELECT
    COUNT(*)                                         AS total_flights,
    SUM(disrupted)                                   AS total_disrupted,
    ROUND(AVG(disrupted) * 100, 1)                   AS overall_disruption_pct,
    SUM(cancelled)                                   AS total_cancelled,
    ROUND(AVG(cancelled) * 100, 2)                   AS cancellation_pct,
    SUM(diverted)                                    AS total_diverted,
    ROUND(AVG(dep_delay), 1)                         AS avg_dep_delay,
    ROUND(AVG(arr_delay), 1)                         AS avg_arr_delay,
    ROUND(SUM(carrier_delay + weather_delay +
              nas_delay + security_delay +
              late_aircraft_delay) / 60.0, 0)        AS total_delay_hours
FROM flights_cleaned;


-- ── Q7: Carrier delay cause breakdown ────────────────────────────────────────
SELECT
    carrier,
    ROUND(AVG(carrier_delay), 2)        AS avg_carrier_delay,
    ROUND(AVG(weather_delay), 2)        AS avg_weather_delay,
    ROUND(AVG(nas_delay), 2)            AS avg_nas_delay,
    ROUND(AVG(security_delay), 2)       AS avg_security_delay,
    ROUND(AVG(late_aircraft_delay), 2)  AS avg_late_aircraft_delay,
    ROUND(AVG(dep_delay), 2)            AS avg_total_dep_delay
FROM flights_cleaned
GROUP BY carrier
ORDER BY avg_total_dep_delay DESC;


-- ── Q8: Airport Fragility Index leaderboard ───────────────────────────────────
SELECT
    origin                              AS airport,
    total_flights,
    ROUND(disruption_rate * 100, 1)     AS disruption_pct,
    ROUND(avg_dep_delay, 1)             AS avg_dep_delay_min,
    ROUND(cancellation_rate * 100, 2)   AS cancel_pct,
    ROUND(afi * 100, 1)                 AS fragility_score   -- scaled 0-100
FROM airport_summary
ORDER BY afi DESC
LIMIT 20;


SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';


TRUNCATE TABLE carrier_summary CASCADE;
TRUNCATE TABLE monthly_summary CASCADE;
TRUNCATE TABLE delay_cause_summary CASCADE;