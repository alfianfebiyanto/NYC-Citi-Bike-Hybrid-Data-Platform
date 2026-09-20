-- Citi Bike station performance summary.
-- Grain: one row per station.

WITH departures AS (
    SELECT
        start_station_key AS station_key,
        COUNT(*) AS total_departures
    FROM {{ ref('fact_trips') }}
    GROUP BY start_station_key
),

arrivals AS (
    SELECT
        end_station_key AS station_key,
        COUNT(*) AS total_arrivals
    FROM {{ ref('fact_trips') }}
    GROUP BY end_station_key
)

SELECT
    s.station_key,
    s.station_code,
    s.station_name,
    s.latitude,
    s.longitude,

    COALESCE(d.total_departures, 0) AS total_departures,
    COALESCE(a.total_arrivals, 0) AS total_arrivals,

    COALESCE(d.total_departures, 0)
        + COALESCE(a.total_arrivals, 0) AS total_trip_activity

FROM {{ ref('dim_station') }} s

LEFT JOIN departures d
    ON s.station_key = d.station_key

LEFT JOIN arrivals a
    ON s.station_key = a.station_key