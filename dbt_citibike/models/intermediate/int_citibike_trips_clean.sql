-- Intermediate model untuk validasi business rules
-- dan enrichment data historical Citi Bike trips.

{{ config(
    materialized='view'
) }}

SELECT
    -- Identitas perjalanan dan kategori
    ride_id,
    rideable_type,
    member_casual,

    -- Waktu perjalanan
    started_at,
    ended_at,
    DATE(started_at) AS trip_date,

    -- Durasi perjalanan dalam menit
    ROUND(
        TIMESTAMP_DIFF(ended_at, started_at, SECOND) / 60.0,
        2
    ) AS ride_duration_minutes,

    -- Atribut waktu
    EXTRACT(YEAR FROM started_at) AS ride_year,
    EXTRACT(MONTH FROM started_at) AS ride_month,
    FORMAT_TIMESTAMP('%A', started_at) AS ride_day_of_week,
    EXTRACT(HOUR FROM started_at) AS ride_hour,

    -- Weekday / weekend
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM started_at) IN (1, 7)
            THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    -- Segmentasi waktu perjalanan
    CASE
        WHEN EXTRACT(HOUR FROM started_at) BETWEEN 0 AND 5
            THEN 'late night'
        WHEN EXTRACT(HOUR FROM started_at) BETWEEN 6 AND 9
            THEN 'morning rush'
        WHEN EXTRACT(HOUR FROM started_at) BETWEEN 10 AND 15
            THEN 'daytime'
        WHEN EXTRACT(HOUR FROM started_at) BETWEEN 16 AND 19
            THEN 'evening rush'
        ELSE 'night'
    END AS time_of_day,

    -- Station keberangkatan
    start_station_id,
    start_station_name,
    start_lat,
    start_lng,

    -- Station tujuan
    end_station_id,
    end_station_name,
    end_lat,
    end_lng,

    -- Jarak garis lurus start ke end dalam kilometer
    ROUND(
        ST_DISTANCE(
            ST_GEOGPOINT(start_lng, start_lat),
            ST_GEOGPOINT(end_lng, end_lat)
        ) / 1000.0,
        2
    ) AS trip_distance_km

FROM {{ ref('stg_citibike_trips') }}

WHERE
    ride_id IS NOT NULL
    AND started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND ended_at > started_at

    AND member_casual IN ('member', 'casual')
    AND rideable_type IN ('classic_bike', 'electric_bike')

    AND start_station_id IS NOT NULL
    AND start_station_name IS NOT NULL
    AND end_station_id IS NOT NULL
    AND end_station_name IS NOT NULL

    AND start_lat IS NOT NULL
    AND start_lng IS NOT NULL
    AND end_lat IS NOT NULL
    AND end_lng IS NOT NULL

    AND start_lat BETWEEN -90 AND 90
    AND end_lat BETWEEN -90 AND 90
    AND start_lng BETWEEN -180 AND 180
    AND end_lng BETWEEN -180 AND 180