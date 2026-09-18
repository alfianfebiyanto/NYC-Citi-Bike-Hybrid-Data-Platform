-- Staging data historical Citi Bike trips.
-- Standardisasi field dan tipe data dari RAW layer.

{{ config(
    materialized='view'
) }}

SELECT
    -- Identitas perjalanan
    CAST(ride_id AS STRING) AS ride_id,

    -- Kategori perjalanan
    LOWER(TRIM(rideable_type)) AS rideable_type,
    LOWER(TRIM(member_casual)) AS member_casual,

    -- Waktu perjalanan
    CAST(started_at AS TIMESTAMP) AS started_at,
    CAST(ended_at AS TIMESTAMP) AS ended_at,

    -- Informasi stasiun
    TRIM(start_station_id) AS start_station_id,
    TRIM(start_station_name) AS start_station_name,
    TRIM(end_station_id) AS end_station_id,
    TRIM(end_station_name) AS end_station_name,

    -- Koordinat
    SAFE_CAST(start_lat AS FLOAT64) AS start_lat,
    SAFE_CAST(start_lng AS FLOAT64) AS start_lng,
    SAFE_CAST(end_lat AS FLOAT64) AS end_lat,
    SAFE_CAST(end_lng AS FLOAT64) AS end_lng,

    -- Metadata staging
    CURRENT_TIMESTAMP() AS _ingested_at

FROM {{ source('raw', 'raw_citibike_trips') }}