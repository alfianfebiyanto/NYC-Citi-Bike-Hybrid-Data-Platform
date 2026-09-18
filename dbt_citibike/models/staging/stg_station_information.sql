-- Staging reference station dari GBFS station_information.
-- Melakukan standardisasi nama field dan tipe data dari RAW layer.

{{ config(
    materialized='view'
) }}

SELECT
    -- Identitas station
    CAST(station_id AS STRING) AS station_id,
    TRIM(name) AS station_name,
    TRIM(short_name) AS short_name,

    -- Lokasi station
    SAFE_CAST(lat AS FLOAT64) AS latitude,
    SAFE_CAST(lon AS FLOAT64) AS longitude,
    CAST(region_id AS STRING) AS region_id,

    -- Informasi station
    SAFE_CAST(capacity AS INT64) AS capacity,
    LOWER(TRIM(station_type)) AS station_type,
    rental_methods,

    -- Metadata ingestion dari source
    _ingested_at

FROM {{ source('raw', 'station_information') }}