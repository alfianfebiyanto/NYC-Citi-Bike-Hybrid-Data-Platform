-- Membersihkan dan memvalidasi reference station dari GBFS.

{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('stg_station_information') }}

WHERE
    station_id IS NOT NULL
    AND short_name IS NOT NULL
    AND station_name IS NOT NULL
    AND latitude BETWEEN -90 AND 90
    AND longitude BETWEEN -180 AND 180
    AND (capacity IS NULL OR capacity >= 0)