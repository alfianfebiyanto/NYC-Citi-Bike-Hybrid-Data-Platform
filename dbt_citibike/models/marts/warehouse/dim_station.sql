-- Dimensi station untuk historical trips dan GBFS realtime.

{{ config(materialized='table') }}

WITH historical AS (
    SELECT
        station_code,
        ANY_VALUE(station_name) AS station_name,
        ANY_VALUE(latitude) AS latitude,
        ANY_VALUE(longitude) AS longitude
    FROM (
        SELECT
            start_station_id AS station_code,
            start_station_name AS station_name,
            start_lat AS latitude,
            start_lng AS longitude
        FROM {{ ref('int_citibike_trips_clean') }}

        UNION ALL

        SELECT
            end_station_id,
            end_station_name,
            end_lat,
            end_lng
        FROM {{ ref('int_citibike_trips_clean') }}
    )
    GROUP BY station_code
)

SELECT
    TO_HEX(MD5(COALESCE(g.short_name, h.station_code))) AS station_key,
    COALESCE(g.short_name, h.station_code) AS station_code,
    g.station_id AS gbfs_station_id,
    COALESCE(g.station_name, h.station_name) AS station_name,
    COALESCE(g.latitude, h.latitude) AS latitude,
    COALESCE(g.longitude, h.longitude) AS longitude,
    g.region_id,
    g.capacity,
    g.station_type,
    g.rental_methods

FROM {{ ref('int_station_information_clean') }} g
FULL JOIN historical h
    ON g.short_name = h.station_code