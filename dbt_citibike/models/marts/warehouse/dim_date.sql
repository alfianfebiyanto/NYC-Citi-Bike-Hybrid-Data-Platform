-- Dimensi tanggal Citi Bike.

{{ config(materialized='table') }}

SELECT DISTINCT
    trip_date AS date_key,
    EXTRACT(DAY FROM trip_date) AS day,
    FORMAT_DATE('%A', trip_date) AS day_name,

    CASE
        WHEN EXTRACT(DAYOFWEEK FROM trip_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    EXTRACT(MONTH FROM trip_date) AS month,
    FORMAT_DATE('%B', trip_date) AS month_name,
    EXTRACT(YEAR FROM trip_date) AS year

FROM {{ ref('int_citibike_trips_clean') }}