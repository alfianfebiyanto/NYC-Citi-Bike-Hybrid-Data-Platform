-- Daily Citi Bike usage summary.
-- Grain: one date x rider type x bike type.

{{ config(
    materialized='table',
    partition_by={
        "field": "ride_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    f.date_key AS ride_date,
    d.day_name,
    d.is_weekend,
    r.rider_type,
    b.bike_type,

    COUNT(*) AS total_rides,
    ROUND(AVG(f.ride_duration_minutes), 2) AS avg_ride_duration_minutes,
    ROUND(AVG(f.trip_distance_km), 2) AS avg_trip_distance_km

FROM {{ ref('fact_trips') }} f

INNER JOIN {{ ref('dim_date') }} d
    ON f.date_key = d.date_key

INNER JOIN {{ ref('dim_rider_type') }} r
    ON f.rider_type_key = r.rider_type_key

INNER JOIN {{ ref('dim_bike_type') }} b
    ON f.bike_type_key = b.bike_type_key

GROUP BY
    f.date_key,
    d.day_name,
    d.is_weekend,
    r.rider_type,
    b.bike_type