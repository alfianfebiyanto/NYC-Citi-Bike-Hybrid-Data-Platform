-- Hourly Citi Bike usage summary.
-- Grain: one hour x rider type x bike type.

{{ config(materialized='table') }}

SELECT
    f.ride_hour,
    r.rider_type,
    b.bike_type,

    COUNT(*) AS total_rides,
    ROUND(AVG(f.ride_duration_minutes), 2) AS avg_ride_duration_minutes,
    ROUND(AVG(f.trip_distance_km), 2) AS avg_trip_distance_km

FROM {{ ref('fact_trips') }} f

INNER JOIN {{ ref('dim_rider_type') }} r
    ON f.rider_type_key = r.rider_type_key

INNER JOIN {{ ref('dim_bike_type') }} b
    ON f.bike_type_key = b.bike_type_key

GROUP BY
    f.ride_hour,
    r.rider_type,
    b.bike_type