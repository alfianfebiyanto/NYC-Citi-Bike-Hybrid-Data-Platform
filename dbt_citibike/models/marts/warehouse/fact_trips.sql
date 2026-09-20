-- Fact perjalanan Citi Bike.
-- Grain: satu row untuk satu ride_id.

{{ config(
    partition_by={
        "field": "date_key",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    t.ride_id,

    -- Dimension keys
    t.trip_date AS date_key,
    s_start.station_key AS start_station_key,
    s_end.station_key AS end_station_key,
    r.rider_type_key,
    b.bike_type_key,

    -- Trip timestamps
    t.started_at,
    t.ended_at,

    -- Trip metrics
    t.ride_duration_minutes,
    t.trip_distance_km,

    -- Time attributes
    t.ride_hour,
    t.is_weekend,
    t.time_of_day

FROM {{ ref('int_citibike_trips_clean') }} t

LEFT JOIN {{ ref('dim_station') }} s_start
    ON t.start_station_id = s_start.station_code

LEFT JOIN {{ ref('dim_station') }} s_end
    ON t.end_station_id = s_end.station_code

LEFT JOIN {{ ref('dim_rider_type') }} r
    ON t.member_casual = r.rider_type

LEFT JOIN {{ ref('dim_bike_type') }} b
    ON t.rideable_type = b.bike_type