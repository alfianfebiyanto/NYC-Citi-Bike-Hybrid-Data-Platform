-- Citi Bike route performance summary.
-- Grain: one start station x one end station.

SELECT
    f.start_station_key,
    s_start.station_code AS start_station_code,
    s_start.station_name AS start_station_name,

    f.end_station_key,
    s_end.station_code AS end_station_code,
    s_end.station_name AS end_station_name,

    COUNT(*) AS total_rides,
    ROUND(AVG(f.ride_duration_minutes), 2) AS avg_ride_duration_minutes,
    ROUND(AVG(f.trip_distance_km), 2) AS avg_trip_distance_km

FROM {{ ref('fact_trips') }} f

INNER JOIN {{ ref('dim_station') }} s_start
    ON f.start_station_key = s_start.station_key

INNER JOIN {{ ref('dim_station') }} s_end
    ON f.end_station_key = s_end.station_key

GROUP BY
    f.start_station_key,
    s_start.station_code,
    s_start.station_name,
    f.end_station_key,
    s_end.station_code,
    s_end.station_name