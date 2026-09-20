-- Latest Citi Bike station availability.
-- Grain: one row per station.

SELECT
    f.station_key,
    s.station_code,
    s.station_name,
    s.latitude,
    s.longitude,
    s.capacity,

    f.num_bikes_available,
    f.num_ebikes_available,
    f.num_docks_available,
    f.num_bikes_disabled,
    f.num_docks_disabled,

    f.is_installed,
    f.is_renting,
    f.is_returning,
    f.is_empty,
    f.is_full,

    f.last_reported,
    f.snapshot_timestamp

FROM {{ ref('fact_station_status') }} f

INNER JOIN {{ ref('dim_station') }} s
    ON f.station_key = s.station_key

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY f.station_key
    ORDER BY f.snapshot_timestamp DESC
) = 1