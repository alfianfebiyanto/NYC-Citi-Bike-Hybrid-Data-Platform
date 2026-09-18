-- Menyimpan Citi Bike trips yang tidak lolos business validation.

{{ config(
    materialized='view'
) }}

SELECT
    ride_id,
    rideable_type,
    member_casual,
    started_at,
    ended_at,
    start_station_id,
    end_station_id,

    CASE
        WHEN ride_id IS NULL THEN 'missing ride_id'
        WHEN started_at IS NULL OR ended_at IS NULL THEN 'missing timestamp'
        WHEN ended_at <= started_at THEN 'invalid trip time'
        WHEN member_casual NOT IN ('member', 'casual') OR member_casual IS NULL
            THEN 'invalid user type'
        WHEN rideable_type NOT IN ('classic_bike', 'electric_bike') OR rideable_type IS NULL
            THEN 'invalid bike type'
        WHEN start_station_id IS NULL OR end_station_id IS NULL
            THEN 'missing station'
        WHEN start_lat IS NULL OR start_lng IS NULL
            OR end_lat IS NULL OR end_lng IS NULL
            THEN 'missing coordinates'
        ELSE 'invalid coordinates'
    END AS invalid_reason

FROM {{ ref('stg_citibike_trips') }}

WHERE NOT (
    ride_id IS NOT NULL
    AND started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND ended_at > started_at
    AND member_casual IN ('member', 'casual')
    AND rideable_type IN ('classic_bike', 'electric_bike')
    AND start_station_id IS NOT NULL
    AND start_station_name IS NOT NULL
    AND end_station_id IS NOT NULL
    AND end_station_name IS NOT NULL
    AND start_lat BETWEEN -90 AND 90
    AND end_lat BETWEEN -90 AND 90
    AND start_lng BETWEEN -180 AND 180
    AND end_lng BETWEEN -180 AND 180
)