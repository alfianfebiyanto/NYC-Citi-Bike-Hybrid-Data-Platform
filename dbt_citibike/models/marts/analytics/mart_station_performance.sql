WITH departures AS (

    SELECT
        date_key AS ride_date,
        start_station_key AS station_key,
        COUNT(*) AS total_departures

    FROM {{ ref('fact_trips') }}

    WHERE start_station_key IS NOT NULL

    GROUP BY
        date_key,
        start_station_key

),

arrivals AS (

    SELECT
        date_key AS ride_date,
        end_station_key AS station_key,
        COUNT(*) AS total_arrivals

    FROM {{ ref('fact_trips') }}

    WHERE end_station_key IS NOT NULL

    GROUP BY
        date_key,
        end_station_key

),

station_dates AS (

    SELECT
        ride_date,
        station_key
    FROM departures

    UNION DISTINCT

    SELECT
        ride_date,
        station_key
    FROM arrivals

)

SELECT
    sd.ride_date,
    s.station_key,
    s.station_code,
    s.station_name,
    s.latitude,
    s.longitude,

    COALESCE(d.total_departures, 0) AS total_departures,
    COALESCE(a.total_arrivals, 0) AS total_arrivals,

    COALESCE(d.total_departures, 0)
        + COALESCE(a.total_arrivals, 0) AS total_trip_activity

FROM station_dates sd

LEFT JOIN {{ ref('dim_station') }} s
    ON sd.station_key = s.station_key

LEFT JOIN departures d
    ON sd.ride_date = d.ride_date
    AND sd.station_key = d.station_key

LEFT JOIN arrivals a
    ON sd.ride_date = a.ride_date
    AND sd.station_key = a.station_key