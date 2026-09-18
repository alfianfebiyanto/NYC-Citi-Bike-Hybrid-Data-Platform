-- Fact status station Citi Bike realtime.
-- Grain: satu station pada satu snapshot waktu.

{{ config(
    materialized='incremental',
    unique_key=['station_key', 'snapshot_timestamp'],
    incremental_strategy='merge',
    partition_by={
        "field": "snapshot_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    d.station_key,

    -- Snapshot
    s.snapshot_timestamp,
    DATE(s.snapshot_timestamp) AS snapshot_date,

    -- Availability
    s.num_bikes_available,
    s.num_ebikes_available,
    s.num_docks_available,
    s.num_bikes_disabled,
    s.num_docks_disabled,
    s.available_capacity,

    -- Station status
    s.is_installed,
    s.is_renting,
    s.is_returning,
    s.is_empty,
    s.is_full,

    -- Source timestamps
    s.last_reported,
    s._ingested_at

FROM {{ ref('int_station_status') }} s

INNER JOIN {{ ref('dim_station') }} d
    ON s.station_id = d.gbfs_station_id

{% if is_incremental() %}

WHERE s.snapshot_timestamp > (
    SELECT COALESCE(MAX(snapshot_timestamp), TIMESTAMP('1970-01-01'))
    FROM {{ this }}
)

{% endif %}