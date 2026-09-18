-- Enrichment data realtime station status dari GBFS.

{{ config(
    materialized='view'
) }}

SELECT
    *,

    -- Total kapasitas yang sedang terpantau
    num_bikes_available
        + num_bikes_disabled
        + num_docks_available
        + num_docks_disabled AS available_capacity,

    -- Kondisi station
    num_bikes_available = 0 AS is_empty,
    num_docks_available = 0 AS is_full

FROM {{ ref('stg_station_status') }}

WHERE
    station_id IS NOT NULL
    AND snapshot_timestamp IS NOT NULL
    AND num_bikes_available >= 0
    AND num_ebikes_available >= 0
    AND num_docks_available >= 0
    AND num_bikes_disabled >= 0
    AND num_docks_disabled >= 0