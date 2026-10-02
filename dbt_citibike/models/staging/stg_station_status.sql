-- Staging data realtime GBFS station_status.
-- Melakukan standardisasi field dan tipe data dari RAW layer.

SELECT
    -- Identitas station
    CAST(station_id AS STRING) AS station_id,

    -- Ketersediaan station
    SAFE_CAST(num_bikes_available AS INT64) AS num_bikes_available,
    SAFE_CAST(num_ebikes_available AS INT64) AS num_ebikes_available,
    SAFE_CAST(num_docks_available AS INT64) AS num_docks_available,
    SAFE_CAST(num_bikes_disabled AS INT64) AS num_bikes_disabled,
    SAFE_CAST(num_docks_disabled AS INT64) AS num_docks_disabled,

    -- Status operasional
    CAST(is_installed AS BOOL) AS is_installed,
    CAST(is_renting AS BOOL) AS is_renting,
    CAST(is_returning AS BOOL) AS is_returning,

    -- Waktu dan metadata
    CAST(last_reported AS TIMESTAMP) AS last_reported,
    CAST(snapshot_timestamp AS TIMESTAMP) AS snapshot_timestamp,
    CAST(_ingested_at AS TIMESTAMP) AS _ingested_at

FROM {{ source('raw', 'station_status') }}