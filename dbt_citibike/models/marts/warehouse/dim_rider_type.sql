-- Dimensi tipe pengguna Citi Bike.

{{ config(materialized='table') }}

SELECT
    1 AS rider_type_key,
    'member' AS rider_type

UNION ALL

SELECT
    2 AS rider_type_key,
    'casual' AS rider_type