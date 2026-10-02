-- Data-quality test untuk memastikan metrik agregasi perjalanan tidak bernilai negatif.
-- dbt akan menganggap test gagal jika query ini menghasilkan satu atau lebih baris.

select *
from {{ ref('mart_daily_rides') }}

-- Ambil hanya baris yang mengandung nilai metrik tidak valid.
where total_rides < 0
   or avg_ride_duration_minutes < 0
   or avg_trip_distance_km < 0