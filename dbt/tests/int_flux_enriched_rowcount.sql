select dedup.nb_rows_dedup, staging.nb_rows_staging
from (select count(*) as nb_rows_dedup from {{ ref('int_flux_deduplicated') }}) as dedup, 
     (select count(distinct (trip_id, station_id)) as nb_rows_staging from {{ ref('stg_flux') }}) as staging
where dedup.nb_rows_dedup <> staging.nb_rows_staging