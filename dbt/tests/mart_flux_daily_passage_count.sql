-- Makes sure no passages are lost between the deduplicated int_flux_flagged table and the mart_flux_daily
select nb_passages_mart, nb_passages_flagged
from (select sum(nb_passages) as nb_passages_mart    from {{ ref('mart_flux_daily') }}),
     (select count(*)         as nb_passages_flagged from {{ ref('int_flux_flagged') }})
where nb_passages_mart <> nb_passages_flagged