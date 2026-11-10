select 
  id as disruption_period_id,
  disruption_id,
  period_start,
  period_end
from {{ source('navitia_structured', 'disruption_periods') }}