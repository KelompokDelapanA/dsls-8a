{{ config(materialized="table") }}

-- query oli for dashboard
with cte as (
  select oil_sampled_id,
    cn.code_unit_number, h.health, INITCAP(c.component) as component,
    CAST(sampled_date AS date) as sampled_date,
    extract(isoweek from sampled_date) as week_num,
    meter, meter_on_fluid,
    (meter - meter_on_fluid) as meter_diff,
    case when fluid_changed = 1 then 'Changed'
    when fluid_changed = 0 then 'Not Changed'
    else 'Unknown' end as fluid_changed,
    Cu, Fe, Cr, Pb, Si, Al, V100, Na, PQI
  from {{ ref('FACT2_oil_sampled')}} as o
  left join {{ ref('DIM_code_unit_number') }} as cn 
    on o.code_unit_id = cn.code_unit_id
  left join {{ ref('DIM2_health')}} as h
    on o.health_id = h.health_id
  left join  {{ ref('DIM2_component')}} as c
    on o.comp_id = c.comp_id
  where cn.egi = 'EGI 12'
    -- and c.component = 'ENGINE'
    and sampled_date between '2022-01-01' and '2022-12-31'
  order by sampled_date, oil_sampled_id
)

select * from cte
-- select count(*) from cte
-- select code_unit_number, count(*) as count from cte group by 1 order by 2 desc
-- select min(cte.sampled_date), max(sampled_date) from cte
-- select fluid_changed, count(*) from cte group by 1 order by 2 desc