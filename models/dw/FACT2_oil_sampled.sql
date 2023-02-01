{{ config(materialized='table') }}

with 
  oil_table as(
    select
      health,
      sampled_date,
      egi,
      code_unit_number,
      component,
      meter,
      meter_on_fluid,
      case when fluid_changed = "Y" then 1 else 0 end as fluid_changed,
      Cu,
      Fe,
      Cr,
      Pb,
      Si,
      Al,
      V100,
      Na,
      PQI,
    from raw_data.raw_oil
    where egi = 'EGI 12'
  )

  , oil_table_id as(
    select
      row_number() over(order by sampled_date asc) as oil_sampled_id
      ,*
    from oil_table
  )

, oil_health as(
    select
        oil_sampled_id
        ,c.code_unit_id
        ,h.health_id
        ,n.comp_id
        ,sampled_date
        ,meter
        ,meter_on_fluid
        ,fluid_changed
        ,Cu
        ,Fe
        ,Cr
        ,Pb
        ,Si
        ,Al
        ,V100
        ,Na
        ,PQI
    from oil_table_id o
    left join {{ ref('DIM_code_unit_number') }} c on o.code_unit_number = c.code_unit_number
    left join {{ ref('DIM2_health') }} h on o.health = h.health
    left join {{ ref('DIM2_component') }} n on o.component = n.component
    
    )


select * from oil_health