{{ config(materialized='table') }}

WITH 
dim_health as(
    select
      case when health = 'Action Required' then 1
      else 0 end as is_action_required
      , health
    from raw_data.raw_oil
  )

, dim_health_id as (
    select
      row_number() over(order by health asc) as health_id
      ,*
    from dim_health
  )

select * from dim_health_id