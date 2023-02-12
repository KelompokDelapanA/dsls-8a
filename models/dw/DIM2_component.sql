{{ config(materialized='table') }}

WITH 
  component_table as(
    select
      distinct component as component
    from raw_data.raw_oil
  )
  
  , comp_id as (
    select
      row_number() over(order by component asc) as comp_id
      ,*
    from component_table
  )
  
  select *
  from comp_id