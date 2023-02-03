{{ config(materialized="table") }}

with
    wo_bd_types as (select distinct TRIM(UPPER(WO___BD_TYPE)) as wo_bd_type from raw_data.raw_all_data_dbr
                    where WO___BD_TYPE is not null),
    wo_bd_types_id as (select row_number() over (order by wo_bd_type asc) as wo_bd_type_id, * from wo_bd_types)

select *
from wo_bd_types_id