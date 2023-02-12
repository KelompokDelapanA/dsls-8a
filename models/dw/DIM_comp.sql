{{ config(materialized="table") }}

with
    comps as (select COMP_CODE, TRIM(UPPER(DESRICTION_COMP__PA_)) as DESC_COMP from `raw_data.raw_all_data_dbr` 
            where DESRICTION_COMP__PA_ IS NOT NULL
            group by 1,2)

select *
from comps