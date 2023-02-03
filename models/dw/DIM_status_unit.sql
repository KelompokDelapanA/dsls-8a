{{ config(materialized="table") }}

with
    STATUS_UNIT_ as (select distinct TRIM(UPPER(STATUS_UNIT)) as STATUS_UNIT from raw_data.raw_all_data_dbr
              where STATUS_UNIT is not null),
    STATUS_UNIT_ID as (select row_number() over (order by STATUS_UNIT asc) as STATUS_UNIT_id, * from STATUS_UNIT_)

select *
from STATUS_UNIT_ID