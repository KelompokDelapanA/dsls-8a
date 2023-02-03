{{ config(materialized="table") }}

with
    sect as (select distinct `SECTION` as `SECTION` from raw_data.raw_all_data_dbr
              where `SECTION` is not null),
    sect_id as (select row_number() over (order by `SECTION` asc) as section_id, * from sect)

select *
from sect_id