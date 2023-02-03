{{ config(materialized="table") }}

with
    lokasi as (select distinct LOKASI_UNIT as LOKASI_UNIT from raw_data.raw_all_data_dbr
              where LOKASI_UNIT is not null),
    lokasi_id as (select row_number() over (order by LOKASI_UNIT asc) as LOKASI_ID, * from lokasi)

select *
from lokasi_id