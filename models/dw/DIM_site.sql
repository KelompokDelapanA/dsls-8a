{{ config(materialized="table") }}

with
    sites as (select distinct site as SITE from raw_data.raw_all_data_dbr
              where site is not null),
    sites_id as (select row_number() over (order by site asc) as SITE_ID, * from sites)

select *
from sites_id