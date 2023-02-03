{{ config(materialized="table") }}

with
    maintenance_types as (select distinct TRIM(UPPER(maintenance_type)) as maintenance_type from raw_data.raw_all_data_dbr
                        where maintenance_type is not null),
    maintenance_types_id as (select row_number() over (order by maintenance_type asc) as maintenance_type_id, * from maintenance_types)

select *
from maintenance_types_id