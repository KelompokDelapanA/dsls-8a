{{ config(materialized="table") }}

with
    activity as (select distinct TRIM(UPPER(ACTIVITY_CODE)) as ACTIVITY_CODE,
                        (TRIM(UPPER(DESCRIPTION_ACTIVITY))) as DESCRIPTION_ACTIVITY 
                from raw_data.raw_all_data_dbr
                where ACTIVITY_CODE is not null OR DESCRIPTION_ACTIVITY IS NOT NULL
                ),
    activity_id as (select row_number() over() AS activity_id,
                    ACTIVITY_CODE, DESCRIPTION_ACTIVITY
                    from activity
                    )
select *
from activity_id