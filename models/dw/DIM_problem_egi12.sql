{{ config(materialized="table") }}

with
    prob as (
        select distinct problem_1_ as `PROBLEM_NAME`
        from `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
        union all
        select distinct problem_2_ as `PROBLEM_NAME`
        from `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
        union all
        select distinct problem_3_ as `PROBLEM_NAME`
        from `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
    ),
    prob_ as (
        select trim(`PROBLEM_NAME`) as `PROBLEM_NAME`
        from prob
        where `PROBLEM_NAME` is not null or `PROBLEM_NAME` != ""
        group by 1
    ),
    prob_id as (
        select
            row_number() over (order by prob_.`PROBLEM_NAME` asc) as problem_id,
            `PROBLEM_NAME`
        from prob_
    )
select *
from prob_id