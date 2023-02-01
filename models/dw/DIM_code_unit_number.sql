{{ config(materialized='table') }}

with
    code_unit as{
        select
            row_number() over(order by egi asc) as code_unit_id
            , code_unit_number
            , egi
        from raw_data.raw_egi_cun
    }

select * from code_unit
;