{{ config(materialized='table') }}

with
    code_unit as(
        select
            code_unit_number
            , egi
        from raw_data.raw_egi_cun
        group by 1, 2
        order by 2
        )
    
    , code_unit_id as(
        select
            *
            , row_number() over(order by egi asc) as code_unit_id
        from code_unit
        )

select * from code_unit_id
;