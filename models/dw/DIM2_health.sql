{{ config(materialized='table') }}

WITH 
    uni_health as(
        select
            distinct health as health
        from raw_data.raw_oil
    )

    , dim_health as(
        select
        case when health = 'Action Required' then 1
        else 0 end as is_action_required
        , health
        from uni_health
    )

    , dim_health_id as (
        select
        row_number() over(order by health asc) as health_id
        ,*
        from dim_health
    )

select * from dim_health_id