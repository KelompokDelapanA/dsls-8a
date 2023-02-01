{{ config(materialized='table') }}

with
    telematics as(
        select
            date
            ,code_unit
            ,boost_pressure
            ,engine_coolant_temperature
            ,engine_oil_pressure
            ,fuel_consumption_rate
            ,fuel_pressure_engine
            ,left_exhaust_temperature
            ,right_exhaust_temperature
        from `raw_data.raw_telematics_egi12`
        where (boost_pressure <> 'DSI 19') and
        (engine_coolant_temperature <> 'DSI 19') and
        (engine_oil_pressure <> 'DSI 19') and
        (fuel_consumption_rate <> 'DSI 19') and
        (fuel_pressure_engine <> 'DSI 19') and
        (left_exhaust_temperature <> 'DSI 19') and
        (right_exhaust_temperature <> 'DSI 19')
    ),

    telematic_id as(
        select 
        row_number() over(order by date asc) as telematics_id
        ,*
        from telematics
        ),

    telematic_cun as(
        select
            telematics_id
            ,date
            ,c.code_unit_id
            ,boost_pressure
            ,engine_coolant_temperature
            ,engine_oil_pressure
            ,fuel_consumption_rate
            ,fuel_pressure_engine
            ,left_exhaust_temperature
            ,right_exhaust_temperature
        from telematic_id t
        join {{ ref('DIM_code_unit_number') }} c on t.code_unit = c.code_unit_number
    )
  
select * from telematic_cun
;