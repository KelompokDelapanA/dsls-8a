{{ config(materialized='table') }}

with
    telematic as{
        select
            'date' as 'date'
            ,'boost_pressure'
            ,'engine_coolant_temperature'
            ,'engine_oil_pressure'
            ,'fuel_consumption_rate'
            ,'fuel_pressure_engine'
            ,'left_exhaust_temperature'
            ,'right_exhaust_temperature'
            ,row_number() over(order by 'date' asc) as 'telematic_id'
        from raw_data.raw_telematic
    }

select * from telematic
;