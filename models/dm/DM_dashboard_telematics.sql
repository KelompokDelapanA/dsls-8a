{{ config(materialized="table") }}

--query for telematics dashboard
with a as (
    SELECT `date` AS  `DATETIME`, cn.code_unit_number, cn.egi, DATE(`date`) AS `DATE`, TIME(`date`) AS `TIME`,
        extract(DAY from `date`)  AS `DAY`, extract(MONTH from `date`) AS `MONTH`, extract(YEAR from `date`) AS `YEAR`,
        extract(ISOWEEK from `date`) as week_num,
        boost_pressure, engine_coolant_temperature, engine_oil_pressure, fuel_consumption_rate,
        fuel_pressure_engine, left_exhaust_temperature, right_exhaust_temperature 
    FROM {{ ref('FACT3_telematics')}} t
    LEFT JOIN {{ ref('DIM_code_unit_number')}} cn
        ON cn.code_unit_id = t.code_unit_id
)
SELECT * FROM a
limit 500
/* limit added automatically by dbt cloud */