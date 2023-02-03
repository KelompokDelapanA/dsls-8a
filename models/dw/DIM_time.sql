{{ config(materialized="table") }}

WITH time_ AS (
        SELECT *
        FROM UNNEST(GENERATE_TIMESTAMP_ARRAY('2022-01-01 00:00:00', '2022-01-01 23:59:00', INTERVAL 1 MINUTE)) AS `TIME`
),
time_id_ AS (
    SELECT row_number() over(ORDER BY `TIME` ASC) AS time_id,
            TIME(`TIME`) AS `TIME`, 
            EXTRACT (HOUR FROM `TIME`) AS `HOUR`,
            EXTRACT (MINUTE FROM `TIME`) AS `MINUTE`
    FROM time_
)
SELECT * FROM time_id_