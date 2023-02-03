{{ config(materialized="table") }}

WITH date_ AS (
        SELECT *
        FROM UNNEST(GENERATE_DATE_ARRAY('2021-01-01', '2023-02-01', INTERVAL 1 DAY)) AS `DATE`
),
date_id_ AS (
    SELECT row_number() over(ORDER BY `DATE` ASC) AS date_id,
            `DATE`, 
            EXTRACT (YEAR FROM `DATE`) AS `YEAR`,
            EXTRACT (MONTH FROM `DATE`) AS `MONTH`,
            EXTRACT (WEEK FROM `DATE`) AS `WEEK`,
            EXTRACT (DAY FROM `DATE`) AS `DAY`
    FROM date_
)
SELECT * FROM date_id_