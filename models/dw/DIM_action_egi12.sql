{{ config(materialized="table") }}

WITH act AS (
              SELECT DISTINCT ACTION_1_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_2_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_3_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_4_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_5_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_6_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_7_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_8_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT ACTION_9_ AS `ACTION_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
),
act_ AS (
  SELECT TRIM (`ACTION_NAME`) AS `ACTION_NAME`
  FROM act
  WHERE `ACTION_NAME` IS NOT NULL OR `ACTION_NAME` != ""
  GROUP BY 1
),
act_id AS (
  SELECT row_number() over (order by act_.`ACTION_NAME` asc) AS ACTION_ID,
  `ACTION_NAME`
  FROM act_
)
SELECT * FROM act_id