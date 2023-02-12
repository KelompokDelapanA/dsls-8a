{{ config(materialized="table") }}

WITH pic AS (
              SELECT DISTINCT PIC_1_ AS `PIC_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT PIC_2_ AS `PIC_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT PIC_3_ AS `PIC_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT PIC_4_ AS `PIC_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT PIC_5_ AS `PIC_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
              UNION ALL
              SELECT DISTINCT PIC_6_ AS `PIC_NAME`
              FROM `stunning-shadow-375813.raw_data.raw_clean_dbr_egi12`
),
pic_ AS (
  SELECT TRIM (`PIC_NAME`) AS `PIC_NAME`
  FROM pic
  WHERE `PIC_NAME` IS NOT NULL OR `PIC_NAME` != ""
  GROUP BY 1
),
pic_id AS (
  SELECT row_number() over (order by pic_.`PIC_NAME` asc) AS PIC_ID,
  `PIC_NAME`
  FROM pic_
)
SELECT * FROM pic_id