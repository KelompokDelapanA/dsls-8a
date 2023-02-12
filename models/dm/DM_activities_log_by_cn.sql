{{ config(materialized="table") }}

WITH grouped AS
(SELECT s.site
      ,c.code_unit_number
      ,sec.section
      ,HM
      ,WO_NUMBER
      ,w.wo_bd_type
      ,m.maintenance_type
      ,stat.STATUS_UNIT
      ,act.DESCRIPTION_ACTIVITY
      ,sc.DESC_SUB_COMP AS DESC_SUB_COMP_PA
      ,sc.DESC_COMP AS DESC_COMP_PA
      ,sc1.DESC_SUB_COMP AS DESC_SUB_COMP_MTBF
      ,sc1.DESC_COMP AS DESC_COMP_MTBF
      ,DATETIME(CONCAT(dt.DATE, ' ', t.TIME)) AS start_bd
      ,DATETIME(CONCAT(dt3.DATE, ' ', t3.TIME)) AS finish_bd_rfu
      ,DATETIME(CONCAT(dt1.DATE, ' ', t1.TIME)) AS start_working
      ,DATETIME(CONCAT(dt2.DATE, ' ', t2.TIME)) AS finish_working
FROM {{ ref('FACT_dbr_engine_egi12') }} dbr
LEFT JOIN {{ ref('DIM_site')}} s
  ON s.site_id = dbr.site_id
LEFT JOIN {{ ref('DIM_activity')}} act
  ON act.activity_id = dbr.activity_id
LEFT JOIN {{ ref('DIM_status_unit')}} stat
  ON stat.STATUS_UNIT_id = dbr.STATUS_UNIT_id
LEFT JOIN {{ ref('DIM_maintenance_type')}} m
  ON m.maintenance_type_id = dbr.maintenance_type_id
LEFT JOIN {{ ref('DIM_code_unit_number')}} c
  ON c.code_unit_id = dbr.code_unit_id
LEFT JOIN {{ ref('DIM_wo_bd')}} w
  ON w.wo_bd_type_id = dbr.wo_bd_type_id
LEFT JOIN {{ ref('DIM_section')}} sec
  ON sec.section_id = dbr.section_id
LEFT JOIN {{ ref('DIM_date')}} dt
  ON dt.date_id = dbr.START_BD_DATE_ID
LEFT JOIN {{ ref('DIM_time')}} t
  ON t.time_id = dbr.START_BD_HOUR_ID
LEFT JOIN {{ ref('DIM_date')}} dt1
  ON dt1.date_id = dbr.START_ACTIVITY_DATE_ID
LEFT JOIN {{ ref('DIM_time')}} t1
  ON t1.time_id = dbr.START_ACTIVITY_HOUR_ID
LEFT JOIN {{ ref('DIM_date')}} dt2
  ON dt2.date_id = dbr.FINISH_ACTIVITY_DATE_ID
LEFT JOIN {{ ref('DIM_time')}} t2
  ON t2.time_id = dbr.FINISH_ACTIVITY_HOUR_ID
LEFT JOIN {{ ref('DIM_date')}} dt3
  ON dt3.date_id = dbr.RFU_DATE_ID
LEFT JOIN {{ ref('DIM_time')}} t3
  ON t3.time_id = dbr.RFU_HOUR_ID
LEFT JOIN {{ ref('DIM_sub_comp')}} sc
  ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
LEFT JOIN {{ ref('DIM_sub_comp')}} sc1
  ON sc1.SUB_COMP_CODE = dbr.SUB_COMP_CODE_MTBF
ORDER BY code_unit_number, HM, start_bd)
SELECT grouped.*
      ,IF(grouped.finish_bd_rfu < grouped.start_bd, 
      SAFE_CAST(DATE_DIFF(DATETIME(grouped.finish_bd_rfu) + interval 1 day, DATETIME(grouped.start_bd), SECOND)/3600 AS FLOAT64),
      SAFE_CAST(DATE_DIFF(DATETIME(grouped.finish_bd_rfu), DATETIME(grouped.start_bd), SECOND)/3600 AS FLOAT64)) AS AGING_BD_HOUR
      ,IF(grouped.finish_working < grouped.start_working,
      SAFE_CAST(DATE_DIFF(DATETIME(grouped.finish_working) + interval 1 day, DATETIME(grouped.start_working), SECOND)/3600 AS FLOAT64),
      SAFE_CAST(DATE_DIFF(DATETIME(grouped.finish_working), DATETIME(grouped.start_working), SECOND)/3600 AS FLOAT64)) AS AGING_ACTIVITY_HOUR
FROM grouped