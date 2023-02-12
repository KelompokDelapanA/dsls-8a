{{ config(materialized='table') }}

with 
  dbr AS (
    SELECT 
        v.id
        ,z.site_id
        ,a.lokasi_id
        ,b.section_id
        ,c.code_unit_id
        ,d.problem_id AS problem_1_id
        ,e.problem_id AS problem_2_id
        ,f.problem_id AS problem_3_id
        ,dt1.date_id AS `START_BD_DATE_ID`
        ,t1.time_id AS `START_BD_HOUR_ID`
        ,g.wo_bd_type_id
        ,h.maintenance_type_id
        ,i.STATUS_UNIT_id
        ,a1.action_id AS action_1_id
        ,a2.action_id AS action_2_id
        ,a3.action_id AS action_3_id
        ,a4.action_id AS action_4_id
        ,a5.action_id AS action_5_id
        ,a6.action_id AS action_6_id
        ,a7.action_id AS action_7_id
        ,a8.action_id AS action_8_id
        ,a9.action_id AS action_9_id
        ,j.activity_id
        ,dt2.date_id AS `START_ACTIVITY_DATE_ID`
        ,t2.time_id AS `START_ACTIVITY_HOUR_ID`
        ,dt3.date_id AS `FINISH_ACTIVITY_DATE_ID`
        ,t3.time_id AS `FINISH_ACTIVITY_HOUR_ID`
        ,dt4.date_id AS `RFU_DATE_ID`
        ,t4.time_id AS `RFU_HOUR_ID`
        ,k.SUB_COMP_CODE AS `SUB_COMP_CODE_PA`
        ,IF(v.DESC_SUB_COMP_MTBF IS NOT NULL OR v.DESC_COMP_MTBF IS NOT NULL,k.SUB_COMP_CODE, NULL) AS `SUB_COMP_CODE_MTBF`
        ,p1.pic_id AS pic_1_id
        ,p2.pic_id AS pic_2_id
        ,p3.pic_id AS pic_3_id
        ,p4.pic_id AS pic_4_id
        ,p5.pic_id AS pic_5_id
        ,p6.pic_id AS pic_6_id
        ,v.`NO`
        ,CAST(SPLIT(REPLACE(v.HM, ',', ''),'.')[SAFE_OFFSET(0)] AS INTEGER) AS HM
        ,TRIM(v.PROBLEM_DESCRIPTION) AS PROBLEM_DESCRIPTION
        ,CAST(v.WO_NUMBER AS STRING) AS WO_NUMBER
        ,TRIM(v.ACTION_PROBLEM) AS ACTION_PROBLEM
        ,v.AGING_BD_DAY
        ,v.AGING_BD_HOUR
        ,v.AGING_ACTIVITY_DAY
        ,v.AGING_ACTIVITY_HOUR
        ,v.REMARK
    FROM raw_data.raw_clean_dbr_egi12 v
    LEFT JOIN {{ ref('DIM_site') }} AS z ON z.SITE = v.SITE
    LEFT JOIN {{ ref('DIM_location') }} AS a ON a.LOKASI_UNIT = v.LOKASI_UNIT
    LEFT JOIN {{ ref('DIM_section') }} AS b ON b.`SECTION` = v.`SECTION`
    LEFT JOIN {{ ref('DIM_code_unit_number') }} AS c ON c.code_unit_number = v.`CODE_NUMBER_UNIT`
    LEFT JOIN {{ ref('DIM_problem_egi12') }} AS d ON TRIM(UPPER(d.PROBLEM_NAME)) = TRIM(UPPER(v.PROBLEM_1_))
    LEFT JOIN {{ ref('DIM_problem_egi12') }} AS e ON TRIM(UPPER(e.PROBLEM_NAME)) = TRIM(UPPER(v.PROBLEM_2_))
    LEFT JOIN {{ ref('DIM_problem_egi12') }} AS f ON TRIM(UPPER(f.PROBLEM_NAME)) = TRIM(UPPER(v.PROBLEM_3_))
    LEFT JOIN {{ ref('DIM_date') }} AS dt1 ON DATE(dt1.`DATE`) = DATE(v.START_BD) 
    LEFT JOIN {{ ref('DIM_time') }} AS t1 ON TIME(t1.TIME) = TIME(v.START_BD) 
    LEFT JOIN {{ ref('DIM_wo_bd') }} AS g ON g.WO_BD_TYPE = TRIM(UPPER(v.WO_BD_TYPE))
    LEFT JOIN {{ ref('DIM_maintenance_type') }} AS h ON h.MAINTENANCE_TYPE = TRIM(UPPER(v.MAINTENANCE_TYPE))
    LEFT JOIN {{ ref('DIM_status_unit') }} AS i ON i.STATUS_UNIT = TRIM(UPPER(v.STATUS_UNIT))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a1 ON TRIM(UPPER(a1.ACTION_NAME)) = TRIM(UPPER(v.ACTION_1_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a2 ON TRIM(UPPER(a2.ACTION_NAME)) = TRIM(UPPER(v.ACTION_2_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a3 ON TRIM(UPPER(a3.ACTION_NAME)) = TRIM(UPPER(v.ACTION_3_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a4 ON TRIM(UPPER(a4.ACTION_NAME)) = TRIM(UPPER(v.ACTION_4_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a5 ON TRIM(UPPER(a5.ACTION_NAME)) = TRIM(UPPER(v.ACTION_5_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a6 ON TRIM(UPPER(a6.ACTION_NAME)) = TRIM(UPPER(v.ACTION_6_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a7 ON TRIM(UPPER(a7.ACTION_NAME)) = TRIM(UPPER(v.ACTION_7_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a8 ON TRIM(UPPER(a8.ACTION_NAME)) = TRIM(UPPER(v.ACTION_8_))
    LEFT JOIN {{ ref('DIM_action_egi12') }} AS a9 ON TRIM(UPPER(a9.ACTION_NAME)) = TRIM(UPPER(v.ACTION_9_))
    LEFT JOIN {{ ref('DIM_activity') }} AS j ON j.ACTIVITY_CODE = TRIM(UPPER(v.ACTIVITY_CODE)) AND
                                                j.DESCRIPTION_ACTIVITY = TRIM(UPPER(v.DESCRIPTION_ACTIVITY))
    LEFT JOIN {{ ref('DIM_date') }} AS dt2 ON DATE(dt2.`DATE`) = DATE(v.START_ACTIVITY)
    LEFT JOIN {{ ref('DIM_time') }} AS t2 ON TIME(t2.TIME) = TIME(v.START_ACTIVITY) 
    LEFT JOIN {{ ref('DIM_date') }} AS dt3 ON DATE(dt3.`DATE`) = DATE(v.FINISH_ACTIVITY) 
    LEFT JOIN {{ ref('DIM_time') }} AS t3 ON TIME(t3.TIME) = TIME(v.FINISH_ACTIVITY) 
    LEFT JOIN {{ ref('DIM_date') }} AS dt4 ON DATE(dt4.`DATE`) = DATE(v.RFU) 
    LEFT JOIN {{ ref('DIM_time') }} AS t4 ON TIME(t4.TIME) = TIME(v.RFU)
    LEFT JOIN {{ ref('DIM_sub_comp') }} AS k ON k.DESC_SUB_COMP = TRIM(UPPER(v.DESC_SUB_COMP_PA))
    LEFT JOIN {{ ref('DIM_pic') }} AS p1 ON TRIM(UPPER(p1.PIC_NAME)) = TRIM(UPPER(v.PIC_1_))
    LEFT JOIN {{ ref('DIM_pic') }} AS p2 ON TRIM(UPPER(p2.PIC_NAME)) = TRIM(UPPER(v.PIC_2_))
    LEFT JOIN {{ ref('DIM_pic') }} AS p3 ON TRIM(UPPER(p3.PIC_NAME)) = TRIM(UPPER(v.PIC_3_))
    LEFT JOIN {{ ref('DIM_pic') }} AS p4 ON TRIM(UPPER(p4.PIC_NAME)) = TRIM(UPPER(v.PIC_4_))
    LEFT JOIN {{ ref('DIM_pic') }} AS p5 ON TRIM(UPPER(p5.PIC_NAME)) = TRIM(UPPER(v.PIC_5_))
    LEFT JOIN {{ ref('DIM_pic') }} AS p6 ON TRIM(UPPER(p6.PIC_NAME)) = TRIM(UPPER(v.PIC_6_))
  )
select * from dbr