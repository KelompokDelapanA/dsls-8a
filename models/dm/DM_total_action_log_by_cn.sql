{{ config(materialized="table") }}

with
    prob_ as (
        select p1.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p1
            ON p1.action_id = dbr.action_1_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p2.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p2
            ON p2.action_id = dbr.action_2_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p3.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p3
            ON p3.action_id = dbr.action_3_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p4.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p4
            ON p4.action_id = dbr.action_4_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p5.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p5
            ON p5.action_id = dbr.action_5_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p6.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p6
            ON p6.action_id = dbr.action_6_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p7.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p7
            ON p7.action_id = dbr.action_7_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p8.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p8
            ON p8.action_id = dbr.action_8_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p9.`ACTION_NAME` as `ACTION_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_action_egi12') }} p9
            ON p9.action_id = dbr.action_9_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
    )
SELECT prob_.*, COUNT(*) AS total_action
from prob_
WHERE ACTION_NAME is NOT NULL
GROUP BY prob_.`ACTION_NAME`, prob_.code_unit_number, prob_.DESC_COMP
ORDER BY total_action DESC