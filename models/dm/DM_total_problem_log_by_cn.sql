{{ config(materialized="table") }}

with
    prob_ as (
        select p1.`PROBLEM_NAME` as `PROBLEM_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_problem_egi12') }} p1
            ON p1.problem_id = dbr.problem_1_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p2.`PROBLEM_NAME` as `PROBLEM_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_problem_egi12') }} p2
            ON p2.problem_id = dbr.problem_2_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
        union all
        select p3.`PROBLEM_NAME` as `PROBLEM_NAME`, c.code_unit_number, sc.DESC_COMP
        from {{ ref('FACT_dbr_engine_egi12') }} dbr
        left join {{ ref('DIM_problem_egi12') }} p3
            ON p3.problem_id = dbr.problem_3_id
        LEFT JOIN {{ ref('DIM_code_unit_number')}} c
        ON c.code_unit_id = dbr.code_unit_id
        LEFT JOIN {{ ref('DIM_sub_comp')}} sc
        ON sc.SUB_COMP_CODE = dbr.SUB_COMP_CODE_PA
    )
SELECT prob_.*, COUNT(*) AS total_problem
from prob_
WHERE PROBLEM_NAME is NOT NULL
GROUP BY prob_.`PROBLEM_NAME`, prob_.code_unit_number, prob_.DESC_COMP