{{ config(materialized="table") }}

with
    sub_comps as (select SUB_COMP_CODE, TRIM(UPPER(DESCRIPTION_SUB_COMP__PA_)) as DESC_SUB_COMP , 
                    COMP_CODE 
                    from `raw_data.raw_all_data_dbr` 
                    where DESCRIPTION_SUB_COMP__PA_ IS NOT NULL
                    group by 1,2,3),
    join_comps as (select SUB_COMP_CODE, DESC_SUB_COMP, comp.COMP_CODE, comp.DESC_COMP
                   FROM sub_comps sc
                   LEFT JOIN {{ ref('DIM_comp')}} comp ON comp.COMP_CODE = sc.COMP_CODE)
select *
from join_comps