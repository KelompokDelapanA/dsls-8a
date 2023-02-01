WITH sections as (
    SELECT case when sa."SECTION" = 'HAULINGOB' then 'HAULING OB'
    else "SECTION" end as "SECTION"
    FROM project_dev.stg_activities as sa
    where "SECTION" is not NULL
    GROUP BY 1
    )
SELECT ROW_NUMBER() OVER() AS section_id, "SECTION"
FROM sections