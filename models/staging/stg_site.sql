WITH sites as (
    SELECT sa.site
    FROM project_dev.stg_activities as sa
    GROUP BY sa.site
    )
SELECT ROW_NUMBER() OVER() AS site_id, site, NULL AS flag_operate
FROM sites
