WITH maintenance_types as (
    SELECT sa.maintenance_type,
    	case when sa.maintenance_type = 'AC' then 'Accident'
    		when sa.maintenance_type = 'BL' then 'BL'
    		when sa.maintenance_type = 'DM' then 'Dumping/Suspensi'
    		when sa.maintenance_type = 'OH' then 'Overhaul'
    		when sa.maintenance_type = 'PS' then 'Periodic Service'
    		when sa.maintenance_type = 'RIT' then 'RIT'
    		when sa.maintenance_type = 'RP' then 'RP'
    		when sa.maintenance_type = 'RPT' then 'Repair Tyre (Tyre Leak)'
    		when sa.maintenance_type = 'WR' then 'ERROR FUEL SYSTEM'
    	else null end as maintenance_type_name
    FROM project_dev.stg_activities as sa
    GROUP BY 1
    )
SELECT ROW_NUMBER() OVER() AS maintenance_type_id, maintenance_type, maintenance_type_name
FROM maintenance_types