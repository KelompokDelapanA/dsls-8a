with prob as (
	select problem_1 as "PROBLEM_NAME"
	from {{ ref('stg_activities') }}
	union
	select problem_2 as "PROBLEM_NAME"
	from {{ ref('stg_activities') }}
	union
	select problem_3 as "PROBLEM_NAME"
	from {{ ref('stg_activities') }}
	),
prob_ as (
	select TRIM("PROBLEM_NAME") as "PROBLEM_NAME" from prob 
	where "PROBLEM_NAME" is not null
		and "PROBLEM_NAME" != ''
	group by 1
	--order by "PROBLEM_NAME"
)
select  ROW_NUMBER() OVER() AS problem_id, "PROBLEM_NAME" 
from prob_