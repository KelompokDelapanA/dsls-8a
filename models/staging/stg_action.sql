
with act as (
	select action_1_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_2_ as "ACTION_NAME"
	from {{ ref('stg_activities') }} 
	union
	select action_3_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_4_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_5_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_6_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_7_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_8_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	union
	select action_9_ as "ACTION_NAME"
	from {{ ref('stg_activities') }}
	),
prob_ as (
	select TRIM("ACTION_NAME") as "ACTION_NAME" 
	from act 
	where "ACTION_NAME" is not null
		and "ACTION_NAME" != ''
	group by 1
	--order by "ACTION_NAME"
)
select  ROW_NUMBER() OVER() AS action_id, "ACTION_NAME" 
from prob_