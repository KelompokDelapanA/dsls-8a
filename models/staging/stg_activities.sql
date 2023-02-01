
--- INITIAL CLEAN DATA FROM RAW DATA

with
    grouped_table as (
        select
            fa.*,
            count(if(`NO` = '', null, `NO`)) over (order by id) as _grp,
            if
            (`NO` = '', null, `NO`) as null_no
        from fact_activities fa
    ),
    final_table as (
        select
            grouped_table.*,
            first_value(null_no) over (partition by _grp order by id) as ffill_no
        from grouped_table
    ),
    raw_activities as (
        select
            id,
            site,
            `MONTH`,
            `LOKASI UNIT`,
            trim(section) as section,
            ffill_no as `NO`,
            egi,
            `CODE NUMBER UNIT`,
            hm,
            `PROBLEM DESCRIPTION`,
            trim(
                if(
                    `PROBLEM DESCRIPTION` like '%TYRE%NO%',
                    `PROBLEM DESCRIPTION`,
                    upper(
                        substring_index(
                            substring_index(
                                substring_index(
                                    substring_index(`PROBLEM DESCRIPTION`, '+', 1),
                                    ',',
                                    1
                                ),
                                ',',
                                -1
                            ),
                            '+',
                            -1
                        )
                    )
                )
            ) as problem_1,
            trim(
                if(
                    `PROBLEM DESCRIPTION` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(
                                substring_index(
                                    substring_index(`PROBLEM DESCRIPTION`, '+', 2),
                                    ',',
                                    2
                                ),
                                ',',
                                -1
                            ),
                            '+',
                            -1
                        )
                    )
                )
            ) as problem_2,
            trim(
                if(
                    `PROBLEM DESCRIPTION` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(
                                substring_index(
                                    substring_index(`PROBLEM DESCRIPTION`, '+', 3),
                                    ',',
                                    3
                                ),
                                ',',
                                -1
                            ),
                            '+',
                            -1
                        )
                    )
                )
            ) as problem_3,
            `START B/D (DATE)`,
            `START B/D (TIME)`,
            if(
                `WO NUMBER` = '300500023435',
                `START B/D (DATE)`,
                # # ADD COLUMN START B/D DATE
                concat(`START B/D (DATE)`, ' ', `START B/D (TIME)`)
            ) as `START B/D`,
            `WO NUMBER`,
            `WO / BD TYPE`,
            `MAINTENANCE TYPE`,
            `STATUS UNIT`,
            `ACTION PROBLEM`,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    `ACTION PROBLEM`,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 1), ',', -1
                        )
                    )
                )
            ) as action_1,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 2), ',', -1
                        )
                    )
                )
            ) as action_2,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 3), ',', -1
                        )
                    )
                )
            ) as action_3,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 4), ',', -1
                        )
                    )
                )
            ) as action_4,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 5), ',', -1
                        )
                    )
                )
            ) as action_5,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 6), ',', -1
                        )
                    )
                )
            ) as action_6,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 7), ',', -1
                        )
                    )
                )
            ) as action_7,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 8), ',', -1
                        )
                    )
                )
            ) as action_8,
            trim(
                if(
                    `ACTION PROBLEM` like '%TYRE%NO%',
                    null,
                    upper(
                        substring_index(
                            substring_index(`ACTION PROBLEM`, ',', 9), ',', -1
                        )
                    )
                )
            ) as action_9,
            `ACTIVITY CODE`,
            `DESCRIPTION ACTIVITY`,
            `START ACTIVITY (DATE)`,
            `START ACTIVITY (TIME)`,
            if(
                `WO NUMBER` = '300500023435',
                `START ACTIVITY (DATE)`,
                # # ADD COLUMN START ACTIVITY DATE
                concat(`START ACTIVITY (DATE)`, ' ', `START ACTIVITY (TIME)`)
            ) as `START ACTIVITY`,
            `FINISH ACTIVITY (DATE)`,
            `FINISH ACTIVITY (TIME)`,
            if(
                `WO NUMBER` = '300500023435',
                `FINISH ACTIVITY (DATE)`,
                # # ADD COLUMN FINISH ACTIVITY DATE
                concat(`FINISH ACTIVITY (DATE)`, ' ', `FINISH ACTIVITY (TIME)`)
            ) as `FINISH ACTIVITY`,
            `RFU (DATE)`,
            `RFU (TIME)`,
            if(
                `WO NUMBER` = '300500023435',
                `RFU (DATE)`,
                concat(`RFU (DATE)`, ' ', `RFU (TIME)`)  # # ADD COLUMN RFU DATE
            ) as `RFU`,
            `AGING B/D (DAY)`,
            `AGING B/D (HOUR)`,
            `AGING ACTIVITY (DAY)`,
            `AGING ACTIVITY (HOUR)`,
            `SUB COMP CODE`,
            `COMP CODE`,
            trim(upper(`DESCRIPTION SUB COMP (PA)`)) as `DESCRIPTION SUB COMP (PA)`,
            trim(upper(`DESRICTION COMP (PA)`)) as `DESRICTION COMP (PA)`,
            trim(upper(`DESCRIPTION SUB COMP (MTBF)`)) as `DESCRIPTION SUB COMP (MTBF)`,
            trim(upper(`DESRICTION COMP (MTBF)`)) as `DESRICTION COMP (MTBF)`,
            `PIC MEKANIK / GROUP LEADER`,
            trim(
                upper(
                    substring_index(
                        substring_index(`PIC MEKANIK / GROUP LEADER`, ',', 1), ',', -1
                    )
                )
            ) as pic_1,
            trim(
                upper(
                    substring_index(
                        substring_index(`PIC MEKANIK / GROUP LEADER`, ',', 2), ',', -1
                    )
                )
            ) as pic_2,
            trim(
                upper(
                    substring_index(
                        substring_index(`PIC MEKANIK / GROUP LEADER`, ',', 3), ',', -1
                    )
                )
            ) as pic_3,
            trim(
                upper(
                    substring_index(
                        substring_index(`PIC MEKANIK / GROUP LEADER`, ',', 4), ',', -1
                    )
                )
            ) as pic_4,
            trim(
                upper(
                    substring_index(
                        substring_index(`PIC MEKANIK / GROUP LEADER`, ',', 5), ',', -1
                    )
                )
            ) as pic_5,
            trim(
                upper(
                    substring_index(
                        substring_index(`PIC MEKANIK / GROUP LEADER`, ',', 6), ',', -1
                    )
                )
            ) as pic_6,
            `RO / PR NUMBER`,
            `RO / PR (DATE)`,
            `RO / PR (STATUS)`,
            `PO NUMBER`,
            `PO (DATE)`,
            `PO (STATUS)`,
            `REMARK`
        from final_table
        where egi = 'EGI 12'  # # FILTER EGI
    ),
    raw_activities_ as (
        select
            id,
            site,
            `MONTH`,
            `LOKASI UNIT` as lokasi_unit,
            section,
            `NO`,
            egi,
            `CODE NUMBER UNIT` as `CODE_NUMBER_UNIT`,
            hm,
            `PROBLEM DESCRIPTION` as problem_description,
            `PROBLEM DESCRIPTION` as prob,
            problem_1 as problem_1,
            if(problem_1 = problem_2, null, problem_2) as problem_2,
            if(problem_2 = problem_3, null, problem_3) as problem_3,
            `START B/D (DATE)` as `START_BD_DATE`,
            `START B/D (TIME)` as `START_BD_TIME`,
            `START B/D` as `START_BD`,
            `WO NUMBER` as `WO_NUMBER`,
            `WO / BD TYPE` as wo_bd_type,
            `MAINTENANCE TYPE` as maintenance_type,
            `STATUS UNIT` as status_unit,
            `ACTION PROBLEM` as action_problem,
            action_1 as action_1_,
            if(action_1 = action_2, null, action_2) as action_2_,
            if(action_2 = action_3, null, action_3) as action_3_,
            if(action_3 = action_4, null, action_4) as action_4_,
            if(action_4 = action_5, null, action_5) as action_5_,
            if(action_5 = action_6, null, action_6) as action_6_,
            if(action_6 = action_7, null, action_7) as action_7_,
            if(action_7 = action_8, null, action_8) as action_8_,
            if(action_8 = action_9, null, action_9) as action_9_,
            `ACTIVITY CODE` as activity_code,
            `DESCRIPTION ACTIVITY` as description_activity,
            `START ACTIVITY (DATE)` as start_activity_date,
            `START ACTIVITY (TIME)` as start_activity_time,
            `START ACTIVITY` as start_activity,  # # ADD COLUMN START ACTIVITY DATE
            `FINISH ACTIVITY (DATE)` as finish_activity_date,
            `FINISH ACTIVITY (TIME)` as finish_activity_time,
            `FINISH ACTIVITY` as finish_activity,  # # ADD COLUMN FINISH ACTIVITY DATE
            `RFU (DATE)` as rfu_date,
            `RFU (TIME)` as rfu_time,
            `RFU`,  # # ADD COLUMN RFU DATE
            `AGING B/D (DAY)` as aging_bd_day,
            `AGING B/D (HOUR)` as aging_bd_hour,
            `AGING ACTIVITY (DAY)` as aging_activity_day,
            `AGING ACTIVITY (HOUR)` as aging_activity_hour,
            `SUB COMP CODE` as sub_comp_code,
            `COMP CODE` as comp_code,
            `DESCRIPTION SUB COMP (PA)` as desc_sub_comp_pa,
            `DESRICTION COMP (PA)` as desc_comp_pa,
            `DESCRIPTION SUB COMP (MTBF)` as desc_sub_comp_mtbf,
            `DESRICTION COMP (MTBF)` as desc_comp_mtbf,
            `PIC MEKANIK / GROUP LEADER` as pic,
            pic_1 as pic_1_,
            if(pic_1 = pic_2, null, pic_2) as pic_2_,
            if(pic_2 = pic_3, null, pic_3) as pic_3_,
            if(pic_3 = pic_4, null, pic_4) as pic_4_,
            if(pic_4 = pic_5, null, pic_5) as pic_5_,
            if(pic_5 = pic_6, null, pic_6) as pic_6_,
            `REMARK`
        from raw_activities
        where `ACTIVITY CODE` is not null  # # TERDAPAT 1 NULL
    )
select *
from raw_activities_
order by start_bd, start_activity, code_number_unit
;
