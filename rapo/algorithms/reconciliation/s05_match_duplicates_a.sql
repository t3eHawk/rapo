merge {parallelism} into rapo_temp_t02_org_a_{process_id} a
using rapo_temp_t04_dup_{process_id} d on (a.a_id = d.a_id)
when matched then update set correlation_status = 'R', correlation_indicator = 'X'
