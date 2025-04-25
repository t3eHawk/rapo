merge {parallelism} into rapo_temp_t02_org_b_{process_id} b
using rapo_temp_t04_dup_{process_id} d on (b.b_id = d.b_id)
when matched then update set correlation_status = 'R', correlation_indicator = 'X'
