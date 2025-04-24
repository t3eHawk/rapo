merge {parallelism} into rapo_temp_t01_mod_{process_id} m
using rapo_temp_t04_dup_{process_id} d on (m.a_id = d.a_id and m.b_id = d.b_id)
when matched then update set correlation_status = 'R', correlation_indicator = 'X'
