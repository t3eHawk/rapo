create table rapo_temp_t04_dup_{process_id}
nologging
as
select {parallelism} a_id, b_id from rapo_temp_t01_mod_{process_id} m
 where correlation_type = 'F'
   and exists (
         select {key_field_a} as a_id, {key_field_b} as b_id
           from rapo_temp_t03_dup_a_{process_id} a join rapo_temp_t03_dup_b_{process_id} b
                on {key_rules}
                and {date_field_a} between {date_field_b}+({time_shift_from}/86400) and {date_field_b}+({time_shift_to}/86400)
                and a.cluster_position_number = b.cluster_position_number
          where m.a_id = {key_field_a} and m.b_id = {key_field_b}
       )
