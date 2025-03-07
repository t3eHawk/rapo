create table rapo_temp_dup_a_{process_id} as
select {parallelism} a.*, x.time_shift_group_number,
       row_number() over (partition by {keys_a}, x.time_shift_group_number order by {date_field_a}, {discrepancy_fields_a} {key_field_a}) as time_shift_group_pos_number
  from rapo_temp_fda_{process_id} a
       left join (select a_id, listagg(b_id, '~') within group (order by b_id) as b_group_id, time_shift_group_number from rapo_temp_comb_{process_id} group by a_id, time_shift_group_number) x
       on {key_field_a} = x.a_id
 where exists (select a_id from rapo_temp_comb_{process_id} c where {key_field_a} = c.a_id and c.total_match_number_a > 1 and c.total_match_number_b > 1 and c.total_match_number_a = c.total_match_number_b)
