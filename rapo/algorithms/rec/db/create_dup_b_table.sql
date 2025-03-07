create table rapo_temp_dup_b_{process_id} as
select {parallelism} b.*, x.time_shift_group_number,
       row_number() over (partition by {keys_b}, x.time_shift_group_number order by {date_field_b}, {discrepancy_fields_b} {key_field_b}) as time_shift_group_pos_number
  from rapo_temp_fdb_{process_id} b
       left join (select b_id, listagg(a_id, '~') within group (order by a_id) as a_group_id, time_shift_group_number from rapo_temp_comb_{process_id} group by b_id, time_shift_group_number) x
       on {key_field_b} = x.b_id
 where exists (select b_id from rapo_temp_comb_{process_id} c where {key_field_b} = c.b_id and c.total_match_number_a > 1 and c.total_match_number_b > 1 and c.total_match_number_a = c.total_match_number_b)
