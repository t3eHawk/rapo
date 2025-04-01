create table rapo_temp_dup_{process_id} as
select {parallelism} ab.*
  from rapo_temp_comb_{process_id} ab
 where exists (
         select {key_field_a} as a_id, {key_field_b} as b_id
           from rapo_temp_dup_a_{process_id} a join rapo_temp_dup_b_{process_id} b
                on {key_rules}
                and {date_field_a} between {date_field_b}+({time_shift_from}/86400) and {date_field_b}+({time_shift_to}/86400)
                and a.time_shift_group_pos_number = b.time_shift_group_pos_number
          where ab.a_id = {key_field_a} and ab.b_id = {key_field_b}
       )

union all

select {parallelism} ab.*
  from rapo_temp_comb_{process_id} ab
 where ab.total_match_number_a != ab.total_match_number_b
   and ab.time_shift_rank_a = 1 and ab.time_shift_rank_b = 1
   and ab.discrepancy_rank = 1
   and (
         (ab.match_position_a between 1 and ab.total_match_number_b-ab.total_match_number_a and ab.total_match_number_a < ab.total_match_number_b) or
         (ab.match_position_b between 1 and ab.total_match_number_a-ab.total_match_number_b and ab.total_match_number_b < ab.total_match_number_a)
       )
