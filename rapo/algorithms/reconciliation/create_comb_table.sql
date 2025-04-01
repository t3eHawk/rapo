create table rapo_temp_comb_{process_id}
nologging
as
select {parallelism}
       a_id,
       b_id,
       time_shift_group_number,
       time_shift_value,
       time_shift_rank_a,
       time_shift_rank_b,
       {discrepancy_fields}
       discrepancy_rank,
       discrepancy_time_a,
       discrepancy_time_b,
       discrepancy_time_value,
       row_number() over (partition by key_value, time_shift_group_number, b_id order by discrepancy_pos_b, time_shift_rank_b) as match_position_a,
       row_number() over (partition by key_value, time_shift_group_number, a_id order by discrepancy_pos_a, time_shift_rank_a) as match_position_b,
       count(distinct b_id) over (partition by key_value, time_shift_group_number) as total_match_number_a,
       count(distinct a_id) over (partition by key_value, time_shift_group_number) as total_match_number_b
  from (
         select {parallelism}
                a_id,
                b_id,
                key_value,
                date_value,
                date_value_a,
                date_value_b,
                sum(time_shift_reset) over (partition by key_value order by date_value) as time_shift_group_number,
                time_shift_value,
                time_shift_rank_a,
                time_shift_rank_b,
                {discrepancy_formulas}
                dense_rank() over (partition by a_id order by {discrepancy_sums}) as discrepancy_rank,
                row_number() over (partition by a_id order by {discrepancy_sums}) as discrepancy_pos_a,
                row_number() over (partition by b_id order by {discrepancy_sums}) as discrepancy_pos_b,
                discrepancy_time_a,
                discrepancy_time_b,
                discrepancy_time_value
           from (
                  select {parallelism}
                         {key_field_a} as a_id,
                         {key_field_b} as b_id,
                         {key_value} as key_value,
                         greatest({date_field_a}, {date_field_b}) as date_value,
                         {date_field_a} as date_value_a,
                         {date_field_b} as date_value_b,
                         case
                              when lag(greatest({date_field_a}, {date_field_b})) over (partition by {key_partition} order by greatest({date_field_a}, {date_field_b}))
                                   between greatest({date_field_a}, {date_field_b})+({time_shift_from}/86400) and greatest({date_field_a}, {date_field_b})+({time_shift_to}/86400) then 0
                              else 1
                         end as time_shift_reset,
                         86400*({date_field_a}-{date_field_b}) as time_shift_value,
                         dense_rank() over (partition by {key_field_a} order by abs(86400*({date_field_a}-{date_field_b}))) as time_shift_rank_a,
                         dense_rank() over (partition by {key_field_b} order by abs(86400*({date_field_a}-{date_field_b}))) as time_shift_rank_b,
                         {discrepancy_rules}
                         case when 86400*({date_field_a}-{date_field_b}) not between {time_shift_from} and {time_shift_to} then '{date_field_name_a}' end as discrepancy_time_a,
                         case when 86400*({date_field_a}-{date_field_b}) not between {time_shift_from} and {time_shift_to} then '{date_field_name_b}' end as discrepancy_time_b,
                         86400*({date_field_a}-{date_field_b}) as discrepancy_time_value
                    from rapo_temp_fda_{process_id} a join rapo_temp_fdb_{process_id} b
                         on {key_rules}
                         and {date_rules}
                )
       )
