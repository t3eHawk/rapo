create table rapo_temp_t01_mod_{process_id}
nologging
as
select {parallelism}
       a_id,
       b_id,
       cluster_id,
       time_shift_value,
       time_shift_rank_a,
       time_shift_rank_b,
       {discrepancy_fields}
       discrepancy_rank_a,
       discrepancy_rank_b,
       discrepancy_time_a,
       discrepancy_time_b,
       discrepancy_time_value,
       total_match_number_a,
       total_match_number_b,
       least(total_match_number_a, total_match_number_b)/greatest(total_match_number_a, total_match_number_b) as correlation_coefficient,
       case
            when total_match_number_a = 1 and total_match_number_b = 1 then 'O'
            when total_match_number_a > 1 and total_match_number_b > 1 and total_match_number_a = total_match_number_b then 'F'
            when total_match_number_a = 1 and total_match_number_a < total_match_number_b then 'A'
            when total_match_number_b = 1 and total_match_number_a > total_match_number_b then 'B'
            when total_match_number_a > 1 and total_match_number_b > 1 and total_match_number_a != total_match_number_b then 'M'
       end as correlation_type,
       case when total_match_number_a = 1 and total_match_number_b = 1 then 'R' end as correlation_status,
       case when total_match_number_a = 1 and total_match_number_b = 1 then 'X' end as correlation_indicator
  from (
         select {parallelism}
                a_id,
                b_id,
                hash_value,
                date_value,
                cluster_id,
                time_shift_value,
                time_shift_rank_a,
                time_shift_rank_b,
                {discrepancy_fields}
                discrepancy_rank_a,
                discrepancy_rank_b,
                discrepancy_time_a,
                discrepancy_time_b,
                discrepancy_time_value,
                count(distinct b_id) over (partition by hash_value, cluster_id) as total_match_number_a,
                count(distinct a_id) over (partition by hash_value, cluster_id) as total_match_number_b
           from (
                  select {parallelism}
                         a_id,
                         b_id,
                         hash_value,
                         date_value,
                         sum(case when date_lag between date_reset_from and date_reset_to then 0 else 1 end) over (partition by hash_value order by date_value) as cluster_id,
                         time_shift_value,
                         time_shift_rank_a,
                         time_shift_rank_b,
                         {discrepancy_fields}
                         discrepancy_rank_a,
                         discrepancy_rank_b,
                         discrepancy_time_a,
                         discrepancy_time_b,
                         discrepancy_time_value
                    from (
                           select {parallelism}
                                  a_id,
                                  b_id,
                                  hash_value,
                                  date_value,
                                  lag(date_value) over (partition by hash_value order by date_value) as date_lag,
                                  date_value+({time_shift_from}/86400) as date_reset_from,
                                  date_value+({time_shift_to}/86400) as date_reset_to,
                                  time_shift_value,
                                  time_shift_rank_a,
                                  time_shift_rank_b,
                                  {discrepancy_formulas}
                                  dense_rank() over (partition by a_id order by {discrepancy_sums}) as discrepancy_rank_a,
                                  dense_rank() over (partition by b_id order by {discrepancy_sums}) as discrepancy_rank_b,
                                  discrepancy_time_a,
                                  discrepancy_time_b,
                                  discrepancy_time_value
                             from (
                                    select {parallelism}
                                           {key_field_a} as a_id,
                                           {key_field_b} as b_id,
                                           {hash_value} as hash_value,
                                           greatest({date_field_a}, {date_field_b}) as date_value,
                                           {date_field_a} as date_value_a,
                                           {date_field_b} as date_value_b,
                                           86400*({date_field_a}-{date_field_b}) as time_shift_value,
                                           dense_rank() over (partition by {key_field_a} order by abs(86400*({date_field_a}-{date_field_b}))) as time_shift_rank_a,
                                           dense_rank() over (partition by {key_field_b} order by abs(86400*({date_field_a}-{date_field_b}))) as time_shift_rank_b,
                                           {discrepancy_rules}
                                           case when 86400*({date_field_a}-{date_field_b}) not between {time_shift_from} and {time_shift_to} then '{date_field_name_a}' end as discrepancy_time_a,
                                           case when 86400*({date_field_a}-{date_field_b}) not between {time_shift_from} and {time_shift_to} then '{date_field_name_b}' end as discrepancy_time_b,
                                           86400*({date_field_a}-{date_field_b}) as discrepancy_time_value
                                      from rapo_temp_source_a_{process_id} a join rapo_temp_source_b_{process_id} b
                                           on {key_rules}
                                           and {date_rules}
                                    {fetch_limit_expression}
                                  )
                         )
                )

       )
