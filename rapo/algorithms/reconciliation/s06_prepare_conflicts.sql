create materialized view rapo_temp_t05_mac_{process_id} refresh on demand as
select {parallelism} a_id, b_id
from (
  select m.a_id, m.b_id,
         row_number() over (partition by m.b_id order by m.discrepancy_rank_b, m.time_shift_rank_b, m.a_id) as match_position_a,
         row_number() over (partition by m.a_id order by m.discrepancy_rank_a, m.time_shift_rank_a, m.b_id) as match_position_b
    from rapo_temp_t01_mod_{process_id} m
         left join rapo_temp_t02_org_a_{process_id} a on a.a_id = m.a_id
         left join rapo_temp_t02_org_b_{process_id} b on b.b_id = m.b_id
   where m.correlation_type in ('A', 'B', 'M') and m.correlation_indicator is null
     and a.correlation_indicator is null and b.correlation_indicator is null
  )
where match_position_a = 1 and match_position_b = 1
