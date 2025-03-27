create table rapo_temp_nf_b_{process_id} as
select {parallelism} {key_field_b} as b_id
  from rapo_temp_fdb_{process_id} b
 where {date_field_b} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
   and not exists (select c.b_id from rapo_temp_comb_{process_id} c where {key_field_b} = c.b_id and c.total_match_number_a = 1 and c.total_match_number_b = 1)
   and not exists (select d.b_id from rapo_temp_dup_{process_id} d where {key_field_b} = d.b_id)
