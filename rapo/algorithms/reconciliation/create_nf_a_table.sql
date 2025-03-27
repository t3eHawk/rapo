create table rapo_temp_nf_a_{process_id} as
select {parallelism} {key_field_a} as a_id
  from rapo_temp_fda_{process_id} a
 where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
   and not exists (select c.a_id from rapo_temp_comb_{process_id} c where {key_field_a} = c.a_id and c.total_match_number_a = 1 and c.total_match_number_b = 1)
   and not exists (select d.a_id from rapo_temp_dup_{process_id} d where {key_field_a} = d.a_id)
