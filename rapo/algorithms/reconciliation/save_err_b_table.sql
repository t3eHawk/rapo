create table rapo_temp_err_b_{process_id} as
select {parallelism} *
  from (
         select {parallelism} b.*,
                case when x.total_match_number > 1 then 'Duplicate' else 'Loss' end as rapo_result_type,
                cast(null as varchar2(4000)) as rapo_discrepancy_id,
                cast(null as varchar2(4000)) as rapo_discrepancy_description
           from rapo_temp_fdb_{process_id} b
                left join (select b_id, avg(total_match_number_a) as total_match_number from rapo_temp_comb_{process_id} group by b_id) x
                on {key_field_b} = x.b_id
          where {date_field_b} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and exists (select nf.b_id from rapo_temp_nf_b_{process_id} nf where {key_field_b} = nf.b_id)

         union all

         select {parallelism} b.*,
                case when x.total_match_number > 1 then 'Duplicate' else 'Discrepancy' end as rapo_result_type,
                cast(c.a_id as varchar2(4000)) as rapo_discrepancy_id,
                case when c.discrepancy_time_b is not null then c.discrepancy_time_b||'['||c.discrepancy_time_value||']'||', ' end{discrepancy_description_b_c} as rapo_discrepancy_description
           from rapo_temp_fdb_{process_id} b join rapo_temp_comb_{process_id} c on {key_field_b} = c.b_id
                left join (select b_id, avg(total_match_number_a) as total_match_number from rapo_temp_comb_{process_id} group by b_id) x
                on {key_field_b} = x.b_id
          where {date_field_b} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and c.total_match_number_a = 1 and c.total_match_number_b = 1
            and (c.discrepancy_time_b is not null {discrepancy_filter_b_c})

         union all

         select {parallelism} b.*,
                case when x.total_match_number > 1 then 'Duplicate' else 'Discrepancy' end as rapo_result_type,
                cast(d.a_id as varchar2(4000)) as rapo_discrepancy_id,
                case when d.discrepancy_time_b is not null then d.discrepancy_time_b||'['||d.discrepancy_time_value||']'||', ' end{discrepancy_description_b_d} as rapo_discrepancy_description
           from rapo_temp_fdb_{process_id} b join rapo_temp_dup_{process_id} d on {key_field_b} = d.b_id
                left join (select b_id, avg(total_match_number_a) as total_match_number from rapo_temp_comb_{process_id} group by b_id) x
                on {key_field_b} = x.b_id
          where {date_field_b} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and (d.discrepancy_time_b is not null {discrepancy_filter_b_d})
       )
 where rapo_result_type in ({target_err_types_b})
