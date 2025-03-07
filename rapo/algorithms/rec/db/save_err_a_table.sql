create table rapo_temp_err_a_{process_id} as
select {parallelism} *
  from (
         select {parallelism} a.*,
                case when x.total_match_number > 1 then 'Duplicate' else 'Loss' end as rapo_result_type,
                cast(null as varchar2(4000)) as rapo_discrepancy_id,
                cast(null as varchar2(4000)) as rapo_discrepancy_description
           from rapo_temp_fda_{process_id} a
                left join (select a_id, avg(total_match_number_b) as total_match_number from rapo_temp_comb_{process_id} group by a_id) x
                on {key_field_a} = x.a_id
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and exists (select nf.a_id from rapo_temp_nf_a_{process_id} nf where {key_field_a} = nf.a_id)

         union all

         select {parallelism} a.*,
                case when x.total_match_number > 1 then 'Duplicate' else 'Discrepancy' end as rapo_result_type,
                cast(c.b_id as varchar2(4000)) as rapo_discrepancy_id,
                case when c.discrepancy_time_a is not null then c.discrepancy_time_a||'['||c.discrepancy_time_value||']'||', ' end{discrepancy_description_a_c} as rapo_discrepancy_description
           from rapo_temp_fda_{process_id} a join rapo_temp_comb_{process_id} c on {key_field_a} = c.a_id
                left join (select a_id, avg(total_match_number_b) as total_match_number from rapo_temp_comb_{process_id} group by a_id) x
                on {key_field_a} = x.a_id
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and c.total_match_number_a = 1 and c.total_match_number_b = 1
            and (c.discrepancy_time_a is not null {discrepancy_filter_a_c})

         union all

         select {parallelism} a.*,
                case when x.total_match_number > 1 then 'Duplicate' else 'Discrepancy' end as rapo_result_type,
                cast(d.b_id as varchar2(4000)) as rapo_discrepancy_id,
                case when d.discrepancy_time_a is not null then d.discrepancy_time_a||'['||d.discrepancy_time_value||']'||', ' end{discrepancy_description_a_d} as rapo_discrepancy_description
           from rapo_temp_fda_{process_id} a join rapo_temp_dup_{process_id} d on {key_field_a} = d.a_id
                left join (select a_id, avg(total_match_number_b) as total_match_number from rapo_temp_comb_{process_id} group by a_id) x
                on {key_field_a} = x.a_id
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and (d.discrepancy_time_a is not null {discrepancy_filter_a_d})
       )
 where rapo_result_type in ({target_err_types_a})
