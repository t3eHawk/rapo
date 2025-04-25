create table rapo_temp_error_a_{process_id} as
select {parallelism} *
  from (
         select {parallelism} a.*,
                case when o.correlation_type in ('F', 'A', 'B', 'M') then 'Duplicate' else 'Loss' end as rapo_result_type,
                cast(null as varchar2(4000)) as rapo_discrepancy_id,
                cast(null as varchar2(4000)) as rapo_discrepancy_description
           from rapo_temp_source_a_{process_id} a
                left join rapo_temp_t02_org_a_{process_id} o on {key_field_a} = o.a_id
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and o.correlation_indicator is null

         union all

         select {parallelism} a.*,
                case when o.correlation_type in ('F', 'A', 'B', 'M') then 'Duplicate' else 'Discrepancy' end as rapo_result_type,
                cast(m.b_id as varchar2(4000)) as rapo_discrepancy_id,
                case when m.discrepancy_time_a is not null then m.discrepancy_time_a||'|'||m.discrepancy_time_value||';' end{discrepancy_description_a} as rapo_discrepancy_description
           from rapo_temp_source_a_{process_id} a
                left join rapo_temp_t01_mod_{process_id} m on {key_field_a} = m.a_id and m.correlation_indicator is not null
                left join rapo_temp_t02_org_a_{process_id} o on {key_field_a} = o.a_id
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and o.correlation_indicator is not null
            and (m.discrepancy_time_a is not null {discrepancy_filter_a})
       )
 where rapo_result_type in ({target_error_types_a})
