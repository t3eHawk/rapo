create table rapo_temp_error_a_{process_id} as
select {parallelism} *
  from (
         select {parallelism} a.*,
                case
                     when o.correlation_indicator is not null and (m.discrepancy_time_a is not null {discrepancy_filters_a}) then 'Discrepancy'
                     when o.correlation_indicator is null then 'Loss'
                end as rapo_result_type,
                cast(m.b_id as varchar2(4000)) as rapo_discrepancy_id,
                case when m.discrepancy_time_a is not null then m.discrepancy_time_a||'|'||m.discrepancy_time_value||';' end{discrepancy_descriptions_a} as rapo_discrepancy_description
           from rapo_temp_source_a_{process_id} a
                left join rapo_temp_t01_mod_{process_id} m on {key_field_a} = m.a_id and m.correlation_indicator is not null
                left join rapo_temp_t02_org_a_{process_id} o on {key_field_a} = o.a_id
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and (o.correlation_type = 'O' or o.correlation_type is null)

         union all

         select {parallelism} a.*,
                case
                     when o.correlation_indicator is not null and (m.discrepancy_time_a is not null {discrepancy_filters_a}) then 'Discrepancy'
                     when o.correlation_indicator is null and (m.discrepancy_time_a is not null {discrepancy_filters_a}) then 'Loss'
                     when o.correlation_indicator is null then 'Duplicate'
                end as rapo_result_type,
                cast(m.b_id as varchar2(4000)) as rapo_discrepancy_id,
                case when m.discrepancy_time_a is not null then m.discrepancy_time_a||'|'||m.discrepancy_time_value||';' end{discrepancy_descriptions_a} as rapo_discrepancy_description
           from rapo_temp_source_a_{process_id} a 
           left join rapo_temp_t02_org_a_{process_id} o on {key_field_a} = o.a_id
           left join rapo_temp_t01_mod_{process_id} m on {key_field_a} = m.a_id and m.distance_rank_a = 1
          where {date_field_a} between to_date('{date_from:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS') and to_date('{date_to:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')
            and o.correlation_type in ('F', 'A', 'B', 'M')
       )
 where rapo_result_type in ({target_error_types_a})
