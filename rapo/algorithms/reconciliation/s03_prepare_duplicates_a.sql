create table rapo_temp_t03_dup_a_{process_id} as
select {parallelism} a.*, x.cluster_id,
       row_number() over (partition by {keys_a}, x.cluster_id order by {date_field_a}, {discrepancy_fields_a} {key_field_a}) as cluster_position_number
  from rapo_temp_source_a_{process_id} a
       left join rapo_temp_t02_org_a_{process_id} o on {key_field_a} = o.a_id
       left join (select a_id, listagg(b_id, '~' on overflow truncate) within group (order by b_id) as b_group_id, cluster_id from rapo_temp_t01_mod_{process_id} group by a_id, cluster_id) x
       on {key_field_a} = x.a_id
 where o.correlation_type = 'F'
