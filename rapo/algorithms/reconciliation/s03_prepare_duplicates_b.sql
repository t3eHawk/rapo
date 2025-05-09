create table rapo_temp_t03_dup_b_{process_id} as
select {parallelism} b.*, x.cluster_id,
       row_number() over (partition by {keys_b}, x.cluster_id order by {date_field_b}, {discrepancy_fields_b} {key_field_b}) as cluster_position_number
  from rapo_temp_source_b_{process_id} b
       left join rapo_temp_t02_org_b_{process_id} o on {key_field_b} = o.b_id
       left join (select b_id, listagg(b_id, '~' on overflow truncate) within group (order by b_id) as b_group_id, cluster_id from rapo_temp_t01_mod_{process_id} group by b_id, cluster_id) x
       on {key_field_b} = x.b_id
 where o.correlation_type = 'F'
