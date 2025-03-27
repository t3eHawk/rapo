create table rapo_temp_res_b_{process_id} as
select {parallelism} b.*,
       'Success' as rapo_result_type,
       cast(null as varchar2(4000)) as rapo_discrepancy_id,
       cast(null as varchar2(4000)) as rapo_discrepancy_description
  from rapo_temp_fdb_{process_id} b
 where not exists (select e.{key_field_name_b} from rapo_temp_err_b_{process_id} e where {key_field_b} = e.{key_field_name_b})
