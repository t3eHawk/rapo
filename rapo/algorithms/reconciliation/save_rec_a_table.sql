create table rapo_temp_res_a_{process_id} as
select {parallelism} a.*,
       'Success' as rapo_result_type,
       cast(null as varchar2(4000)) as rapo_discrepancy_id,
       cast(null as varchar2(4000)) as rapo_discrepancy_description
  from rapo_temp_fda_{process_id} a
 where not exists (select e.{key_field_name_a} from rapo_temp_err_a_{process_id} e where {key_field_a} = e.{key_field_name_a})
