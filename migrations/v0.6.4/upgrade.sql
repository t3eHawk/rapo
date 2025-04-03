-- ============================================================================
-- Migration scripts from Rapo v0.5.1 to v0.6.4
-- ============================================================================

-- ------------------------------------
-- Control configuration table migration.

-- Modify control types to replace previous Reconciliation design.
insert into rapo_ref_types values ('CMP', 'Comparison');
commit;

update rapo_config set control_type = 'CMP', control_subtype = null
 where control_type = 'REC'
   and control_subtype = 'MA'
;
commit;

-- Add new case types for Reconciliations.
insert into rapo_ref_cases values ('Success', 'Within this case the successful test result is reached');
insert into rapo_ref_cases values ('Loss', 'Within this case a data is lost');
insert into rapo_ref_cases values ('Duplicate', 'Within this case a data is duplicated');

-- Add new control parameters.
alter table rapo_config add source_key_field_a varchar2(128);
alter table rapo_config add source_type_a varchar2(90);
alter table rapo_config add source_key_field_b varchar2(128);
alter table rapo_config add source_type_b varchar2(90);
alter table rapo_config add period_number number(*, 0) default 1 not null;
alter table rapo_config add period_type varchar2(1) default 'D' not null;
alter table rapo_config add iteration_config clob;
alter table rapo_config add timeout number(*, 0);
alter table rapo_config add instance_limit number(*, 0) default 1;
alter table rapo_config add output_limit number(*, 0);
alter table rapo_config add parallelism number(*, 0) default 4;

-- Rename control parameters.
alter table rapo_config rename column control_desc to control_description;
alter table rapo_config rename column result_config to case_definition;
alter table rapo_config rename column error_config to error_definition;
alter table rapo_config rename column schedule to schedule_config;
alter table rapo_config rename column days_back to period_back;

-- Delete depreciated control sub type structure.
alter table rapo_config drop constraint rapo_config_subtype_fk;
alter table rapo_config drop column control_subtype;
drop table rapo_ref_subtypes;

-- Update control configuration structure.
declare
  type list is table of varchar2(255) index by binary_integer;
  fieldnames list;
begin
  fieldnames(fieldnames.count+1) := 'control_name';
  fieldnames(fieldnames.count+1) := 'control_description';
  fieldnames(fieldnames.count+1) := 'control_alias';
  fieldnames(fieldnames.count+1) := 'control_group';
  fieldnames(fieldnames.count+1) := 'control_type';
  fieldnames(fieldnames.count+1) := 'control_engine';
  fieldnames(fieldnames.count+1) := 'source_name';
  fieldnames(fieldnames.count+1) := 'source_date_field';
  fieldnames(fieldnames.count+1) := 'source_filter';
  fieldnames(fieldnames.count+1) := 'output_table';
  fieldnames(fieldnames.count+1) := 'source_name_a';
  fieldnames(fieldnames.count+1) := 'source_date_field_a';
  fieldnames(fieldnames.count+1) := 'source_key_field_a';
  fieldnames(fieldnames.count+1) := 'source_filter_a';
  fieldnames(fieldnames.count+1) := 'source_type_a';
  fieldnames(fieldnames.count+1) := 'output_table_a';
  fieldnames(fieldnames.count+1) := 'source_name_b';
  fieldnames(fieldnames.count+1) := 'source_date_field_b';
  fieldnames(fieldnames.count+1) := 'source_key_field_b';
  fieldnames(fieldnames.count+1) := 'source_filter_b';
  fieldnames(fieldnames.count+1) := 'source_type_b';
  fieldnames(fieldnames.count+1) := 'output_table_b';
  fieldnames(fieldnames.count+1) := 'rule_config';
  fieldnames(fieldnames.count+1) := 'case_config';
  fieldnames(fieldnames.count+1) := 'case_definition';
  fieldnames(fieldnames.count+1) := 'error_definition';
  fieldnames(fieldnames.count+1) := 'need_a';
  fieldnames(fieldnames.count+1) := 'need_b';
  fieldnames(fieldnames.count+1) := 'schedule_config';
  fieldnames(fieldnames.count+1) := 'period_back';
  fieldnames(fieldnames.count+1) := 'period_number';
  fieldnames(fieldnames.count+1) := 'period_type';
  fieldnames(fieldnames.count+1) := 'iteration_config';
  fieldnames(fieldnames.count+1) := 'timeout';
  fieldnames(fieldnames.count+1) := 'instance_limit';
  fieldnames(fieldnames.count+1) := 'output_limit';
  fieldnames(fieldnames.count+1) := 'days_retention';
  fieldnames(fieldnames.count+1) := 'with_deletion';
  fieldnames(fieldnames.count+1) := 'with_drop';
  fieldnames(fieldnames.count+1) := 'parallelism';
  fieldnames(fieldnames.count+1) := 'need_hook';
  fieldnames(fieldnames.count+1) := 'need_prerun_hook';
  fieldnames(fieldnames.count+1) := 'need_postrun_hook';
  fieldnames(fieldnames.count+1) := 'preparation_sql';
  fieldnames(fieldnames.count+1) := 'prerequisite_sql';
  fieldnames(fieldnames.count+1) := 'completion_sql';
  fieldnames(fieldnames.count+1) := 'status';
  fieldnames(fieldnames.count+1) := 'updated_by';
  fieldnames(fieldnames.count+1) := 'updated_date';
  fieldnames(fieldnames.count+1) := 'created_by';
  fieldnames(fieldnames.count+1) := 'created_date';
  for i in 1..fieldnames.count loop
    execute immediate 'alter table rapo_config modify '||fieldnames(i)||' invisible';
    execute immediate 'alter table rapo_config modify '||fieldnames(i)||' visible';
  end loop;
end;
/

-- Update control configuration audit.
create table rapo_config_bak_v_0_5_1 as select * from rapo_config_bak order by audit_date;
drop table rapo_config_bak;
create table rapo_config_bak as select * from rapo_config where 1 = 0;
alter table rapo_config_bak add audit_action varchar2(10);
alter table rapo_config_bak add audit_user varchar2(128);
alter table rapo_config_bak add audit_date date;
insert into rapo_config_bak
select control_id,
       control_name,
       control_desc as control_description,
       control_alias,
       control_group,
       control_type,
       control_engine,
       source_name,
       source_date_field,
       source_filter,
       output_table,
       source_name_a,
       source_date_field_a,
       null as source_key_field_a,
       source_filter_a,
       null as source_type_a,
       output_table_a,
       source_name_b,
       source_date_field_b,
       null as source_key_field_b,
       source_filter_b,
       null as source_type_b,
       output_table_b,
       rule_config,
       case_config,
       result_config as case_definition,
       error_config as error_definition,
       need_a,
       need_b,
       schedule as schedule_config,
       days_back as period_back,
       1 as period_number,
       'D' as period_type,
       null as iteration_config,
       null as timeout,
       1 as instance_limit,
       null as output_limit,
       days_retention,
       with_deletion,
       with_drop,
       4 as parallelism,
       need_hook,
       need_prerun_hook,
       need_postrun_hook,
       preparation_sql,
       prerequisite_sql,
       completion_sql,
       status,
       updated_by,
       updated_date,
       created_by,
       created_date,
       audit_action,
       audit_user,
       audit_date
  from rapo_config_bak_v_0_5_1
 order by audit_date
;
commit;
drop table rapo_config_bak_v_0_5_1;

-- Recompile a new version of the audit procedure.
create or replace trigger rapo_config_audit
after insert or update or delete on rapo_config
for each row
declare
  v_audit_action rapo_config_bak.audit_action%type;
  v_audit_user   rapo_config_bak.audit_user%type  := sys_context('userenv', 'os_user');
  v_audit_date   rapo_config_bak.audit_date%type  := sysdate;
begin
  if inserting then
    v_audit_action := 'INSERT';
    insert into rapo_config_bak values (
      :new.control_id,
      :new.control_name,
      :new.control_description,
      :new.control_alias,
      :new.control_group,
      :new.control_type,
      :new.control_engine,
      :new.source_name,
      :new.source_date_field,
      :new.source_filter,
      :new.output_table,
      :new.source_name_a,
      :new.source_date_field_a,
      :new.source_key_field_a,
      :new.source_filter_a,
      :new.source_type_a,
      :new.output_table_a,
      :new.source_name_b,
      :new.source_date_field_b,
      :new.source_key_field_b,
      :new.source_filter_b,
      :new.source_type_b,
      :new.output_table_b,
      :new.rule_config,
      :new.case_config,
      :new.case_definition,
      :new.error_definition,
      :new.need_a,
      :new.need_b,
      :new.schedule_config,
      :new.period_back,
      :new.period_number,
      :new.period_type,
      :new.iteration_config,
      :new.timeout,
      :new.instance_limit,
      :new.output_limit,
      :new.days_retention,
      :new.with_deletion,
      :new.with_drop,
      :new.parallelism,
      :new.need_hook,
      :new.need_prerun_hook,
      :new.need_postrun_hook,
      :new.preparation_sql,
      :new.prerequisite_sql,
      :new.completion_sql,
      :new.status,
      :new.updated_by,
      :new.updated_date,
      :new.created_by,
      :new.created_date,
      v_audit_action,
      v_audit_user,
      v_audit_date
    );
  else
    if updating then
      v_audit_action := 'UPDATE';
    elsif deleting then
      v_audit_action := 'DELETE';
    end if;
    insert into rapo_config_bak values (
      :old.control_id,
      :old.control_name,
      :old.control_description,
      :old.control_alias,
      :old.control_group,
      :old.control_type,
      :old.control_engine,
      :old.source_name,
      :old.source_date_field,
      :old.source_filter,
      :old.output_table,
      :old.source_name_a,
      :old.source_date_field_a,
      :old.source_key_field_a,
      :old.source_filter_a,
      :old.source_type_a,
      :old.output_table_a,
      :old.source_name_b,
      :old.source_date_field_b,
      :old.source_key_field_b,
      :old.source_filter_b,
      :old.source_type_b,
      :old.output_table_b,
      :old.rule_config,
      :old.case_config,
      :old.case_definition,
      :old.error_definition,
      :old.need_a,
      :old.need_b,
      :old.schedule_config,
      :old.period_back,
      :old.period_number,
      :old.period_type,
      :old.iteration_config,
      :old.timeout,
      :old.instance_limit,
      :old.output_limit,
      :old.days_retention,
      :old.with_deletion,
      :old.with_drop,
      :old.parallelism,
      :old.need_hook,
      :old.need_prerun_hook,
      :old.need_postrun_hook,
      :old.preparation_sql,
      :old.prerequisite_sql,
      :old.completion_sql,
      :old.status,
      :old.updated_by,
      :old.updated_date,
      :old.created_by,
      :old.created_date,
      v_audit_action,
      v_audit_user,
      v_audit_date
    );
  end if;
end;
/


---------------------------------------
-- Control run history table migration.

-- Rename history properties.
alter table rapo_log rename column fetched to fetched_number;
alter table rapo_log rename column success to success_number;
alter table rapo_log rename column errors to error_number;
alter table rapo_log rename column fetched_a to fetched_number_a;
alter table rapo_log rename column fetched_b to fetched_number_b;
alter table rapo_log rename column success_a to success_number_a;
alter table rapo_log rename column success_b to success_number_b;
alter table rapo_log rename column errors_a to error_number_a;
alter table rapo_log rename column errors_b to error_number_b;
