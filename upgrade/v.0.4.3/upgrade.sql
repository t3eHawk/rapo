/* ============================================================================
Migration scripts from RAPO v0.3.3 to v0.4.3
============================================================================ */

----------------------------------------------
-- New dictionary with Case types description.
create table rapo_ref_cases (
  case_type varchar2(15) not null,
  case_desc varchar2(300),
  constraint rapo_ref_cases_pk primary key (case_type)
);
insert into rapo_ref_cases values ('Normal', 'Within this case normal result is defined');
insert into rapo_ref_cases values ('Info', 'Within this case something found that must be noted');
insert into rapo_ref_cases values ('Error', 'Within this case an error found that must be inspected');
insert into rapo_ref_cases values ('Warning', 'Within this case a minor error found that should be inspected as soon as possible');
insert into rapo_ref_cases values ('Incident', 'Within this case a critical error found that must investigated immediately');
insert into rapo_ref_cases values ('Discrepancy', 'Within this case the difference between some values found');
commit;

-----------------------------------------
-- Control configuration table migration.
drop trigger rapo_config_audit;
alter table rapo_config modify control_name varchar2(45);
alter table rapo_config modify control_desc varchar2(500);
alter table rapo_config modify control_alias varchar2(90);
alter table rapo_config modify control_group varchar2(90);
alter table rapo_config modify source_name varchar2(128);
alter table rapo_config modify source_date_field varchar2(128);
alter table rapo_config modify source_name_a varchar2(128);
alter table rapo_config modify source_date_field_a varchar2(128);
alter table rapo_config modify source_name_b varchar2(128);
alter table rapo_config modify source_date_field_b varchar2(128);
alter table rapo_config modify created_by varchar2(128);
alter table rapo_config modify created_date date;

alter table rapo_config add with_deletion varchar2(1) default 'N' not null;
alter table rapo_config add with_drop varchar2(1) default 'N' not null;
alter table rapo_config add need_prerun_hook varchar2(1) default 'N' not null;
alter table rapo_config add need_postrun_hook varchar2(1) default 'N' not null;
alter table rapo_config add source_filter clob;
alter table rapo_config add source_filter_a clob;
alter table rapo_config add source_filter_b clob;
alter table rapo_config add case_config clob;
alter table rapo_config add result_config clob;
alter table rapo_config add preparation_sql clob;
alter table rapo_config add prerequisite_sql clob;
alter table rapo_config add completion_sql clob;
alter table rapo_config add updated_by varchar2(128);
alter table rapo_config add updated_date date;

update rapo_config set need_postrun_hook = need_hook;
commit;

declare
  type list is table of varchar2(255) index by binary_integer;
  fieldnames list;
begin
  fieldnames(fieldnames.count+1) := 'output_table';
  fieldnames(fieldnames.count+1) := 'output_table_a';
  fieldnames(fieldnames.count+1) := 'output_table_b';
  fieldnames(fieldnames.count+1) := 'rule_config';
  fieldnames(fieldnames.count+1) := 'error_config';
  fieldnames(fieldnames.count+1) := 'schedule';
  for i in 1..fieldnames.count loop
    execute immediate 'alter table rapo_config add '||fieldnames(i)||'_new clob';
    execute immediate 'update rapo_config set '||fieldnames(i)||'_new = '||fieldnames(i);
    commit;
    execute immediate 'alter table rapo_config drop column '||fieldnames(i);
    execute immediate 'alter table rapo_config rename column '||fieldnames(i)||'_new to '||fieldnames(i);
  end loop;
end;
/

declare
  type list is table of varchar2(255) index by binary_integer;
  fieldnames list;
begin
  fieldnames(fieldnames.count+1) := 'control_name';
  fieldnames(fieldnames.count+1) := 'control_desc';
  fieldnames(fieldnames.count+1) := 'control_alias';
  fieldnames(fieldnames.count+1) := 'control_group';
  fieldnames(fieldnames.count+1) := 'control_type';
  fieldnames(fieldnames.count+1) := 'control_subtype';
  fieldnames(fieldnames.count+1) := 'control_engine';
  fieldnames(fieldnames.count+1) := 'source_name';
  fieldnames(fieldnames.count+1) := 'source_date_field';
  fieldnames(fieldnames.count+1) := 'source_filter';
  fieldnames(fieldnames.count+1) := 'output_table';
  fieldnames(fieldnames.count+1) := 'source_name_a';
  fieldnames(fieldnames.count+1) := 'source_date_field_a';
  fieldnames(fieldnames.count+1) := 'source_filter_a';
  fieldnames(fieldnames.count+1) := 'output_table_a';
  fieldnames(fieldnames.count+1) := 'source_name_b';
  fieldnames(fieldnames.count+1) := 'source_date_field_b';
  fieldnames(fieldnames.count+1) := 'source_filter_b';
  fieldnames(fieldnames.count+1) := 'output_table_b';
  fieldnames(fieldnames.count+1) := 'rule_config';
  fieldnames(fieldnames.count+1) := 'case_config';
  fieldnames(fieldnames.count+1) := 'result_config';
  fieldnames(fieldnames.count+1) := 'error_config';
  fieldnames(fieldnames.count+1) := 'need_a';
  fieldnames(fieldnames.count+1) := 'need_b';
  fieldnames(fieldnames.count+1) := 'schedule';
  fieldnames(fieldnames.count+1) := 'days_back';
  fieldnames(fieldnames.count+1) := 'days_retention';
  fieldnames(fieldnames.count+1) := 'with_deletion';
  fieldnames(fieldnames.count+1) := 'with_drop';
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

-- New triggers.
create or replace trigger rapo_config_ins_trg
before insert on rapo_config
for each row
begin
  select sys_context('userenv', 'os_user') as created_by,
         sysdate as created_date
    into :new.created_by,
         :new.created_date
    from dual;
end;
/

create or replace trigger rapo_config_upd_trg
before update on rapo_config
for each row
begin
  select sys_context('userenv', 'os_user') as updated_by,
         sysdate as updated_date
    into :new.updated_by,
         :new.updated_date
    from dual;
end;
/

create table rapo_config_old as select * from rapo_config_bak order by audit_date;
drop table rapo_config_bak;
create table rapo_config_bak as select * from rapo_config where 1 = 0;
alter table rapo_config_bak add audit_action varchar2(10);
alter table rapo_config_bak add audit_user varchar2(128);
alter table rapo_config_bak add audit_date date;
insert into rapo_config_bak
select control_id,
       control_name,
       control_desc,
       control_alias,
       control_group,
       control_type,
       control_subtype,
       control_engine,
       source_name,
       source_date_field,
       null as source_filter,
       output_table,
       source_name_a,
       source_date_field_a,
       null as source_filter_a,
       output_table_a,
       source_name_b,
       source_date_field_b,
       null as source_filter_b,
       output_table_b,
       rule_config,
       null as case_config,
       null as result_config,
       error_config,
       need_a,
       need_b,
       schedule,
       days_back,
       days_retention,
       'N' as with_deletion,
       'N' as with_drop,
       need_hook,
       'N' need_prerun_hook,
       need_hook as need_postrun_hook,
       null as preparation_sql,
       null as prerequisite_sql,
       status,
       null as updated_by,
       null as updated_date,
       created_by,
       created_date,
       upper(audit_action) as audit_action,
       audit_user,
       audit_date
  from rapo_config_old
;
commit;

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
      :new.control_desc,
      :new.control_alias,
      :new.control_group,
      :new.control_type,
      :new.control_subtype,
      :new.control_engine,
      :new.source_name,
      :new.source_date_field,
      :new.source_filter,
      :new.output_table,
      :new.source_name_a,
      :new.source_date_field_a,
      :new.source_filter_a,
      :new.output_table_a,
      :new.source_name_b,
      :new.source_date_field_b,
      :new.source_filter_b,
      :new.output_table_b,
      :new.rule_config,
      :new.case_config,
      :new.result_config,
      :new.error_config,
      :new.need_a,
      :new.need_b,
      :new.schedule,
      :new.days_back,
      :new.days_retention,
      :new.with_deletion,
      :new.with_drop,
      :new.need_hook,
      :new.need_prerun_hook,
      :new.need_postrun_hook,
      :new.preparation_sql,
      :new.prerequisite_sql,
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
      :old.control_desc,
      :old.control_alias,
      :old.control_group,
      :old.control_type,
      :old.control_subtype,
      :old.control_engine,
      :old.source_name,
      :old.source_date_field,
      :old.source_filter,
      :old.output_table,
      :old.source_name_a,
      :old.source_date_field_a,
      :old.source_filter_a,
      :old.output_table_a,
      :old.source_name_b,
      :old.source_date_field_b,
      :old.source_filter_b,
      :old.output_table_b,
      :old.rule_config,
      :old.case_config,
      :old.result_config,
      :old.error_config,
      :old.need_a,
      :old.need_b,
      :old.schedule,
      :old.days_back,
      :old.days_retention,
      :old.with_deletion,
      :old.with_drop,
      :old.need_hook,
      :old.need_prerun_hook,
      :old.need_postrun_hook,
      :old.preparation_sql,
      :old.prerequisite_sql,
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
alter table rapo_log add prerequisite_value number(*, 0);
alter table rapo_log add text_message clob;
declare
  type list is table of varchar2(255) index by binary_integer;
  fieldnames list;
begin
  fieldnames(fieldnames.count+1) := 'control_id';
  fieldnames(fieldnames.count+1) := 'added';
  fieldnames(fieldnames.count+1) := 'updated';
  fieldnames(fieldnames.count+1) := 'start_date';
  fieldnames(fieldnames.count+1) := 'end_date';
  fieldnames(fieldnames.count+1) := 'status';
  fieldnames(fieldnames.count+1) := 'date_from';
  fieldnames(fieldnames.count+1) := 'date_to';
  fieldnames(fieldnames.count+1) := 'fetched';
  fieldnames(fieldnames.count+1) := 'success';
  fieldnames(fieldnames.count+1) := 'errors';
  fieldnames(fieldnames.count+1) := 'error_level';
  fieldnames(fieldnames.count+1) := 'fetched_a';
  fieldnames(fieldnames.count+1) := 'fetched_b';
  fieldnames(fieldnames.count+1) := 'success_a';
  fieldnames(fieldnames.count+1) := 'success_b';
  fieldnames(fieldnames.count+1) := 'errors_a';
  fieldnames(fieldnames.count+1) := 'errors_b';
  fieldnames(fieldnames.count+1) := 'error_level_a';
  fieldnames(fieldnames.count+1) := 'error_level_b';
  fieldnames(fieldnames.count+1) := 'prerequisite_value';
  fieldnames(fieldnames.count+1) := 'text_log';
  fieldnames(fieldnames.count+1) := 'text_error';
  fieldnames(fieldnames.count+1) := 'text_message';
  for i in 1..fieldnames.count loop
    execute immediate 'alter table rapo_log modify '||fieldnames(i)||' invisible';
    execute immediate 'alter table rapo_log modify '||fieldnames(i)||' visible';
  end loop;
end;
/

-----------------------------------
-- Control result tables migration.
declare
  control_type rapo_config.control_type%type;
begin
  for i in (
    select table_name from user_tables where table_name like 'RAPO_REST%'
  ) loop
    select control_type into control_type from rapo_config where i.table_name like '%'||control_name;
    if control_type = 'ANL' then
      execute immediate 'alter table '||i.table_name||' add rapo_result_key number(*, 0)';
      execute immediate 'alter table '||i.table_name||' add rapo_result_value varchar2(100 char)';
      execute immediate 'alter table '||i.table_name||' add rapo_result_type varchar2(15 char)';
      execute immediate 'alter table '||i.table_name||' modify rapo_process_id invisible';
      execute immediate 'alter table '||i.table_name||' modify rapo_process_id visible';
    end if;
  end loop;
end;
/

------------------------------
-- Component tables migration.
alter table rapo_scheduler modify server varchar2(255);
alter table rapo_scheduler modify username varchar2(128);
alter table rapo_web_api modify server varchar2(255);
alter table rapo_web_api modify username varchar2(128);
alter table rapo_web_api modify url varchar2(255);
