/*******************************************************************************
Script to deploy Rapo schema in your Oracle database.
Just run the script then check if all objects created successfully.
*******************************************************************************/

create table rapo_ref_types (
  type_code varchar2(3) not null,
  type_desc varchar2(30) not null,
  constraint rapo_ref_types_pk primary key (type_code)
);
insert into rapo_ref_types values ('ANL', 'Analysis');
insert into rapo_ref_types values ('REC', 'Reconciliation');
insert into rapo_ref_types values ('CMP', 'Comparison');
insert into rapo_ref_types values ('REP', 'Report');
insert into rapo_ref_types values ('KPI', 'Key Performance Indicator');
commit;

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
insert into rapo_ref_cases values ('Success', 'Within this case the successful test result is reached');
insert into rapo_ref_cases values ('Loss', 'Within this case a data is lost');
insert into rapo_ref_cases values ('Discrepancy', 'Within this case the difference between some values found');
insert into rapo_ref_cases values ('Duplicate', 'Within this case a data is duplicated');
commit;

create table rapo_ref_engines (
  engine_code varchar2(2) not null,
  engine_desc varchar2(30) not null,
  constraint rapo_ref_engines_pk primary key (engine_code)
);
insert into rapo_ref_engines values ('DB', 'Database SQL');
insert into rapo_ref_engines values ('PY', 'Python');
commit;

create table rapo_config (
  control_id          number(*, 0),
  control_name        varchar2(45) not null,
  control_description varchar2(500),
  control_alias       varchar2(90),
  control_group       varchar2(90),
  control_type        varchar2(30) not null,
  control_engine      varchar2(30) not null,
  source_name         varchar2(128),
  source_date_field   varchar2(128),
  source_filter       clob,
  output_table        clob,
  source_name_a       varchar2(128),
  source_date_field_a varchar2(128),
  source_key_field_a  varchar2(128),
  source_filter_a     clob,
  source_type_a       varchar2(90),
  output_table_a      clob,
  source_name_b       varchar2(128),
  source_date_field_b varchar2(128),
  source_key_field_b  varchar2(128),
  source_filter_b     clob,
  source_type_b       varchar2(90),
  output_table_b      clob,
  rule_config         clob,
  case_config         clob,
  case_definition     clob,
  error_definition    clob,
  need_a              varchar2(1),
  need_b              varchar2(1),
  schedule_config     clob,
  period_back         number(*, 0) default 1 not null,
  period_number       number(*, 0) default 1 not null,
  period_type         varchar2(1) default 'D' not null,
  iteration_config    clob,
  timeout             number(*, 0),
  instance_limit      number(*, 0) default 1,
  output_limit        number(*, 0),
  days_retention      number(*, 0) default 365 not null,
  with_deletion       varchar2(1) default 'N' not null,
  with_drop           varchar2(1) default 'N' not null,
  parallelism         number(*, 0) default 4,
  need_hook           varchar2(1) default 'Y' not null,
  need_prerun_hook    varchar2(1) default 'N' not null,
  need_postrun_hook   varchar2(1) default 'N' not null,
  preparation_sql     clob,
  prerequisite_sql    clob,
  completion_sql      clob,
  status              varchar2(1) default 'N' not null,
  updated_by          varchar2(128),
  updated_date        date,
  created_by          varchar2(128) not null,
  created_date        date not null,
  constraint rapo_config_pk primary key (control_id),
  constraint rapo_config_type_fk
    foreign key (control_type)
    references rapo_ref_types(type_code),
  constraint rapo_config_engine_fk
    foreign key (control_engine)
    references rapo_ref_engines(engine_code)
);

create unique index rapo_config_control_name_ix on rapo_config (control_name);

create sequence rapo_config_seq
increment by 1
start with 1
nocache;

create or replace trigger rapo_config_id_trg
before insert on rapo_config
for each row
begin
  select rapo_config_seq.nextval
    into :new.control_id
    from dual;
end;
/

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

create table rapo_config_bak as select * from rapo_config where 1 = 0;
alter table rapo_config_bak add audit_action varchar2(10);
alter table rapo_config_bak add audit_user varchar2(128);
alter table rapo_config_bak add audit_date date;

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

create table rapo_log (
  process_id         number(*, 0),
  control_id         number(*, 0),
  added              date,
  updated            date,
  start_date         date,
  end_date           date,
  status             varchar2(1),
  date_from          date,
  date_to            date,
  fetched_number     number(*, 0),
  success_number     number(*, 0),
  error_number       number(*, 0),
  error_level        float,
  fetched_number_a   number(*, 0),
  fetched_number_b   number(*, 0),
  success_number_a   number(*, 0),
  success_number_b   number(*, 0),
  error_number_a     number(*, 0),
  error_number_b     number(*, 0),
  error_level_a      float,
  error_level_b      float,
  prerequisite_value number(*, 0),
  text_log           clob,
  text_error         clob,
  text_message       clob,
  constraint rapo_log_pk primary key (process_id)
);

create index rapo_log_control_id_ix on rapo_log (control_id);
create index rapo_log_start_date_ix on rapo_log (start_date);
create index rapo_log_status_ix on rapo_log (status);
create index rapo_log_date_from_ix on rapo_log (date_from);

create sequence rapo_log_seq
increment by 1
start with 1000000001
maxvalue 2000000000
minvalue 1000000000
nocache;

create or replace trigger rapo_log_id_trg
before insert on rapo_log
for each row
begin
  select rapo_log_seq.nextval
    into :new.process_id
    from dual;
end;
/

create table rapo_scheduler (
  id         varchar2(15 char) not null,
  server     varchar2(255),
  username   varchar2(128),
  pid        number(*, 0),
  start_date date,
  stop_date  date,
  status     varchar2(1) not null,
  constraint rapo_scheduler_pk primary key (id)
);
insert into rapo_scheduler (id, status) values ('RAPO.SCHEDULER', 'N');
commit;

create table rapo_web_api (
  id         varchar2(15 char) not null,
  server     varchar2(255 char),
  username   varchar2(128 char),
  pid        number(*, 0),
  url        varchar2(255 char),
  debug      char(1 char),
  start_date date,
  stop_date  date,
  status     varchar2(1) not null
);
insert into rapo_web_api (id, status) values ('RAPO.WEB.API', 'N');
commit;

create or replace function rapo_prerun_control_hook (in_process_id number) return varchar2
as
  v_control_name varchar2(20);
  v_date_from    date;
  v_return_code  varchar2(2000) := null;
begin
  -- extract variables "v_control_name" and "v_date_from" from initiated control
  select
    c.control_name,
    l.date_from
  into
    v_control_name,
    v_date_from
  from rapo_log l
  join rapo_config c on c.control_id = l.control_id
  where l.process_id = in_process_id
  fetch first 1 row only;

  -- Set pre-run conditions here
  -- return NULL or 'OK' to continue control execution
  -- return non null string as error code to terminate the execution
  -- e.g.
  /*
      if v_control_name = 'MY_CONTROL' then
          select
              case
                  when (select count(*) from DS_MSCS where load_date between v_date_from and v_date_from + 1) = 0 then 'No records in table DS_MSCS'
                  else 'OK' -- null would also do
              end into v_return_code
          from dual;
      end if;
  */

  -- Default return
  return v_return_code;

exception
  when others then return 'Error executing RAPO prerun hook: ' || sqlerrm || ', backtrace:' || dbms_utility.format_error_backtrace;
end;
/

create or replace procedure rapo_postrun_control_hook (
  in_process_id number
)
as
begin
  null;
end;
/
