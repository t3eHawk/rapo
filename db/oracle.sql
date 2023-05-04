/*******************************************************************************
Script to deploy RAPO schema in your Oracle database.
Just run the script then check if all objects created successfully.
*******************************************************************************/

create table rapo_ref_types (
  type_code varchar2(3) not null,
  type_desc varchar2(30) not null,
  constraint rapo_ref_types_pk primary key (type_code)
);
insert into rapo_ref_types values ('ANL', 'Analysis');
insert into rapo_ref_types values ('REC', 'Reconciliation');
insert into rapo_ref_types values ('REP', 'Report');
insert into rapo_ref_types values ('KPI', 'Key Performance Indicator');
commit;

create table rapo_ref_subtypes (
  type_code    varchar2(3) not null,
  subtype_code varchar2(3) not null,
  subtype_desc varchar2(30) not null,
  constraint rapo_ref_subtypes_pk primary key (type_code, subtype_code)
);
insert into rapo_ref_subtypes values ('REC', 'MA', 'Matching');
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
  control_name        varchar2(20) not null,
  control_desc        varchar2(300),
  control_alias       varchar2(60),
  control_group       varchar2(60),
  control_type        varchar2(30) not null,
  control_subtype     varchar2(30),
  control_engine      varchar2(30) not null,
  source_name         varchar2(30),
  source_date_field   varchar2(30),
  source_filter       varchar2(30),
  output_table        clob,
  source_name_a       varchar2(30),
  source_date_field_a varchar2(30),
  source_filter_a     clob,
  output_table_a      clob,
  source_name_b       varchar2(30),
  source_date_field_b varchar2(30),
  source_filter_b     clob,
  output_table_b      clob,
  rule_config         clob,
  case_config         clob,
  result_config       clob,
  error_config        clob,
  need_a              varchar2(1),
  need_b              varchar2(1),
  schedule            varchar2(300) not null,
  days_back           number(*, 0) default 1 not null,
  days_retention      number(*, 0) default 365 not null,
  need_drop           varchar2(1) default 'N' not null,
  prerun_sql_list     clob,
  prerun_check_sql    clob,
  prerun_need_hook    varchar2(1) default 'N' not null,
  need_hook           varchar2(1) default 'Y' not null,
  need_prerun_hook    varchar2(1) default 'N' not null,
  need_postrun_hook   varchar2(1) default 'N' not null,
  status              varchar2(1) default 'N' not null,
  updated_date        date default sysdate not null,
  created_date        date default sysdate not null,
  created_by          varchar2(60) default user not null,
  constraint rapo_config_pk primary key (control_id),
  constraint rapo_config_type_fk
    foreign key (control_type)
    references rapo_ref_types(type_code),
  constraint rapo_config_subtype_fk
    foreign key (control_type, control_subtype)
    references rapo_ref_subtypes(type_code, subtype_code),
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

create table rapo_config_bak as select * from rapo_config where 1 = 0;
alter table rapo_config_bak add audit_action varchar2(10);
alter table rapo_config_bak add audit_date date;
alter table rapo_config_bak add audit_user varchar2(60);

create or replace trigger rapo_config_audit
after insert or update or delete on rapo_config
for each row
declare
  v_audit_action varchar2(10);
  v_audit_date   date         := sysdate;
  v_audit_user   varchar2(60) := user;
begin
  if inserting then
    v_audit_action := 'insert';
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
      :new.output_table,
      :new.source_name_a,
      :new.source_date_field_a,
      :new.output_table_a,
      :new.source_name_b,
      :new.source_date_field_b,
      :new.output_table_b,
      :new.rule_config,
      :new.error_config,
      :new.need_a,
      :new.need_b,
      :new.schedule,
      :new.days_back,
      :new.days_retention,
      :new.need_hook,
      :new.status,
      :new.created_date,
      :new.created_by,
      v_audit_action,
      v_audit_date,
      v_audit_user
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
        :old.output_table,
        :old.source_name_a,
        :old.source_date_field_a,
        :old.output_table_a,
        :old.source_name_b,
        :old.source_date_field_b,
        :old.output_table_b,
        :old.rule_config,
        :old.error_config,
        :old.need_a,
        :old.need_b,
        :old.schedule,
        :old.days_back,
        :old.days_retention,
        :old.need_hook,
        :old.status,
        :old.created_date,
        :old.created_by,
        v_audit_action,
        v_audit_date,
        v_audit_user
      );
  end if;
end;
/

create table rapo_log (
  process_id    number(*, 0),
  control_id    number(*, 0),
  added         date,
  updated       date,
  start_date    date,
  end_date      date,
  status        varchar2(1),
  date_from     date,
  date_to       date,
  fetched       number(*, 0),
  success       number(*, 0),
  errors        number(*, 0),
  error_level   float,
  fetched_a     number(*, 0),
  fetched_b     number(*, 0),
  success_a     number(*, 0),
  success_b     number(*, 0),
  errors_a      number(*, 0),
  errors_b      number(*, 0),
  error_level_a float,
  error_level_b float,
  text_log      clob,
  text_error    clob,
  text_message  clob,
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
  server     varchar2(30),
  username   varchar2(30),
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
  server     varchar2(30 char),
  username   varchar2(30 char),
  pid        number(*, 0),
  url        varchar2(30 char),
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
  v_date_from date;
  v_return_code varchar2(2000) := null;
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

create or replace procedure rapo_postrun_control_hook (
  in_process_id number
)
as
begin
  null;
end;
