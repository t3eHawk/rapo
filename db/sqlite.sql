/*******************************************************************************
Script to deploy RAPO schema in your SQLite database.
Just run the script then check if all objects created successfully.
*******************************************************************************/

create table rapo_ref_types (
  type_code text not null,
  type_desc text not null,
  constraint rapo_ref_types_pk primary key (type_code)
);
insert into rapo_ref_types values ('ANL', 'Analysis');
insert into rapo_ref_types values ('REC', 'Reconciliation');
insert into rapo_ref_types values ('REP', 'Report');
insert into rapo_ref_types values ('KPI', 'Key Performance Indicator');

create table rapo_ref_subtypes (
  type_code    text not null,
  subtype_code text not null,
  subtype_desc text not null,
  constraint rapo_ref_subtypes_pk primary key (type_code, subtype_code)
);
insert into rapo_ref_subtypes values ('REC', 'MA', 'Matching');

create table rapo_ref_engines (
  engine_code text not null,
  engine_desc text not null,
  constraint rapo_ref_engines_pk primary key (engine_code)
);
insert into rapo_ref_engines values ('DB', 'Database SQL');
insert into rapo_ref_engines values ('PY', 'Python');

create table rapo_config (
  control_id          integer primary key autoincrement,
  control_name        text not null,
  control_desc        text,
  control_alias       text,
  control_group       text,
  control_type        text not null,
  control_subtype     text,
  control_engine      text not null,
  source_name         text,
  source_date_field   text,
  source_filter       text,
  output_table        text,
  source_name_a       text,
  source_date_field_a text,
  source_filter_a     text,
  output_table_a      text,
  source_name_b       text,
  source_date_field_b text,
  source_filter_b     text,
  output_table_b      text,
  rule_config         text,
  case_config         text,
  result_config       text,
  error_config        text,
  need_a              text,
  need_b              text,
  schedule            text not null,
  days_back           integer default 1 not null,
  days_retention      integer default 365 not null,
  need_drop           text default 'N' not null,
  prerun_sql_list     text,
  prerun_check_sql    text,
  prerun_need_hook    text default 'N' not null,
  need_hook           text default 'Y' not null,
  need_prerun_hook    text default 'N' not null,
  need_postrun_hook   text default 'N' not null,
  status              text default 'N' not null,
  updated_date        date default (datetime('now', 'localtime')) not null,
  created_date        date default (datetime('now', 'localtime')) not null,
  created_by          text default user not null,
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

create table rapo_log (
  process_id    integer primary key autoincrement,
  control_id    integer,
  added         date,
  updated       date,
  start_date    date,
  end_date      date,
  status        text,
  date_from     date,
  date_to       date,
  fetched       integer,
  success       integer,
  errors        integer,
  error_level   float,
  fetched_a     integer,
  fetched_b     integer,
  success_a     integer,
  success_b     integer,
  errors_a      integer,
  errors_b      integer,
  error_level_a float,
  error_level_b float,
  text_message  text
);

create index rapo_log_control_id_ix on rapo_log (control_id);
create index rapo_log_start_date_ix on rapo_log (start_date);
create index rapo_log_status_ix on rapo_log (status);
create index rapo_log_date_from_ix on rapo_log (date_from);

create table rapo_scheduler (
  id         text not null primary key,
  server     text,
  username   text,
  pid        integer,
  start_date date,
  stop_date  date,
  status     text not null
);
insert into rapo_scheduler (id, status) values ('RAPO.SCHEDULER', 'N');

create table rapo_web_api (
  id         text not null,
  server     text,
  username   text,
  pid        integer,
  url        text,
  debug      text,
  start_date date,
  stop_date  date,
  status     text not null
);
insert into rapo_web_api (id, status) values ('RAPO.WEB.API', 'N');
