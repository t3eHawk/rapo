/*******************************************************************************
Script to deploy schema for RAPO.
Just run the script then check if all objects created successfully.
*******************************************************************************/

CREATE TABLE rapo_ref_types (
  type_code VARCHAR2(3) NOT NULL,
  type_desc VARCHAR2(30) NOT NULL,
  CONSTRAINT rapo_ref_types_pk PRIMARY KEY (type_code)
);
INSERT INTO rapo_ref_types VALUES ('ANL', 'Analysis');
INSERT INTO rapo_ref_types VALUES ('REC', 'Reconciliation');
INSERT INTO rapo_ref_types VALUES ('KPI', 'Key Performance Indicator');
COMMIT;

CREATE TABLE rapo_ref_methods (
  method_code VARCHAR2(3) NOT NULL,
  method_desc VARCHAR2(30) NOT NULL,
  CONSTRAINT rapo_ref_methods_pk PRIMARY KEY (method_code)
);
INSERT INTO rapo_ref_methods VALUES ('MA', 'Matching');
INSERT INTO rapo_ref_methods VALUES ('MI', 'Missing');
COMMIT;

CREATE TABLE rapo_ref_engines (
  engine_code VARCHAR2(2) NOT NULL,
  engine_desc VARCHAR2(30) NOT NULL,
  CONSTRAINT rapo_ref_engines_pk PRIMARY KEY (engine_code)
);
INSERT INTO rapo_ref_engines VALUES ('DB', 'Database SQL');
INSERT INTO rapo_ref_engines VALUES ('PY', 'Python');
COMMIT;

CREATE TABLE rapo_config (
  control_id          NUMBER,
  -- control_id          NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1 INCREMENT BY 1 NOCACHE,
  control_name        VARCHAR2(20) NOT NULL,
  control_desc        VARCHAR2(300),
  control_alias       VARCHAR2(60),
  control_group       VARCHAR2(60),
  control_type        VARCHAR2(30) NOT NULL,
  control_method      VARCHAR2(30),
  control_engine      VARCHAR2(30) NOT NULL,
  source_name         VARCHAR2(30),
  source_date_field   VARCHAR2(30),
  output_table        VARCHAR2(4000),
  source_name_a       VARCHAR2(30),
  source_date_field_a VARCHAR2(30),
  output_table_a      VARCHAR2(4000),
  source_name_b       VARCHAR2(30),
  source_date_field_b VARCHAR2(30),
  output_table_b      VARCHAR2(4000),
  match_config        VARCHAR2(4000),
  mismatch_config     VARCHAR2(4000),
  error_config        VARCHAR2(4000),
  need_a              VARCHAR2(1),
  need_b              VARCHAR2(1),
  schedule            VARCHAR2(300) NOT NULL,
  days_back           NUMBER DEFAULT 1 NOT NULL,
  days_retention      NUMBER DEFAULT 365 NOT NULL,
  need_hook           VARCHAR2(1) DEFAULT 'Y' NOT NULL,
  status              VARCHAR2(1) DEFAULT 'N' NOT NULL,
  created_date        DATE DEFAULT SYSDATE NOT NULL,
  created_by          VARCHAR2(60) DEFAULT USER NOT NULL,
  CONSTRAINT rapo_config_pk PRIMARY KEY (control_id),
  CONSTRAINT rapo_config_type_fk
    FOREIGN KEY (control_type)
    REFERENCES rapo_ref_types(type_code),
  CONSTRAINT rapo_config_method_fk
    FOREIGN KEY (control_method)
    REFERENCES rapo_ref_methods(method_code),
  CONSTRAINT rapo_config_engine_fk
    FOREIGN KEY (control_engine)
    REFERENCES rapo_ref_engines(engine_code)
);

CREATE UNIQUE INDEX rapo_config_control_name_ix ON rapo_config (control_name);

CREATE SEQUENCE rapo_config_seq
INCREMENT BY 1
START WITH 1
NOCACHE;

CREATE OR REPLACE TRIGGER rapo_config_id_trg
BEFORE INSERT ON rapo_config
FOR EACH ROW
BEGIN
  SELECT rapo_config_seq.nextval
    INTO :new.control_id
    FROM dual;
END;
/

CREATE TABLE rapo_config_bak AS SELECT * FROM rapo_config WHERE 1 = 0;
ALTER TABLE rapo_config_bak ADD audit_action VARCHAR2(10);
ALTER TABLE rapo_config_bak ADD audit_date DATE;
ALTER TABLE rapo_config_bak ADD audit_user VARCHAR2(60);

CREATE OR REPLACE TRIGGER rapo_config_audit
AFTER INSERT OR UPDATE OR DELETE ON rapo_config
FOR EACH ROW
DECLARE
  v_audit_action VARCHAR2(10);
  v_audit_date   DATE         := SYSDATE;
  v_audit_user   VARCHAR2(60) := USER;
BEGIN
  IF INSERTING THEN
    v_audit_action := 'INSERT';
    INSERT INTO rapo_config_bak VALUES (
      :new.control_id,
      :new.control_name,
      :new.control_desc,
      :new.control_alias,
      :new.control_group,
      :new.control_type,
      :new.control_method,
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
      :new.match_config,
      :new.mismatch_config,
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
  ELSE
    IF UPDATING THEN
      v_audit_action := 'UPDATE';
    ELSIF DELETING THEN
      v_audit_action := 'DELETE';
    END IF;
      INSERT INTO rapo_config_bak VALUES (
        :old.control_id,
        :old.control_name,
        :old.control_desc,
        :old.control_alias,
        :old.control_group,
        :old.control_type,
        :old.control_method,
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
        :old.match_config,
        :old.mismatch_config,
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
  END IF;
END;
/

CREATE TABLE rapo_log (
  process_id    NUMBER,
  -- process_id    NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1000000001 INCREMENT BY 1 MAXVALUE 2000000000 NOCACHE,
  control_id    NUMBER,
  added         DATE,
  updated       DATE,
  start_date    DATE,
  end_date      DATE,
  status        VARCHAR2(1),
  date_from     DATE,
  date_to       DATE,
  fetched       NUMBER,
  success       NUMBER,
  errors        NUMBER,
  error_level   FLOAT,
  fetched_a     NUMBER,
  fetched_b     NUMBER,
  success_a     NUMBER,
  success_b     NUMBER,
  errors_a      NUMBER,
  errors_b      NUMBER,
  error_level_a FLOAT,
  error_level_b FLOAT,
  text_log      CLOB,
  text_error    CLOB,
  CONSTRAINT rapo_log_pk PRIMARY KEY (process_id)
);

CREATE INDEX rapo_log_control_id_ix ON rapo_log (control_id);
CREATE INDEX rapo_log_start_date_ix ON rapo_log (start_date);
CREATE INDEX rapo_log_status_ix ON rapo_log (status);
CREATE INDEX rapo_log_date_from_ix ON rapo_log (date_from);

CREATE SEQUENCE rapo_log_seq
INCREMENT BY 1
START WITH 1000000001
MAXVALUE 2000000000
MINVALUE 1000000000
NOCACHE;

CREATE OR REPLACE TRIGGER rapo_log_id_trg
BEFORE INSERT ON rapo_log
FOR EACH ROW
BEGIN
  SELECT rapo_log_seq.nextval
    INTO :new.process_id
    FROM dual;
END;
/

CREATE TABLE rapo_scheduler (
  id         NUMBER,
  -- id         NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1 INCREMENT BY 1 NOCACHE,
  server     VARCHAR2(30) NOT NULL,
  username   VARCHAR2(30) NOT NULL,
  pid        NUMBER NOT NULL,
  start_date DATE NOT NULL,
  stop_date  DATE,
  status     VARCHAR2(1) NOT NULL,
  CONSTRAINT rapo_scheduler_pk PRIMARY KEY (id)
);

CREATE SEQUENCE rapo_scheduler_seq
INCREMENT BY 1
START WITH 1
NOCACHE;

CREATE OR REPLACE TRIGGER rapo_scheduler_id_trg
BEFORE INSERT ON rapo_scheduler
FOR EACH ROW
BEGIN
  SELECT rapo_scheduler_seq.nextval
    INTO :new.id
    FROM dual;
END;
/

CREATE OR REPLACE PROCEDURE rapo_control_hook (
  in_process_id NUMBER
)
AS
BEGIN
END;
