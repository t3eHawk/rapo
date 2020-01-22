CREATE TABLE rapo_config (
  control_id     NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1 INCREMENT BY 1 NOCACHE,
  control_name   VARCHAR2(20) NOT NULL,
  control_desc   VARCHAR2(300),
  control_alias  VARCHAR2(60),
  control_group  VARCHAR2(60),
  control_type   VARCHAR2(30) NOT NULL,
  control_engine VARCHAR2(30) NOT NULL,
  source_x       VARCHAR2(30),
  date_x         VARCHAR2(30),
  source_a       VARCHAR2(30),
  date_a         VARCHAR2(30),
  source_b       VARCHAR2(30),
  date_b         VARCHAR2(30),
  matching       VARCHAR2(4000),
  tolerance      VARCHAR2(4000),
  error          VARCHAR2(4000),
  output_x       VARCHAR2(4000),
  output_a       VARCHAR2(4000),
  output_b       VARCHAR2(4000),
  schedule       VARCHAR2(300) NOT NULL,
  days_back      NUMBER DEFAULT 1 NOT NULL,
  need_b         VARCHAR2(1),
  need_hook      VARCHAR2(1) DEFAULT 'Y' NOT NULL,
  retention      NUMBER DEFAULT 365 NOT NULL,
  status         VARCHAR2(1) DEFAULT 'N' NOT NULL,
  created_date   DATE DEFAULT SYSDATE NOT NULL,
  created_by     VARCHAR2(60) DEFAULT USER NOT NULL,
  CONSTRAINT rapo_config_pk PRIMARY KEY (control_id)
)
;

CREATE UNIQUE INDEX rapo_config_control_name_ix ON rapo_config (control_name);

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
      :new.control_engine,
      :new.source_x,
      :new.date_x,
      :new.source_a,
      :new.date_a,
      :new.source_b,
      :new.date_b,
      :new.matching,
      :new.tolerance,
      :new.error,
      :new.output_x,
      :new.output_a,
      :new.output_b,
      :new.schedule,
      :new.days_back,
      :new.need_b,
      :new.need_hook,
      :new.retention,
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
        :old.control_engine,
        :old.source_x,
        :old.date_x,
        :old.source_a,
        :old.date_a,
        :old.source_b,
        :old.date_b,
        :old.matching,
        :old.tolerance,
        :old.error,
        :old.output_x,
        :old.output_a,
        :old.output_b,
        :old.schedule,
        :old.days_back,
        :old.need_b,
        :old.need_hook,
        :old.retention,
        :old.status,
        :old.created_date,
        :old.created_by,
        v_audit_action,
        v_audit_date,
        v_audit_user
      );
  END IF;
END;
