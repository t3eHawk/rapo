CREATE TABLE rapo_log (
  process_id    NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1000000001 INCREMENT BY 1 MAXVALUE 2000000000 NOCACHE,
  control_id    NUMBER,
  added         DATE,
  start_date    DATE,
  end_date      DATE,
  status        VARCHAR2(1),
  date_from     DATE,
  date_to       DATE,
  fetched_x     NUMBER,
  fetched_a     NUMBER,
  fetched_b     NUMBER,
  success_x     NUMBER,
  success_a     NUMBER,
  success_b     NUMBER,
  errors_x      NUMBER,
  errors_a      NUMBER,
  errors_b      NUMBER,
  error_level_x FLOAT,
  error_level_a FLOAT,
  error_level_b FLOAT,
  updated_date   DATE,
  text_log      CLOB,
  CONSTRAINT rapo_log_pk PRIMARY KEY (process_id)
);

CREATE INDEX rapo_log_control_id_ix ON rapo_log (control_id);
CREATE INDEX rapo_log_start_date_ix ON rapo_log (start_date);
CREATE INDEX rapo_log_status_ix ON rapo_log (status);
CREATE INDEX rapo_log_date_from_ix ON rapo_log (date_from);
