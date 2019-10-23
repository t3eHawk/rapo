CREATE TABLE rapo_config (
  control_id     NUMBER GENERATED ALWAYS AS IDENTITY,
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
  CONSTRAINT rapo_config_pk PRIMARY KEY (control_id)
)
;

CREATE UNIQUE INDEX rapo_config_control_name_ix ON rapo_config (control_name);
