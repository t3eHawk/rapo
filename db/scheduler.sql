CREATE TABLE rapo_scheduler (
  id         NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1 INCREMENT BY 1 NOCACHE,
  server     VARCHAR2(30) NOT NULL,
  username   VARCHAR2(30) NOT NULL,
  pid        NUMBER NOT NULL,
  start_date DATE NOT NULL,
  stop_date  DATE,
  status     VARCHAR2(1) NOT NULL,
  CONSTRAINT rapo_scheduler_pk PRIMARY KEY (id)
)
