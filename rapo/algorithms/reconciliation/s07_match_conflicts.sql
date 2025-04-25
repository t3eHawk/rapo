declare
  v_counter number := 1;
begin
  while v_counter > 0 loop
    merge {parallelism} into rapo_temp_t01_mod_{process_id} m
    using rapo_temp_t05_mac_{process_id} c on (m.a_id = c.a_id and m.b_id = c.b_id)
    when matched then update set correlation_status = 'R', correlation_indicator = 'X';
    commit;

    merge {parallelism} into rapo_temp_t02_org_a_{process_id} a
    using rapo_temp_t05_mac_{process_id} c on (a.a_id = c.a_id)
    when matched then update set correlation_status = 'R', correlation_indicator = 'X';
    commit;

    merge {parallelism} into rapo_temp_t02_org_b_{process_id} b
    using rapo_temp_t05_mac_{process_id} c on (b.b_id = c.b_id)
    when matched then update set correlation_status = 'R', correlation_indicator = 'X';
    commit;

    dbms_mview.refresh('rapo_temp_t05_mac_{process_id}');
    select count(*) into v_counter from rapo_temp_t05_mac_{process_id};
    exit when v_counter = 0;
  end loop;
end;
