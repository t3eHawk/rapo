create table rapo_temp_t02_org_b_{process_id}
nologging
as
select {parallelism}
       {key_field_b} as b_id,
       case
            when m.one_to_one_match is not null then 'O'
            when m.fuzzy_match_conflict is not null then 'F'
            when m.one_to_many_match_a is not null then 'A'
            when m.one_to_many_match_b is not null then 'B'
            when m.many_to_many_match is not null then 'M'
       end as correlation_type,
       case when m.one_to_one_match is not null then 'R' end as correlation_status,
       case when m.one_to_one_match is not null then 'X' end as correlation_indicator
  from rapo_temp_source_b_{process_id} b
       left join (
         select b_id,
                case when sum(decode(correlation_type, 'O', 1)) is not null then 'X' end as one_to_one_match,
                case when sum(decode(correlation_type, 'F', 1)) is not null then 'X' end as fuzzy_match_conflict,
                case when sum(decode(correlation_type, 'A', 1)) is not null then 'X' end as one_to_many_match_a,
                case when sum(decode(correlation_type, 'B', 1)) is not null then 'X' end as one_to_many_match_b,
                case when sum(decode(correlation_type, 'M', 1)) is not null then 'X' end as many_to_many_match
           from rapo_temp_t01_mod_{process_id}
          group by b_id
       ) m
       on {key_field_b} = m.b_id
