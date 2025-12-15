coalesce((abs({field_name}) - avg(abs({field_name})) over (partition by {key_field})) / nullif(stddev_pop(abs({field_name})) over (partition by {key_field}), 0), 0)
