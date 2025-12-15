coalesce((abs({field_name}) - min(abs({field_name})) over ()) / nullif(max(abs({field_name})) over () - min(abs({field_name})) over (), 0), 0)
