(abs({field_name}) / count(*) over (partition by {key_field}))
