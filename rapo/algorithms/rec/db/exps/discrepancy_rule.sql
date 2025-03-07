case when (coalesce({field_a}, 0)-coalesce({field_b}, 0)){percentage_formula} not between {tolerance_from} and {tolerance_to} then '{field_name_a}' end as discrepancy_{discrepancy_number}_a,
case when (coalesce({field_a}, 0)-coalesce({field_b}, 0)){percentage_formula} not between {tolerance_from} and {tolerance_to} then '{field_name_b}' end as discrepancy_{discrepancy_number}_b,
coalesce({field_a}, 0)-coalesce({field_b}, 0) as discrepancy_{discrepancy_number}_value,
