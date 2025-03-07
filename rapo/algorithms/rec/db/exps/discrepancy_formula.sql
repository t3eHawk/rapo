discrepancy_{discrepancy_number}_a,
discrepancy_{discrepancy_number}_b,
discrepancy_{discrepancy_number}_value,
dense_rank() over (partition by a_id order by abs(discrepancy_{discrepancy_number}_value)) as discrepancy_{discrepancy_number}_rank,
