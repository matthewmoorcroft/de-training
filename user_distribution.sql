Create or replace table mm_dublin_training.gold.user_distribution
as select
job_role,
count(1) as headcount
from mm_dublin_training.silver.transformed_pulse_data
group by job_role
order by headcount desc;