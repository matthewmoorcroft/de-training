Create or replace table most_favoured_city
as select
city,
avg(rating) as average_rating
from mm_dublin_training.silver.transformed_pulse_data
group by city
order by average_rating desc;