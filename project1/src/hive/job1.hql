SET mapreduce.job.reduces = 1;

SELECT 
	`ticker`,
	round(min(`price_low`),4) as `min_price`,
	round(max(`price_high`),4) as `max_price`,
	round(avg(`volume`),4) as `avg_volume`,
	round((`final_price` - `initial_price`) * 100 / `initial_price`) as `growth_percentage`
FROM (
	SELECT 
		`ticker`, `price_close`, `price_low`, `price_high`,	`volume`,
		first_value(`price_close`) over (partition by `ticker` order by `date_created`) as `initial_price`,
		first_value(`price_close`) over (partition by `ticker` order by `date_created` desc) as `final_price`
	FROM `u33_history` h
	WHERE `date_created` between Date('1998-01-01') and Date('2018-12-31')) q1
GROUP BY `ticker`, `initial_price`, `final_price`
SORT BY `growth_percentage` desc
LIMIT 10
