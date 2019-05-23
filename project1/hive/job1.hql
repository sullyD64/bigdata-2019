SET mapreduce.job.reduces = 1;

SELECT 
	`ticker`,
	round(min(price_close),4) as `min_price`,
	round(max(price_close),4) as `max_price`,
	round(avg(volume),4) as `avg_volume`,
	max(growth_percentage) as `growth_percentage`
FROM (
	SELECT
		`ticker`,
		`price_close`,
		`volume`,
		round((final_price - initial_price) * 100 / initial_price) as growth_percentage
	FROM (
		SELECT 
			`ticker`,
			`price_close`,
			`volume`,
			first_value(`price_close`) over (partition by `ticker` order by `date_created`) as `initial_price`,
			first_value(`price_close`) over (partition by `ticker` order by `date_created` desc) as `final_price`
		FROM history h
		WHERE `date_created` between Date('1998-01-01') and Date('2018-12-31')) src
) src2
GROUP BY `ticker`
SORT BY `growth_percentage` desc
LIMIT 10
