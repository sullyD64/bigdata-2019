SET mapreduce.job.reduces = 1;

SELECT
	tmp2.ticker, as `ticker`
	round(min(tmp2.price_close),4) as `min_price`,
	round(max(tmp2.price_close),4) as `max_price`,
	round(avg(tmp2.volume),4) as `avg_volume`,
	max(tmp2.growth_percentage) as `growth_percentage`
FROM (
	SELECT
		h.ticker, h.price_close, h.volume,
		round((tmp1.final_price - tmp1.initial_price) * 100 / tmp1.initial_price) as growth_percentage
	FROM history h
	JOIN (
		SELECT 
			`ticker`,
			first_value(`price_close`) over (partition by `ticker` order by `date_created`) as `initial_price`,
			first_value(`price_close`) over (partition by `ticker` order by `date_created` desc) as `final_price`
		FROM history h
		WHERE `date_created` between Date('1998-01-01') and Date('2018-12-31')
		) tmp1 on tmp1.ticker=h.ticker
	) tmp2
GROUP BY ticker
SORT BY growth_percentage desc
LIMIT 10