SET mapreduce.job.reduces = 1;

SELECT
  `sector`, `year`,
  sum(volume) as `tot_volume`,
	round(avg(`daily_price_sum`),4) as `avg_daily_price`,
  round((`final_price` - `initial_price`) * 100 / `initial_price`) as `growth_percentage`
FROM (
  SELECT
    `sector`, `year`, `date_created`, `daily_price_sum`, `volume`,
    first_value(`daily_price_sum`) over (partition by `sector`, `year` order by `date_created`) as `initial_price`,
    first_value(`daily_price_sum`) over (partition by `sector`, `year` order by `date_created` desc) as `final_price`
  FROM (
    SELECT 
      `sector`, `year`, `date_created`, `volume`,
      sum(`price_close`) over (partition by `sector`, `year`, `date_created` order by `date_created`) as `daily_price_sum`
    FROM (
      SELECT 
        l.`sector`, 
        year(h.date_created) as `year`,
        h.`date_created`, h.`price_close`, h.`volume`
      FROM u33_legend l JOIN u33_history h on l.`ticker`=h.`ticker`
      WHERE h.`date_created` between Date('2004-01-01') and Date('2018-12-31')) q1
    ) q2
) q3

GROUP BY sector, `year`, `initial_price`, `final_price`
ORDER BY sector, `year`
--LIMIT 200