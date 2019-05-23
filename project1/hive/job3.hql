SET hive.mapred.mode = "nonstrict";

-- DROP TABLE IF EXISTS `company_trends`;
-- CREATE TABLE `company_trends` AS
SELECT
  collect_set(`year_trend`) as `trend`,
  `name`, `sector`
FROM (
  SELECT
    `name`, `sector`,
    CONCAT_WS(':',cast(`year` as STRING), cast(`growth_percentage` as STRING)) as `year_trend`
  FROM (
    SELECT
      `name`, `sector`, `year`,
      round((final_price - initial_price) * 100 / initial_price) as `growth_percentage`
    FROM (
      SELECT
        `name`, `sector`, `year`, `date_created`,
        first_value(`daily_price_sum`) over (partition by `name`, `year` order by `date_created`) as `initial_price`,
        first_value(`daily_price_sum`) over (partition by `name`, `year` order by `date_created` desc) as `final_price`
      FROM (
        SELECT 
          `name`, `sector`, `year`, `date_created`,
          sum(price_close) over (partition by `name`, `year`, date_created order by date_created) as `daily_price_sum`
        FROM (
          SELECT 
            l.name, l.sector, 
            year(h.date_created) as `year`,
            h.date_created, h.price_close, h.volume
          FROM legend l JOIN history h on l.ticker=h.ticker
          WHERE h.date_created between Date('2016-01-01') and Date('2018-12-31')
          ) src
        ) src2
      ) src3
      GROUP BY `name`, `sector`, `year`, `initial_price`, `final_price`
  ) src4
) src5
GROUP BY `name`, `sector`
ORDER BY `trend`
LIMIT 200