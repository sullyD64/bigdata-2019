SET mapreduce.job.reduces = 1;

DROP TABLE IF EXISTS `u33_company_trends`;

-- CREATE EXTERNAL TABLE `u33_company_trends` (
CREATE TABLE `u33_company_trends` (
  `trend` array<STRING>,
  `name` STRING,
  `sector` STRING)
-- LOCATION 'hdfs:////user/user33/hive/u33_company_trends'
-- LOCATION 'file:////home/lorenzo/git/bigdata-2019/project1/src/hive/u33_company_trends'
;


INSERT OVERWRITE TABLE `u33_company_trends`
SELECT
  collect_set(q5.`year_trend`) as `trend`,
  q5.`name`, q5.`sector`
FROM (
  SELECT
    q4.`name`, q4.`sector`,
    concat_ws(':',cast(`year` as STRING), cast(`growth_percentage` as STRING)) as `year_trend`
  FROM (
    SELECT
      q3.`name`, q3.`sector`, q3.`year`,
      round((q3.`final_price` - q3.`initial_price`) * 100 / q3.`initial_price`) as `growth_percentage`
    FROM (
      SELECT
        q2.`name`, q2.`sector`, q2.`year`, q2.`date_created`,
        first_value(`daily_price_sum`) over (partition by `name`, `year` order by `date_created`) as `initial_price`,
        first_value(`daily_price_sum`) over (partition by `name`, `year` order by `date_created` desc) as `final_price`
      FROM (
        SELECT 
          q1.`name`, q1.`sector`, q1.`year`, q1.`date_created`,
          sum(`price_close`) over (partition by `name`, `year`, `date_created` order by `date_created`) as `daily_price_sum`
        FROM (
          SELECT 
            l.`name`, l.`sector`, 
            year(h.`date_created`) as `year`,
            h.`date_created`, h.`price_close`
          FROM u33_legend l JOIN u33_history h on l.`ticker`=h.`ticker`
          WHERE h.`date_created` between Date('2016-01-01') and Date('2018-12-31')
          ) q1
        ) q2
      ) q3  
      GROUP BY `name`, `sector`, `year`, `initial_price`, `final_price`
    ) q4
  ) q5
GROUP BY `name`, `sector`
ORDER BY `trend`;

SELECT * FROM `u33_company_trends`; --LIMIT 10;
