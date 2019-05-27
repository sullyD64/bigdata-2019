SET mapreduce.job.reduces = 1;
SET hive.mapred.mode = "nonstrict";

SELECT 
  `trend`,
  `similar_trending_companies`[0] as `company_A`,
  `similar_trending_companies`[1] as `company_B`
FROM (
  SELECT DISTINCT *
  FROM (
    SELECT
      concat_ws(', ',`trend`) as `trend`,
      sort_array(array(company_A, company_B)) as `similar_trending_companies`
    FROM (
      SELECT
        `trend`,
        concat_ws(' : ', `name_A`,`sector_A`) as `company_A`,
        concat_ws(' : ', `name_B`,`sector_B`) as `company_B`
      FROM (
        SELECT 
          ct1.trend as `trend`, 
          ct1.name as `name_A`, 
          ct1.sector as `sector_A`, 
          ct2.name as `name_B`, 
          ct2.sector as `sector_B`
        FROM `company_trends` ct1 LEFT JOIN `company_trends` ct2
        ON concat_ws(',', ct1.trend)=concat_ws(',', ct2.trend)
        WHERE ct1.name <> ct2.name 
        AND ct1.sector <> ct2.sector
        ORDER BY `trend`
        ) q1
      ) q2
    ) q3
  ORDER BY `trend`, `similar_trending_companies`
  ) q4