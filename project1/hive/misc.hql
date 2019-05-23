
-- generare in ordine le 10 azioni la cui quotazione (prezzo di chiusura) 
-- è cresciuta maggiormente dal 1998 al 2018, indicando per ogni azione: 
-- (ticker, growth%, min_global, max_global, avg_daily_volume)


-- legend: TICKER, EXCHANGE, NAME, SECTOR, INDUSTRY)
-- history: (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
-- goal: show the top 10 stocks (identified by TICKER) by their %INCREASE.
-- output: (TICKER, (%INCREASE, MIN_PRICE, MAX_PRICE, AVG_VOL))


ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' STORED AS TEXTFILE


-- ALTER TABLE `history` REPLACE COLUMNS(
--   `ticker` STRING,
--   `price_open` FLOAT,
--   `price_close` FLOAT,
--   `price_low` FLOAT,
--   `price_high` FLOAT,
--   `volume` BIGINT,
--   `date_created` DATE);







-- CREATE TABLE 1prime_date_min AS 
-- SELECT ticker, min(data) as prima_data 
-- FROM 1hsp 
-- WHERE year(data) >= '1998' and year(data) <= '2018' 
-- group by ticker;

-- CREATE TABLE 1ultime_date_min AS 
-- SELECT ticker, max(data) as ultima_data 
-- FROM 1hsp 
-- WHERE year(data) >= '1998' and year(data) <= '2018' 
-- group by ticker;

-- CREATE TABLE 1prime_chiusure AS 
-- SELECT p.ticker, p.prima_data, ROUND(h.close,4) as close 
-- FROM 1prime_date_min p 
-- JOIN 1hsp h ON p.prima_data=h.data and p.ticker=h.ticker 
-- order by p.ticker, p.prima_data;

-- CREATE TABLE 1ultime_chiusure AS 
-- SELECT u.ticker, u.ultima_data, ROUND(h.close,4) as close 
-- FROM 1ultime_date_min u 
-- JOIN 1hsp h ON u.ultima_data=h.data and u.ticker=h.ticker 
-- order by u.ticker, u.ultima_data;

-- CREATE TABLE 1ticker__perc_incremento AS 
-- SELECT u.ticker, ROUND(((u.close - p.close) / p.close) * 100) as perc_incremento 
-- FROM 1ultime_chiusure u 
-- JOIN 1prime_chiusure p ON u.ticker=p.ticker 
-- order by u.ticker;

-- CREATE TABLE 1prezzo_minimo_massimo AS 
-- SELECT ticker, ROUND(min(low)) as min, ROUND(max(high)) as max 
-- FROM 1hsp 
-- WHERE year(data) >= '1998' and year(data) <= '2018' 
-- group by ticker;

-- CREATE TABLE 1t_p_m_M AS 
-- SELECT p.ticker, t.perc_incremento, p.min, p.max 
-- FROM 1prezzo_minimo_massimo p 
-- JOIN 1ticker__perc_incremento t ON p.ticker=t.ticker 
-- order by p.ticker;

-- CREATE TABLE 1volumi_medi AS 
-- SELECT ticker, ROUND(AVG(volume), 4) as volume_medio_giornaliero 
-- FROM 1hsp 
-- WHERE year(data) >= '1998' AND year(data) <= '2018' 
-- GROUP BY ticker;

-- CREATE TABLE 1result1b AS 
-- SELECT t.ticker, t.perc_incremento, t.min, t.max, v.volume_medio_giornaliero 
-- FROM 1volumi_medi v 
-- JOIN 1t_p_m_M t ON v.ticker=t.ticker 
-- ORDER BY t.perc_incremento DESC LIMIT 10;




CREATE TABLE `top10stocks` AS
	WITH tmp2 AS (SELECT `t.ticker` AS `ticker`,
		(t.final_price - t.initial_price) * 100 / t.initial_price AS `growth_percentage`,
		MIN(`t.price_close`) AS `min_price`,
		MAX(`t.price_close`) AS `max_price`,
		AVG(`t.volume`) AS `avg_volume`,
		rank() OVER (PARTITION BY `t.ticker` ORDER BY `growth_percentage` DESC) AS `ranking`
	FROM `tmp` t
	GROUP BY `ticker`)
	SELECT * FROM tmp2
	WHERE `ranking` < 11
	ORDER BY `ranking`
------------------------------------------------------------------


