/*
 generare in ordine le 10 azioni la cui quotazione (prezzo di chiusura) è cresciuta maggiormente dal 1998 al 2018, indicando per ogni azione: 
 (ticker, growth%, min_global, max_global, avg_daily_volume)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
*/