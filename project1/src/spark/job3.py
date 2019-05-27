import csv
import math
from datetime import datetime

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

# HISTORY_PATH = "hdfs://localhost:9000/user/bigdata/historical_stock_prices.csv"
HISTORY_PATH = "hdfs://localhost:9000/user/bigdata/dataset_test_100k_header.csv"
LEGEND_PATH = "hdfs://localhost:9000/user/bigdata/historical_stocks.csv"
MIN_DATE = datetime.strptime("2016-01-01", "%Y-%m-%d").date()
MAX_DATE = datetime.strptime("2018-12-31", "%Y-%m-%d").date()


def filter_period(row):
    return MIN_DATE <= row["date"].date() <= MAX_DATE


def select_columns_history(row):
    return (row.ticker, Row(price_close=row.close,
                            date_created=row["date"].date()))


def select_columns_legend(row):
    return (row.ticker, Row(name=row.name,
                            sector=row.sector))


def pretty_print(row):
    return str([utils.prettify_growth(row.growth), row.avg_daily_price, row.tot_volume])


def iterate(iterable):
    r = []
    for v in iterable.__iter__():
        # for y in x.__iter__():
        r.append(v)
    # return r
    return tuple(r)


# returns true when company_a and company_b are equal, or when their sectors are equal 
def filter_couples(row):
    company_a, company_b = row[1][0], row[1][1]
    a_name, a_sector = company_a[0], company_a[1]
    b_name, b_sector = company_b[0], company_b[1]
    return ((a_name != b_name) and (a_sector != b_sector))


def run_job(history_rdd, legend_rdd):
    history_rdd = history_rdd.filter(filter_period) \
        .map(select_columns_history)

    legend_rdd = legend_rdd.map(select_columns_legend)

    # initialize accumulator
    # ((initial_date, initial_price),(final_date, final_price))
    growth_acc_initial = (MAX_DATE, 0)
    growth_acc_final = (MIN_DATE, 0)
    yeargrowth_rdd = legend_rdd.join(history_rdd) \
        .map(lambda row: ((row[1][0].name, row[1][0].sector, row[1][1].date_created.year, row[1][1].date_created),
                          row[1][1].price_close)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda row: ((row[0][0], row[0][1], row[0][2]),
                          (row[0][3], row[1]))) \
        .aggregateByKey((growth_acc_initial, growth_acc_final),
                        utils.update_growth_acc,
                        utils.combine_growth_accs) \
        .mapValues(utils.calculate_growth) \
        .sortBy(lambda row: row[0]) \
        .cache()

    trend_rdd = yeargrowth_rdd \
        .map(lambda row: ((row[0][0], row[0][1]),
                          str(row[0][2]) + ":" + utils.prettify_growth(row[1]))) \
        .groupByKey() \
        .mapValues(iterate) \
        .map(lambda row: (row[1], (row[0]))) \
        .cache()
    # .mapValues(iterate) put it after groupByKey to see content of trend

    similartrendingcompanies_rdd = trend_rdd.join(trend_rdd) \
        .filter(filter_couples) \
        .collect()
    
    # sort values and remove duplicates (A,B)(B,A))
    # .map(lambda row: (row[0], tuple(sorted(row[1])))).distinct() 
    # it seems that the rdd already does combinations

    for kv in similartrendingcompanies_rdd:
        print(kv)
        

if __name__ == "__main__":
    spark = utils.create_session("job2")
    sc = spark.sparkContext
    # sqlContext = SQLContext(sc)

    history_rdd = utils.load_data(spark, HISTORY_PATH, preview=False)
    legend_rdd = utils.load_data(spark, LEGEND_PATH, preview=False)
    run_job(history_rdd, legend_rdd)
