import csv
import math
from datetime import datetime

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

# HISTORY_PATH = "hdfs://localhost:9000/user/user33/testset/dataset_test_100k_header.csv"
HISTORY_PATH = "hdfs://localhost:9000/user/user33/dataset/historical_stock_prices.csv"
LEGEND_PATH = "hdfs://localhost:9000/user/user33/dataset/historical_stocks.csv"
MIN_DATE = datetime.strptime("1998-01-01", "%Y-%m-%d").date()
MAX_DATE = datetime.strptime("2018-12-31", "%Y-%m-%d").date()


def filter_period(row):
    return MIN_DATE <= row["date"].date() <= MAX_DATE


def select_columns(row):
    return (row.ticker, Row(price_close=row.close,
                            price_low=row.low,
                            price_high=row.high,
                            volume=row.volume,
                            date_created=row["date"].date()))


def pretty_print(row):
    return str([utils.prettify_growth(row.growth), row.min_price, row.max_price, row.avg_volume])


def run_job(rdd):
    rdd = rdd.filter(filter_period) \
        .map(select_columns)

    minprice_rdd = rdd.map(lambda row: (row[0], row[1].price_low)) \
        .reduceByKey(min) \
        .mapValues(lambda x: round(x, 4)) \
        .cache()

    maxprice_rdd = rdd.map(lambda row: (row[0], row[1].price_high)) \
        .reduceByKey(max) \
        .mapValues(lambda x: round(x, 4)) \
        .cache()

    # initialize accumulator
    # (runningSum, runningCount)
    avgvol_acc = (0, 0)
    avgvol_rdd = rdd.map(lambda row: (row[0], row[1].volume)) \
        .aggregateByKey(avgvol_acc,
                        lambda acc, x: (acc[0] + x, acc[1] + 1),
                        lambda acc_a, acc_b: (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1])) \
        .mapValues(lambda acc: round(acc[0]/acc[1], 4)) \
        .cache()

    # initialize accumulator
    # ((initial_date, initial_price),(final_date, final_price))
    growth_acc_initial = (MAX_DATE, 0)
    growth_acc_final = (MIN_DATE, 0)
    growth_rdd = rdd.map(lambda row: (row[0], (row[1].date_created, row[1].price_close))) \
        .aggregateByKey((growth_acc_initial, growth_acc_final),
                        utils.update_growth_acc,
                        utils.combine_growth_accs) \
        .mapValues(utils.calculate_growth) \
        .cache()

    min_max_rdd = minprice_rdd.join(maxprice_rdd)
    min_max_avgvol_rdd = min_max_rdd.join(avgvol_rdd) \
        .mapValues(lambda value: (value[0][0], value[0][1], value[1]))
    metrics_rdd = min_max_avgvol_rdd.join(growth_rdd) \
        .mapValues(lambda value: Row(growth=value[1],
                                 min_price=value[0][0],
                                 max_price=value[0][1],
                                 avg_volume=value[0][2],
                                 )) \
        .cache()

    ranked_rdd = metrics_rdd.sortBy(lambda row: row[1].growth, ascending=False) \
        .mapValues(pretty_print) \
        .take(10)

    for k, v in ranked_rdd:
        print(k, v)


if __name__ == "__main__":
    spark = utils.create_session("job1")
    sc = spark.sparkContext
    # sqlContext = SQLContext(sc)

    history_rdd = utils.load_data(spark, HISTORY_PATH)
    run_job(history_rdd)
