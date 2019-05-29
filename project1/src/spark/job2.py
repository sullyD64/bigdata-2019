import csv
import math
from datetime import datetime

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

# HISTORY_PATH = "hdfs://localhost:9000/user/user33/testset/dataset_test_100k_header.csv"
HISTORY_PATH = "hdfs://localhost:9000/user/user33/dataset/historical_stock_prices.csv"
LEGEND_PATH = "hdfs://localhost:9000/user/user33/dataset/historical_stocks.csv"
MIN_DATE = datetime.strptime("2004-01-01", "%Y-%m-%d").date()
MAX_DATE = datetime.strptime("2018-12-31", "%Y-%m-%d").date()


def filter_period(row):
    return MIN_DATE <= row["date"].date() <= MAX_DATE


def select_columns_history(row):
    return (row.ticker, Row(price_close=row.close,
                            volume=row.volume,
                            date_created=row["date"].date()))


def select_columns_legend(row):
    return (row.ticker, Row(sector=row.sector))


def pretty_print(row):
    return str([utils.prettify_growth(row.growth), row.avg_daily_price, row.tot_volume])


def run_job(history_rdd, legend_rdd):
    history_rdd = history_rdd.filter(filter_period) \
        .map(select_columns_history)

    legend_rdd = legend_rdd.map(select_columns_legend)

    sectoryear_rdd = legend_rdd.join(history_rdd) \
        .map(lambda row: ((row[1][0].sector, row[1][1].date_created.year), row[1][1]))

    totvolume_rdd = sectoryear_rdd.map(lambda row: (row[0], row[1].volume)) \
        .reduceByKey(lambda x, y: x + y) \
        .cache()

    dailypricessum_rdd = sectoryear_rdd \
        .map(lambda row: ((row[0][0], row[0][1], row[1].date_created), row[1].price_close)) \
        .reduceByKey(lambda x, y: x + y)

    # initialize accumulator
    # (runningSum, runningCount)
    avgdailyprice_acc = (0, 0)
    avgdailyprice_rdd = dailypricessum_rdd.map(lambda row: ((row[0][0], row[0][1]), row[1])) \
        .aggregateByKey(avgdailyprice_acc,
                        lambda acc, x: (acc[0] + x, acc[1] + 1),
                        lambda acc_a, acc_b: (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1])) \
        .mapValues(lambda acc: round(acc[0]/acc[1], 4)) \
        .cache()

    # initialize accumulator
    # ((initial_date, initial_price),(final_date, final_price))
    growth_acc_initial = (MAX_DATE, 0)
    growth_acc_final = (MIN_DATE, 0)
    growth_rdd = dailypricessum_rdd.map(lambda row: ((row[0][0], row[0][1]), (row[0][2], row[1]))) \
        .aggregateByKey((growth_acc_initial, growth_acc_final),
                        utils.update_growth_acc,
                        utils.combine_growth_accs) \
        .mapValues(utils.calculate_growth) \
        .cache()


    avgdailyprice_growth_rdd = avgdailyprice_rdd.join(growth_rdd)
    metrics_rdd = avgdailyprice_growth_rdd.join(totvolume_rdd) \
        .mapValues(lambda value: Row(growth=value[0][1],
                                     avg_daily_price=value[0][0],
                                     tot_volume=value[1],
                                     )) \
        .mapValues(pretty_print) \
        .sortBy(lambda row: row[0]) \
        .collect()

    for kv in metrics_rdd:
        print(kv)


if __name__ == "__main__":
    spark = utils.create_session("job2")
    sc = spark.sparkContext
    # sqlContext = SQLContext(sc)

    history_rdd = utils.load_data(spark, HISTORY_PATH, preview=False)
    legend_rdd = utils.load_data(spark, LEGEND_PATH, preview=False)
    run_job(history_rdd, legend_rdd)
