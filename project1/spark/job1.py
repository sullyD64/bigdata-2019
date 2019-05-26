from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime
import csv
import math

'''
calculate the following values among all rows with same TICKER between 1998 and 2018:
- %INCREASE = difference(avg(CLOSE in 2018), avg(CLOSE in 1998))
- MIN_PRICE = min(LOW)
- MAX_PRICE = max(HIGH)
- AVG_VOL = avg(VOLUME)

goal: show the top 10 stocks (identified by TICKER) by their %INCREASE.
output: (TICKER, (%INCREASE, MIN_PRICE, MAX_PRICE, AVG_VOL))

strategy:
- map by TICKER but filter out every row out of the date period (ticker, details)
- reduce in the following way:
    input comes sorted by TICKER, so all rows with same TICKER are consecutive.
    to calculate %INCREASE, fill up two lists records_first_year and records_last_year containing CLOSE prices,
      then calculate average values and subtract them
    initialize MIN_PRICE with LOW and MAX_PRICE with HIGH
    to calculate AVG_VOL, fill an empty list with VOLUMEs and calculate avg

'''

HISTORY_PATH = "hdfs://localhost:9000/user/bigdata/dataset_test_100k_header.csv"
LEGEND_PATH = "hdfs://localhost:9000/user/bigdata/historical_stocks.csv"
MIN_DATE = datetime.strptime("1998-01-01", "%Y-%m-%d").date()
MAX_DATE = datetime.strptime("2018-12-31", "%Y-%m-%d").date()


def create_session():
    return SparkSession.builder.master('local[2]').appName('job1').getOrCreate()


def load_data(spark, path):
    df = spark.read.format("csv") \
        .option("inferSchema", "true").option("header", "true") \
        .load(path)
    # df.show(truncate=False)
    return df.rdd


def prefilter_period(row):
    return MIN_DATE <= row["date"].date() <= MAX_DATE


def prefilter_columns(row):
    return (row.ticker, Row(price_close=row.close,
                            price_low=row.low,
                            price_high=row.high,
                            volume=row.volume,
                            date_created=row["date"].date()))


# compares two dates and returns the date-price pair associated to the date with the best value
# (default criterium is: earlier date wins, set initial=false to pick later date)
def compare_dateprices(dp_a, dp_b, initial=True):
    date_a, date_b = dp_a[0], dp_b[0]
    if initial:
        best_dp = dp_a if date_a <= date_b else dp_b
    else:
        best_dp = dp_a if date_a > date_b else dp_b
    return best_dp


# updates one growth accumulator
def update_growth_acc(acc, row_dp):
    initial_dp, final_dp = acc[0], acc[1]
    new_initial_dp = compare_dateprices(initial_dp, row_dp)
    new_final_dp = compare_dateprices(initial_dp, row_dp, initial=False)
    return (new_initial_dp, new_final_dp)


# combines two growth accumulators into one with the lowest initial date and highest final date
def combine_growth_accs(acc_a, acc_b):
    initial_dp_a, final_dp_a = acc_a[0], acc_a[1]
    initial_dp_a, final_dp_b = acc_b[0], acc_b[1]
    combined_initial_dp = compare_dateprices(initial_dp_a, initial_dp_a)
    combined_final_dp = compare_dateprices(
        final_dp_a, final_dp_b, initial=False)
    return (combined_initial_dp, combined_final_dp)


def calculate_growth(acc):
    initial_price, final_price = acc[0][1], acc[1][1]
    growth = math.floor((final_price - initial_price) * 100 / initial_price)
    # growth_str = "+" + str(growth) + \
    #     "%" if growth >= 0 else str(growth) + "%"
    return growth


def run_job(rdd):
    rdd = rdd.filter(prefilter_period) \
        .map(prefilter_columns)

    minprice_rdd = rdd.map(lambda row: (row[0], row[1].price_low)) \
        .reduceByKey(min) \
        .mapValues(lambda x: round(x, 4)) \
        .cache()
    # .mapValues(lambda x: Row(min_price=round(x, 4))) \

    maxprice_rdd = rdd.map(lambda row: (row[0], row[1].price_high)) \
        .reduceByKey(max) \
        .mapValues(lambda x: round(x, 4)) \
        .cache()
    # .mapValues(lambda x: Row(max_price=round(x, 4))) \

    # initialize accumulator
    # (runningSum, runningCount)
    avgvol_acc = (0, 0)
    avgvol_rdd = rdd.map(lambda row: (row[0], row[1].volume)) \
        .aggregateByKey(avgvol_acc,
                        lambda acc, x: (acc[0] + x, acc[1] + 1),
                        lambda acc_a, acc_b: (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1])) \
        .mapValues(lambda acc: round(acc[0]/acc[1], 4)) \
        .cache()
    # .mapValues(lambda acc: Row(avg_volume=(round(acc[0]/acc[1], 4)))) \

    # initialize accumulator
    # ((initial_date, initial_price),(final_date, final_price))
    growth_acc1 = (MAX_DATE, 0)
    growth_acc2 = (MIN_DATE, 0)
    growth_rdd = rdd.map(lambda row: (row[0], (row[1].date_created, row[1].price_close))) \
        .aggregateByKey((growth_acc1, growth_acc2),
                        update_growth_acc,
                        combine_growth_accs) \
        .mapValues(calculate_growth) \
        .cache()
    # .mapValues(lambda acc: Row(growth=calculate_growth(acc))) \

    min_max_rdd = minprice_rdd.join(maxprice_rdd)
    min_max_avgvol_rdd = min_max_rdd.join(avgvol_rdd) \
        .mapValues(lambda x: (x[0][0], x[0][1], x[1]))
    metrics_rdd = min_max_avgvol_rdd.join(growth_rdd) \
        .mapValues(lambda x: Row(growth=x[1],
                                 min_price=x[0][0],
                                 max_price=x[0][1],
                                 avg_volume=x[0][2],
                                 )) \
        .cache()

    ranked_rdd = metrics_rdd.sortBy(lambda x: x[1].growth, ascending=False) \
        .take(10)


if __name__ == "__main__":
    spark = create_session()
    sc = spark.sparkContext
    # sqlContext = SQLContext(sc)
    history_rdd = load_data(spark, HISTORY_PATH)
    run_job(history_rdd)
