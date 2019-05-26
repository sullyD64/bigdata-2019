import math
from datetime import datetime

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext


def create_session(app_name):
    return SparkSession.builder.master('local[4]').appName(app_name).getOrCreate()

def load_data(spark, path, preview=False):
    df = spark.read.format("csv") \
        .option("inferSchema", "true").option("header", "true") \
        .load(path)

    if preview:
        df.show(truncate=False)
    return df.rdd


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
    growth = math.floor((final_price - initial_price)
                        * 100 / initial_price)
    return growth

def prettify_growth(growth):
    growth_str = "+" + str(growth) + \
        "%" if growth >= 0 else str(growth) + "%"
    return growth_str
