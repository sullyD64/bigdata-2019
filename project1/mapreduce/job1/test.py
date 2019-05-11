import pandas as pd
import random
from pathlib import Path

# COMMAND USED FOR CLEANING LEGEND INPUT
# cat historical_stocks.csv | sed 's:\("\)\([^"]*\)\(,\)\([^"]*\):\2:g' > historical_stocks__cleaned.csv

# COMMAND USED FOR CREATING SAMPLE SUBSET FROM STOCKS
# tail -n +2 historical_stock_prices.csv | shuf -n 100 -o stocks_test.csv

# PRETTY-PRINT
# column <filename> -t -s,


# calculate the following values among all rows with same TICKER between 1998 and 2018:
# - %INCREASE = difference(avg(CLOSE in 2018), avg(CLOSE in 1998))
# - MIN_PRICE = min(LOW)
# - MAX_PRICE = max(HIGH)
# - AVG_VOL = avg(VOLUME)

# goal: show the top 10 stocks (identified by TICKER) by their %INCREASE.
# output: (TICKER, (%INCREASE, MIN_PRICE, MAX_PRICE, AVG_VOL))

# strategy:
# - map by TICKER but filter out every row out of the date period (ticker, details)
# - reduce in the following way:
#     input comes sorted by TICKER, so all rows with same TICKER are consecutive.
#     to calculate %INCREASE, fill up two lists records_first_year and records_last_year containing CLOSE prices, 
#       then calculate average values and subtract them
#     initialize MIN_PRICE with LOW and MAX_PRICE with HIGH
#     to calculate AVG_VOL, fill an empty list with VOLUMEs and calculate avg


INPUTDIR = Path(__file__).parent.parent.parent / "dataset"
STOCKS = INPUTDIR / "historical_stock_prices.csv"
LEGEND = INPUTDIR / "historical_stocks.csv"

if __name__ == '__main__':
    num_records_stocks = 20973890  # records in historical_stock_prices.csv
    num_records_legend = 6461  # records in historical_stocks.csv
    sample_size = 200

    # skip = sorted(random.sample(range(num_records_stocks-1), num_records_stocks-1 - sample_size))
    # df = pd.read_csv(STOCKS, skiprows=skip)

    # p = 0.01
    # df = pd.read_csv(str(STOCKS), header=0, skiprows=lambda i: i > 0 and random() > p)


