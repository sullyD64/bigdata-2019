import sys
from datetime import datetime

# input: (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
#        0        1     2      3          4    5     6       7

# calculate the following values among all rows with same TICKER between 1998 and 2018:
# - %INCREASE = difference(CLOSE in 2018, CLOSE in 1998)
# - MIN = min(LOW)
# - MAX = min(HIGH)
# - AVG_VOL = avg(VOLUME)

# goal: show the top 10 stocks (identified by TICKER) by their %INCREASE.
# output: (TICKER, (%INCREASE, MIN, MAX, AVG_VOL))

# strategy:
# - map by TICKER but filter out every row out of the date period (ticker, details)
# - reduce in the following way:
#     input comes sorted by TICKER, so all rows with same TICKER are consecutive.
#     to calculate %INCREASE, initialize min_date and max_date with DATE.
#     initialize MIN with LOW and MAX with HIGH
#     to calculate AVG_VOL, create an empty list with volumes
#     TODO ....

class StockDetails:
    def __init__(self, open_value, close_value, low, high, volume, date):
        self.open_value = open_value
        self.close_value = close_value
        self.low = low
        self.high = high
        self.volume = volume
        self.date = date

    def __str__(self):
        return str([self.open_value, self.close_value, self.low, self.high, self.volume, self.date])


def to_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")


MIN_DATE = to_date("1998-01-01")
MAX_DATE = to_date("2018-12-31")


def read_input(file):
    for line in file:
        yield line.strip().split(',')


def main(separator='\t'):
    infile = sys.stdin
    next(infile)    # skip the header
    data = read_input(infile)
    for row in data:

        date = to_date(row[7])

        if MIN_DATE <= date <= MAX_DATE:
            details = StockDetails(row[1], row[2], row[4], row[5], row[6], row[7])
            print("%s%s%s" % (row[0], separator, details))


if __name__ == "__main__":
    main()
