#!/usr/bin/env python
from datetime import datetime
# from statistics import mean
import numpy as np 
import sys
import re
import math

# input: (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
#        0        1     2      3          4    5     6       7

NUM_RANKS = 10

class StockMetrics:
    def __init__(self):
        # growth metrics
        self.initial_price = None
        self.final_price = None
        self.growth = None
        # min and max price
        self.min_price = 99999999
        self.max_price = -99999999
        # transaction volume
        self.volumes = []
        self.avg_volume = None

    # row = [price_close, price_low, price_high, volume]
    def update(self, date_created, row):
        # growth metrics
        price_close = float(row[0])

        if self.initial_price == None:
            self.initial_price = price_close

        self.final_price = price_close

        # min and max price
        self.min_price = min(self.min_price, float(row[1]))
        self.max_price = max(self.max_price, float(row[2]))
        # transaction volume
        self.volumes.append(float(row[3]))

    def finalize(self):
        # calculate growth
        self.growth = math.floor(
            (self.final_price - self.initial_price)*100 / self.initial_price)

        # round min and max price
        self.min_price = round(self.min_price, 4)
        self.max_price = round(self.max_price, 4)

        # calculate average transaction volume
        self.avg_volume = round(np.average(self.volumes), 4)


class TopStocks:
    def __init__(self):
        self.stocks = []

    def update(self, ticker, metrics):
        candidate = TopStocksEntry(
            ticker, metrics.growth, metrics.min_price, metrics.max_price, metrics.avg_volume)
        length = len(self.stocks)
        if length > 0:
            worst_growth = self.stocks[-1].growth
        if length < NUM_RANKS:
            self.stocks.append(candidate)
        elif (length == NUM_RANKS and metrics.growth > worst_growth):
            self.stocks[-1] = candidate
        self.stocks.sort(key=lambda x: x.growth, reverse=True)


class TopStocksEntry:
    def __init__(self, ticker, growth, min_price, max_price, avg_volume):
        self.ticker = ticker
        self.growth = growth
        self.min_price = min_price
        self.max_price = max_price
        self.avg_volume = avg_volume

    def __str__(self):
        growth_str = "+" + str(self.growth) + \
            "%" if self.growth >= 0 else str(self.growth) + "%"
        return self.ticker + '\t' + str([growth_str, self.min_price, self.max_price, self.avg_volume])

    def __repr__(self):
        return str([self.ticker, self.growth])


def main(input_file):
    top_stocks = TopStocks()
    current_ticker = None
    metrics = StockMetrics()

    for line in input_file:
        ticker, date_created, details = line.strip().split('\t')
        details = re.sub("[\s\[\]']", "", details).split(",")
        date_created = datetime.strptime(date_created, "%Y-%m-%d").date()

        if current_ticker != ticker:
            if current_ticker:
                metrics.finalize()
                top_stocks.update(current_ticker, metrics)
            current_ticker = ticker
            metrics = StockMetrics()

        metrics.update(date_created, details)
    
    # Last line
    metrics.finalize()
    top_stocks.update(current_ticker, metrics)

    for i, entry in enumerate(top_stocks.stocks):
        print('%d\t%s' % (i+1, entry))


if __name__ == "__main__":
    main(sys.stdin)
