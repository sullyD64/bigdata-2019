from datetime import datetime
from statistics import mean
import sys, re
import math

# input: (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
#        0        1     2      3          4    5     6       7

NUM_RANKS = 10
FIRST_YEAR = 1998
LAST_YEAR = 2018


class StockMetrics:
    def __init__(self):
        # growth metrics
        self.records_first_year = []
        self.records_last_year = []
        self.oldest_record = (datetime.max, None)
        self.newest_record = (datetime.min, None)
        self.growth = None
        # min and max price
        self.min_price = 99999999
        self.max_price = -99999999
        # transaction volume
        self.volumes = []
        self.avg_volume = None

    # row = [opening_price, closing_price, min_price, max_price, volume, date]
    def update(self, row):        
        # growth metrics
        date = datetime.strptime(row[5], "%Y-%m-%d")
        closing_price = float(row[1])
        if date.year == FIRST_YEAR:
            self.records_first_year.append(closing_price)
        elif date.year == LAST_YEAR:
            self.records_last_year.append(closing_price)
        if (date < self.oldest_record[0]) and (date.year > FIRST_YEAR):
            self.oldest_record = (date, closing_price)
        if (date > self.newest_record[0]) and (date.year < LAST_YEAR):
            self.newest_record = (date, closing_price)
        # min and max price
        self.min_price = min(self.min_price, float(row[2]))
        self.max_price = max(self.max_price, float(row[3]))
        # transaction volume
        self.volumes.append(float(row[4]))

    def finalize(self):
        # calculate growth
        if len(self.records_first_year) != 0:
            initial_price = mean(self.records_first_year)
        else:
            initial_price = self.oldest_record[1]
        
        if len(self.records_last_year) != 0:
            final_price = mean(self.records_last_year)
        else:
            final_price = self.newest_record[1]
        self.growth = math.floor((final_price - initial_price)*100 / initial_price)
        # calculate average transaction volume
        self.avg_volume = mean(self.volumes)


class TopStocks:
    def __init__(self):
        self.stocks = []
    
    def update(self, ticker, metrics):
        candidate = TopStocksEntry(ticker, metrics.growth, metrics.min_price, metrics.max_price, metrics.avg_volume)
        length = len(self.stocks)
        if length > 0:
            lowest_stock_growth = self.stocks[-1].growth
            
        if length < NUM_RANKS:
            self.stocks.append(candidate)
        elif (length == NUM_RANKS and metrics.growth > lowest_stock_growth):
            self.stocks[-1] = candidate
        self.stocks.sort(key = lambda x:x.growth, reverse=True)


class TopStocksEntry:
    def __init__(self, ticker, growth, min_price, max_price, avg_volume):
        self.ticker = ticker
        self.growth = growth
        self.min_price = min_price
        self.max_price = max_price
        self.avg_volume = avg_volume

    def __str__(self):
        growth_str = "+" + str(self.growth) + "%" if self.growth > 0 else str(self.growth) + "%"
        return self.ticker + '\t' + str([growth_str, self.min_price, self.max_price, self.avg_volume])

    def __repr__(self):
        return str([self.ticker, self.growth])


def main(input_file, separator='\t'):
    top_stocks = TopStocks()
    current_ticker = None
    metrics = StockMetrics()

    for line in input_file:
        ticker, details = line.strip().split('\t', 1)
        details = re.sub("[\s\[\]']" ,"", details).split(",")

        if current_ticker != ticker:
            if current_ticker:
                metrics.finalize()
                top_stocks.update(current_ticker, metrics)
                # print(current_ticker, metrics.growth, len(top_stocks.stocks), top_stocks.stocks)
                # print(current_ticker, metrics.growth, metrics.oldest_record, metrics.newest_record)
            current_ticker = ticker
            metrics = StockMetrics()

        metrics.update(details)
    # Last line
    metrics.finalize()
    top_stocks.update(current_ticker, metrics) 
    # print(current_ticker, metrics.growth, len(top_stocks.stocks), top_stocks.stocks)
    # print(current_ticker, metrics.growth, metrics.oldest_record, metrics.newest_record)
    
    for i, entry in enumerate(top_stocks.stocks):
        print('%d\t%s' % (i+1, entry))


if __name__ == "__main__":
    main (sys.stdin)