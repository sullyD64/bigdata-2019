from operator import itemgetter
from datetime import datetime
from statistics import mean
import sys, os, re, math

# historical_stock_prices.csv (8 fields): 
# (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
#  0       1     2      3          4    5     6       7

# historical_stocks.csv (5 fields):
# (TICKER, EXCHANGE, NAME, SECTOR, INDUSTRY)
# 0        1         2     3       4

def main(input_file, separator='\t'):
    current_ticker = None
    ticker_sector = None

    for line in input_file:
        ticker, flag, value = line.strip().split(separator)

        if current_ticker != ticker:
            # cambia ticker
            current_ticker = ticker
            ticker_sector = value if int(flag)==0 else "__uncategorized__"

        # arricchisci e stampa se è uno storico
        if (int(flag)==1):
            details = re.sub("[\s\[\]']" ,"", value).split(",")
            details.append(ticker_sector)
            print('%s\t%s' % (current_ticker, details))

    # arricchisci e stampa l'ultima linea se è uno storico
    if (int(flag)==1):
        details = re.sub("[\s\[\]']" ,"", value).split(",")
        details.append(ticker_sector)
        print('%s\t%s' % (current_ticker, details))


if __name__ == "__main__":
    main (sys.stdin)