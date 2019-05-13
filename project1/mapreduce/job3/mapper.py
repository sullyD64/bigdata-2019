from datetime import datetime
import sys, re, csv

# historical_stock_prices.csv (8 fields): 
# (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
#  0       1     2      3          4    5     6       7

# historical_stocks.csv (5 fields):
# (TICKER, EXCHANGE, NAME, SECTOR, INDUSTRY)
# 0        1         2     3       4

MIN_DATE = "2016-01-01"
MAX_DATE = "2018-12-31"

def read_input(file):
      for line in file:        
            yield list(csv.reader([line.strip()]))[0]

def main():
      infile = sys.stdin
      data = read_input(infile)
      current_company = None
      
      for row in data:
            print(row)

if __name__ == "__main__":
    main()
