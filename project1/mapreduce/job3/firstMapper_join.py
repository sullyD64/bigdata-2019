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

    for row in data:
      if ("ticker" in row[0]): # skip headers
        continue
      if (len(row)==8 and (row[7]<=MIN_DATE or row[7]>=MAX_DATE)): # skip records outside desired time period
        continue

      ticker = row[0]
      value = ""
      flag = ""

      if len(row) == 5: # we are reading from legend
        flag = "0"
        value = [row[2] , row[3]]
      elif len(row) == 8: # we are reading from history
        flag = "1"
        value = [row[2], row[7]]
      print('%s\t%s\t%s' % (ticker, flag, value))

if __name__ == "__main__":
    main()
