from datetime import datetime
import sys, re, csv

# input: both files (historical_stocks.csv and historical_stock_prices.csv)
#        we refer to first file as "legend", and second file as "dataset"
# output: a triple of tab-delimited values, in which:
#       - first field always contains TICKER.
#       - second field is contains 'K' when reading from legend and contains "details" when reading from dataset.
#          "details" = [OPEN, CLOSE, LOW, HIGH, VOLUME, DATE]
#       - third field contains SECTOR when reading from legend and is empty when reading from dataset.

# historical_stock_prices.csv (8 fields): 
# (TICKER, OPEN, CLOSE, ADJ_CLOSE, LOW, HIGH, VOLUME, DATE)
#  0       1     2      3          4    5     6       7

# historical_stocks.csv (5 fields):
# (TICKER, EXCHANGE, NAME, SECTOR, INDUSTRY)
# 0        1         2     3       4

MIN_DATE = "2004-01-01"
MAX_DATE = "2018-12-31"

def read_input(file):
    for line in file:        
        yield list(csv.reader([line.strip()], delimiter=',', quotechar='"'))[0]

def main():
    infile = sys.stdin
    data = read_input(infile)

    for row in data:
      if ("ticker" in row[0]): # skip headers
        continue;

      ticker = row[0]
      sector = "-"
      details = "K"

      if len(row) == 5: # we are reading from legend
        sector = row[3]
      elif len(row) == 8: # we are reading from dataset
        if MIN_DATE <= row[7] <= MAX_DATE: # filter records outside desired time period
          details = [row[1], row[2], row[4], row[5], row[6], row[7]]
      print('%s\t%s\t%s' % (ticker,details,sector))

if __name__ == "__main__":
    main()
