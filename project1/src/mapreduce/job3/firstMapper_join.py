#!/usr/bin/env python
import sys
import csv

MIN_DATE = "2016-01-01"
MAX_DATE = "2018-12-31"


def read_input(file):
    for line in file:
        yield list(csv.reader([line.strip()]))[0]


def main():
    infile = sys.stdin
    data = read_input(infile)

    for row in data:
        if ("ticker" in row[0]):  # skip headers
            continue
        # skip records outside desired time period
        if (len(row) == 8 and (row[7] <= MIN_DATE or row[7] >= MAX_DATE)):
            continue

        ticker = row[0]
        values = ""
        flag = ""

        if len(row) == 5:  # we are reading from legend
            flag = "0"
            values = [row[2], row[3]]
        elif len(row) == 8:  # we are reading from history
            flag = "1"
            values = [row[2], row[7]]
        print('%s\t%s\t%s' % (ticker, flag, values))


if __name__ == "__main__":
    main()
