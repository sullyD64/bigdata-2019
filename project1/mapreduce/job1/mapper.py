import sys
from datetime import datetime

MIN_DATE = "1998-01-01"
MAX_DATE = "2018-12-31"


def read_input(file):
    for line in file:
        yield line.strip().split(',')


def main(separator='\t'):
    infile = sys.stdin
    next(infile)    # skip the header
    data = read_input(infile)
    for row in data:
        if MIN_DATE <= row[7] <= MAX_DATE:
            details = [row[1], row[2], row[4], row[5], row[6], row[7]]
            print("%s%s%s" % (row[0], separator, details))


if __name__ == "__main__":
    main()
