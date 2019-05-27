import sys
from datetime import datetime

MIN_DATE = datetime.strptime("1998-01-01", "%Y-%m-%d").date()
MAX_DATE = datetime.strptime("2018-12-31", "%Y-%m-%d").date()


def read_input(file):
    for line in file:
        yield line.strip().split(',')


def main():
    infile = sys.stdin
    next(infile)    # skip the header
    data = read_input(infile)
    for row in data:
        date_created = datetime.strptime(row[7], "%Y-%m-%d").date()
        if MIN_DATE <= date_created <= MAX_DATE:
            details = [row[2], row[4], row[5], row[6]]
            print("%s\t%s\t%s" % (row[0], date_created,  details))


if __name__ == "__main__":
    main()
