#!/usr/bin/env python
import sys

# COMPANY SECTOR YEAR:GROWTH
# 0       1      2


class Company:
    def __init__(self, name=None, sector=None):
        self.name = name
        self.sector = sector
        self.trend = []

    def update(self, year_growth):
        self.trend.append(year_growth)

    def equals(self, that):
        return (self.name == that.name and self.sector == that.sector)


def read_input(file):
    for line in file:
        yield line.strip().split('\t')


def main():
    infile = sys.stdin
    data = read_input(infile)

    curr_cs = Company()
    for row in data:
        new_cs = Company(row[0], row[1])

        if curr_cs.equals(new_cs):
            curr_cs.update(row[2])
        else:
            if curr_cs.name:
                print('%s\t%s\t%s' %
                      (curr_cs.trend, curr_cs.name, curr_cs.sector))
            new_cs.update(row[2])
            curr_cs = new_cs

    print('%s\t%s\t%s' % (curr_cs.trend, curr_cs.name, curr_cs.sector))


if __name__ == "__main__":
    main()
