#!/usr/bin/env python
from itertools import combinations
import sys
import re

# TREND, NAME, SECTOR


# decodes trend string
def pretty_format(trend_str=None):
    if trend_str:
        trend = re.sub("[\s\[\]']", "", trend_str).split(",")
        out_trend = []
        for year_growth in trend:
            year, growth = year_growth.split(':')
            sign = growth[-1]
            if sign == 'p':
                growth = "+" + growth[:1] + "%"
            elif sign == 'n':
                growth = "-" + growth[:1] + "%"
            out_trend.append(year + ':' + growth)
        return out_trend


class Company:
    def __init__(self, name=None, sector=None):
        self.name = name
        self.sector = sector
        self.trend = []

    def update(self, year_growth):
        self.trend.append(year_growth)

    def equals(self, that):
        return (self.name == that.name and self.sector == that.sector)

    def __repr__(self):
        return str([self.name, self.sector])


class SimilarTrendingCompanies:
    def __init__(self, trend=None):
        self.trend = pretty_format(trend)
        self.companies = []

    def update(self, company):
        self.companies.append(company)

    def generate_similar_couples(self):
        for couple in combinations(self.companies, r=2):
            if not(couple[0].sector == couple[1].sector):
                print('%s\t%s\t%s' %
                      (self.trend,
                       ':'.join([couple[0].name, couple[0].sector]),
                       ':'.join([couple[1].name, couple[1].sector])))


def main(input_file):
    curr_trend = None
    curr_trend_companies = SimilarTrendingCompanies()

    for line in input_file:
        trend, name, sector = line.strip().split('\t')

        if curr_trend != trend:
            if curr_trend:
                curr_trend_companies.generate_similar_couples()
            curr_trend = trend
            curr_trend_companies = SimilarTrendingCompanies(curr_trend)

        curr_trend_companies.update(Company(name, sector))

    # print last trend couples
    curr_trend_companies.generate_similar_couples()


if __name__ == "__main__":
    main(sys.stdin)
