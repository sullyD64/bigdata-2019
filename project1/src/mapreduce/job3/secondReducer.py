#!/usr/bin/env python
import sys
import re
import math

# input: SECTOR DATE [CLOSING_PRICE VOLUME]


class YearMetrics:
    def __init__(self):
        self.daily_price_sum = 0
        self.initial_price = None
        self.final_price = None

    def update(self, price):
        self.daily_price_sum += price

    def update_day(self):
        if self.initial_price == None:
            self.initial_price = self.daily_price_sum
        self.final_price = self.daily_price_sum
        self.daily_price_sum = 0

    def finalize(self):
        self.growth = math.floor(
            (self.final_price - self.initial_price)*100 / self.initial_price)

    def __str__(self):
        # encoding necessary for sorting correctly growth values in next step
        # p=positive, n=negative
        sign = 'p' if self.growth >= 0 else 'n'
        val = abs(self.growth)
        return str(val) + sign


def main(input_file):
    curr_company = None
    curr_sector = None
    curr_year = None
    curr_date = None

    curr_metrics = YearMetrics()

    for line in input_file:
        company, date_created, price_close, sector = line.strip().split('\t')
        year = date_created.split("-")[0]

        if curr_date != date_created:
            if curr_date:
                curr_metrics.update_day()
            curr_date = date_created

            if curr_year != year:
                if curr_year:
                    curr_metrics.finalize()
                    year_growth = str(curr_year) + ":" + str(curr_metrics)
                    print('%s\t%s\t%s' %
                          (curr_company, curr_sector, year_growth))

                curr_year = year
                curr_metrics = YearMetrics()

        if curr_company != company:
            curr_company = company

        if curr_sector != sector:
            curr_sector = sector

        curr_metrics.update(float(price_close))

    # print last company annual report
    curr_metrics.update_day()
    curr_metrics.finalize()
    year_growth = str(curr_year) + ":" + str(curr_metrics)
    print('%s\t%s\t%s' % (curr_company, sector, year_growth))


if __name__ == "__main__":
    main(sys.stdin)
