#!/usr/bin/env python
import numpy as np
import sys
import re
import math

# input: SECTOR DATE [CLOSING_PRICE VOLUME]


class YearMetrics:
    def __init__(self):
        self.daily_prices_sums = []
        self.daily_price = 0
        self.tot_volume = 0

    def update(self, price, volume):
        self.daily_price += price
        self.tot_volume += volume

    def update_day(self):
        self.daily_prices_sums.append(self.daily_price)
        self.daily_price = 0

    def finalize(self):
        first_day_price = self.daily_prices_sums[0]
        last_day_price = self.daily_prices_sums[-1]
        self.growth = math.floor(
            (last_day_price - first_day_price)*100 / first_day_price)
        self.avg_daily_price = round(np.average(self.daily_prices_sums), 4)
        # no further actions required for tot_volume

    def __str__(self):
        growth_str = "+" + str(self.growth) + \
            "%" if self.growth >= 0 else str(self.growth) + "%"
        return str([self.tot_volume, growth_str, self.avg_daily_price])


def main(input_file):
    curr_sector = None
    curr_year = None
    curr_date = None

    curr_metrics = YearMetrics()

    for line in input_file:
        sector, date_created, details = line.strip().split('\t')
        details = re.sub("[\s\[\]']", "", details).split(",")
        year = date_created.split("-")[0]

        if curr_date != date_created:
            if curr_date:
                curr_metrics.update_day()
            curr_date = date_created

            if curr_year != year:
                if curr_year:
                    curr_metrics.finalize()
                    print('%s\t%s\t%s' %
                          (curr_sector, curr_year, curr_metrics))

                curr_year = year
                curr_metrics = YearMetrics()

                if curr_sector != sector:
                    curr_sector = sector

        curr_metrics.update(float(details[0]), int(details[1]))

    # print last sector annual report
    curr_metrics.update_day()
    curr_metrics.finalize()
    print('%s\t%s\t%s' % (curr_sector, curr_year, curr_metrics))


if __name__ == "__main__":
    main(sys.stdin)
