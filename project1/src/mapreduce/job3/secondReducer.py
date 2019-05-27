from statistics import mean
import sys, re
import math

# input: SECTOR DATE [CLOSING_PRICE VOLUME]

class CompanyMetrics:
  def __init__(self):
    self.daily_prices_sums = []
    self.daily_price = 0
  
  def update(self, price):
    self.daily_price += price

  def update_sums(self):
    self.daily_prices_sums.append(self.daily_price)
    # TODO check if we can use only two elements on the list (we only need first and last to calculate growth)
    self.daily_price = 0
  
  def finalize(self):
    first_day_price = self.daily_prices_sums[0]
    last_day_price = self.daily_prices_sums[-1]
    self.growth = math.floor((last_day_price - first_day_price)*100 / first_day_price)
    
  def __str__(self):
    sign = 'p' if self.growth >= 0 else 'n'
    val = abs(self.growth)
    return str(val) + sign


def main(input_file):
  curr_company = None
  curr_year = None
  curr_date = None

  curr_metrics = CompanyMetrics()

  for line in input_file:
    company, date, price, sector = line.strip().split('\t')
    year = date.split("-")[0]

    if curr_date != date:
      if curr_date:
        curr_metrics.update_sums()
      curr_date = date

      if curr_year != year:
        if curr_year:
            curr_metrics.finalize()
            year_growth = str(curr_year) + ":" + str(curr_metrics)
            print('%s\t%s\t%s' % (curr_company, sector, year_growth))

        curr_year = year
        curr_metrics = CompanyMetrics()
        
    if curr_company != company:
        curr_company = company
    
    curr_metrics.update(float(price))

  # print last company annual report
  curr_metrics.update_sums()
  curr_metrics.finalize()
  year_growth = str(curr_year) + ":" + str(curr_metrics)
  print('%s\t%s\t%s' % (curr_company, sector, year_growth))

if __name__ == "__main__":
  main (sys.stdin)