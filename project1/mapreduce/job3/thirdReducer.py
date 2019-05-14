from itertools import combinations
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

  def __repr__(self):
    return str([self.name, self.sector])
  
    
class SimilarTrendingCompanies:
  def __init__(self, trend=None):
    self.trend = trend
    self.companies = []

  def update(self, company):
    self.companies.append(company)

  def generate_similar_couples(self):
    for couple in combinations(self.companies, r=2): 
      if not(couple[0].sector == couple[1].sector):
        print('%s\t%s\t%s' % (couple[0].name, couple[1].name, self.trend))


def main(input_file):
  curr_trend = None
  curr_trend_companies = SimilarTrendingCompanies()

  for line in input_file:
    trend, name, sector = line.strip().split('\t')
  

    if curr_trend != trend:
      if curr_trend:
        # CALCOLO PRODOTTO CARTESIANO
        # STAMPA FILTRATA CON CICLO
        print("HO FINITO")
        curr_trend_companies.generate_similar_couples()
        pass
      curr_trend = trend
      curr_trend_companies = SimilarTrendingCompanies(curr_trend)

    curr_trend_companies.update(Company(name, sector))
    # print('%s\t%s' % (line.strip() , len(curr_trend_companies.companies)))

  # CALCOLO PRODOTTO CARTESIANO
  # STAMPA FILTRATA CON CICLO
  curr_trend_companies.generate_similar_couples()
  print("HO FINITO")

if __name__ == "__main__":
    main(sys.stdin)

