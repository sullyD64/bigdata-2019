from operator import itemgetter
from datetime import datetime
from statistics import mean
import sys, os, re, math

# input: SECTOR DATE [CLOSING_PRICE VOLUME]

def main(input_file):
    current_sector = None
    current_company = None
    current_year = None
    current_date = None
    FIRST_DAY_OF_YEAR_PRICE = None
    last_price = 0
    company_closing_prices = []


    def get_total_year_growth(year , first_price, last_price):
        total_growth = math.floor(100*(last_price - first_price)/first_price)
        return year+":"+str(total_growth)

    for line in input_file:
        company, date, price , sector = line.strip().split('\t')
        year = date.split("-")[0]

        if current_company != company:  
            if current_company:
                company_closing_prices.append(get_total_year_growth(current_year,FIRST_DAY_OF_YEAR_PRICE,last_price))  
                print('%s\t%s\t%s' % (company_closing_prices , sector, current_company))

            current_company = company
            company_closing_prices = []
            current_year = year 
            FIRST_DAY_OF_YEAR_PRICE = float(price)

        if current_year != year:
            if current_year:
                company_closing_prices.append(get_total_year_growth(current_year,FIRST_DAY_OF_YEAR_PRICE,last_price))  

            current_year = year 
            FIRST_DAY_OF_YEAR_PRICE = float(price)

        last_price = float(price)
    
                    
    company_closing_prices.append(get_total_year_growth(current_year,FIRST_DAY_OF_YEAR_PRICE,last_price))  
    print('%s\t%s\t%s' % (company_closing_prices , sector, current_company))


if __name__ == "__main__":
    main (sys.stdin)