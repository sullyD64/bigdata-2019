from operator import itemgetter
from datetime import datetime
from statistics import mean
import sys, os, re, math

# input: SECTOR DATE [CLOSING_PRICE VOLUME]

FIRST_YEAR = 2004
LAST_YEAR = 2018

def main(input_file, separator='\t'):
    current_sector = None
    current_company = None
    current_year = None
    current_date = None
    FIRST_DAY_OF_YEAR_PRICE = None
    last_price = 0
    def print_report(year, firs_price, last_price, company):
        TOTAL_GROWTH = 100*(last_price - firs_price)/firs_price
        print('%s\t%s\t%s' % (company,year ,TOTAL_GROWTH))

    for line in input_file:
        company, date, price , sector = line.strip().split('\t')
        year = date.split("-")[0]
        if current_year != year:
            if current_year:
                print_report(year, FIRST_DAY_OF_YEAR_PRICE, last_price, current_company)

            current_year = year
            FIRST_DAY_OF_YEAR_PRICE = float(price)

            if current_company != company:  
                # devo cambiare il nome del settore 
                current_company = company

        last_price = float(price)

    print_report(year, FIRST_DAY_OF_YEAR_PRICE, last_price, current_company)


if __name__ == "__main__":
    #main(open(os.path.join(sys.path[0], "reducerinput.txt")))
    main (sys.stdin)