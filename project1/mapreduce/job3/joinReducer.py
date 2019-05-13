from operator import itemgetter
from datetime import datetime
from statistics import mean
import sys, os, re, math

# input: TICKER FLAG VALUE
# VALUE = SECTOR or [CLOSING_PRICE, VOLUME, DATE]

def main(input_file):
    current_ticker = None
    ticker_sector_and_name= None

    def send_output(sector_name, flag, value):
        if (int(flag)==1):
            details = re.sub("[\s\[\]']" ,"", value).split(",")
            s_n = re.sub("[\[\]]" ,"", sector_name).split("', '")
            sector = s_n[0].replace("'","")
            company_name = s_n[1].replace("'","")
            date = details[1]
            price = details[0]
            print('%s\t%s\t%s\t%s' % ( company_name, date , price , sector))

    for line in input_file:
        ticker, flag, value = line.strip().split('\t')

        if current_ticker != ticker:
            # cambia ticker
            current_ticker = ticker
            ticker_sector_and_name = value if int(flag)==0 else "['N/A', 'N/A']"
        # arricchisci e stampa
        send_output(ticker_sector_and_name, flag, value)

    # arricchisci e stampa l'ultima linea se è uno storico
    send_output(ticker_sector_and_name, flag, value)


if __name__ == "__main__":
    main (sys.stdin)