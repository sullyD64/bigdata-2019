#!/usr/bin/env python
import sys
import re
import math

# input: TICKER FLAG VALUE
# VALUE = [NAME, SECTOR] or [CLOSING_PRICE, DATE]


def main(input_file):
    current_ticker = None
    ticker_sector_and_name = None

    def send_output(info, flag, value):
        if (int(flag) == 1):
            details = re.sub("[\s\[\]']", "", value).split(",")
            name, sector = re.sub("[\[\]]", "", info).split("', '")
            name = name.replace("'", "")
            sector = sector.replace("'", "")
            date_created = details[1]
            price_close = details[0]
            print('%s\t%s\t%s\t%s' % (name, date_created, price_close, sector))

    for line in input_file:
        ticker, flag, value = line.strip().split('\t')

        if current_ticker != ticker:
           # ticker has changed
            current_ticker = ticker
            company_info = value if int(flag) == 0 else "['N/A', 'N/A']"
       
        send_output(company_info, flag, value)

   # print last line if comes from history
    send_output(company_info, flag, value)


if __name__ == "__main__":
    main(sys.stdin)
