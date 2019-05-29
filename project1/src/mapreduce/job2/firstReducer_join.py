#!/usr/bin/env python
import sys
import re

# input: TICKER FLAG VALUE
# VALUE = SECTOR or [CLOSING_PRICE, VOLUME, DATE]


def main(input_file):
    current_ticker = None

    def send_output(ticker, flag, value):
        if (int(flag) == 1):
            value = re.sub("[\s\[\]']", "", value).split(",")
            date_created = value[-1]
            print('%s\t%s\t%s' % (sector, date_created, value[:-1]))

    for line in input_file:
        ticker, flag, value = line.strip().split('\t')

        if current_ticker != ticker:
            # ticker has changed
            current_ticker = ticker
            sector = value if int(flag) == 0 else "N/A"

        send_output(ticker, flag, value)

    # print last line if comes from history
    send_output(ticker, flag, value)


if __name__ == "__main__":
    main(sys.stdin)
