import sys
import datetime
import time


def read_input(file):
    for line in file:
        yield line.strip().split("|")


def main(separator='\t'):
    data = read_input(sys.stdin)
    for line in data:

        if(line[4] == "1"):
            callerNumber = line[0]
            start = time.mktime(datetime.datetime.strptime(line[2], "%Y-%m-%d %H:%M:%S").timetuple())
            end = time.mktime(datetime.datetime.strptime(line[3], "%Y-%m-%d %H:%M:%S").timetuple())
            duration = (end - start)/60
            print("%s%s%f" % (callerNumber, separator, duration))


if __name__ == "__main__":
    main()
