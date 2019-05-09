import sys
import re

# tokens = re.compile(r'[^0-9a-zA-Z ]+', re.IGNORECASE)
#
#
# def clean(word: str):
#     return re.sub(tokens, '', word.lower())


def read_input(file):
    for line in file:
        yield line[15:19], line[87:92]


def main(separator='\t'):
    data = read_input(sys.stdin)
    # data = read_input([test])
    for year, temp in data:
        print("%s%s%s" % (year, separator, temp))


if __name__ == "__main__":
    main()
