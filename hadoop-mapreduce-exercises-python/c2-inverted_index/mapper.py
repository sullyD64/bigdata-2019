import sys
import re

#TODO

tokens = re.compile(r'[^0-9a-zA-Z]+', re.IGNORECASE)


def clean(word: str):
    return re.sub(tokens, ' ', word.lower())


def read_input(file):
    for line in file:
        index , strings = line.split('\t')
        strings = clean(strings).split()
        yield index , strings


def main(separator="\t"):
    data = read_input(sys.stdin)

    for index , words in data:

        for word in words:
            print('%s%s%s' % (word, separator, index))


if __name__ == '__main__':
    main()