import sys
import re

tokens = re.compile(r'[^0-9a-zA-Z ]+', re.IGNORECASE)


def clean(word: str):
    return re.sub(tokens, '', word.lower())


def read_input(file):
    for line in file:
        yield line.split()


def main(separator="\t"):
    data = read_input(sys.stdin)

    for words in data:
        bigrams = [' '.join(words[i: i + 2]) for i in range(0, len(words) - 1, 1)]
        for bigram in bigrams:
            print('%s\t%s' % (clean(bigram), 1))

if __name__ == '__main__':
    main()