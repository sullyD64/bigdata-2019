
# TODO TODO TODO

from itertools import groupby
from operator import itemgetter
import sys

DEFAULT_RANK = 20


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def main(argv, separator='\t'):
    rank_length = int(argv[1]) if len(argv) > 1 else DEFAULT_RANK
    data = read_mapper_output(sys.stdin, separator=separator)

    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items

    wordcounts = []
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            wordcounts.append((current_word, total_count))
        except ValueError:
            pass

    wordcounts.sort(key=itemgetter(1), reverse=True)
    for key, value in wordcounts[:rank_length]:
        print("%s%s%d" % (key, separator, value))


if __name__ == "__main__":
    main(sys.argv)
