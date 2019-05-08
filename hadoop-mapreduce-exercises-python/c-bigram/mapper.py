import sys
import re
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    line = re.sub("!|,", "", line)
    # split the line into words
    words = line.split()
    bigrams = [' '.join(words[i: i + 2]) for i in range(0, len(words)-1, 1)]
    # increase counters
    for bigram in bigrams:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # reducer step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print ('%s\t%s' % (bigram, 1))
        