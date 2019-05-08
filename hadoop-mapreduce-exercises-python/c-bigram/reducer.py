#from operator import itemgetter
import sys

current_bigram = None
current_count = 0
bigram = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    bigram, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_bigram == bigram:
        current_count += count
    else:
        if current_bigram:
            # write result to STDOUT
            print ('%s\t%s' % (current_bigram, current_count))
        current_count = count
        current_bigram = bigram

# do not forget to output the last bigram if needed!
if current_bigram == bigram:
    print ('%s\t%s' % (current_bigram, current_count))