import sys

#TODO

current_bigram = None
current_count = 0
bigram = None

for line in sys.stdin:
    line = line.strip()

    bigram, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_bigram == bigram:
        current_count += count
    else:
        if current_bigram:
            print('%s\t%s' % (current_bigram, current_count))
        current_count = count
        current_bigram = bigram

if current_bigram == bigram:
    print('%s\t%s' % (current_bigram, current_count))
