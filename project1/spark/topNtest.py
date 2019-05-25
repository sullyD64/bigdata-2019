from pyspark import SparkContext, SparkConf
import re

tokens = re.compile(r'[^0-9a-zA-Z]+', re.IGNORECASE)


def clean(line):
    line = re.sub(tokens, ' ', line.lower()).strip()
    return line.strip().split(' ')


conf = SparkConf().setAppName("topN")
sc = SparkContext(conf=conf)

contentRDD = sc.textFile("hdfs://localhost:9000/user/bigdata/hamlet.txt")

wordcounts = contentRDD.flatMap(lambda x: clean(x)) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .collect()

wordcounts_sorted = wordcounts.sort(key=lambda x: x[1], reverse=True)

for wc in wordcounts[:10]:
    print(wc)
