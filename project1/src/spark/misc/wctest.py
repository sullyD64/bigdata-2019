from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)

contentRDD = sc.textFile("hdfs://localhost:9000/user/bigdata/words.txt")

wordcounts = contentRDD.flatMap(lambda x: x.split(' ')) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .collect()

for wc in wordcounts:
    print(wc)
