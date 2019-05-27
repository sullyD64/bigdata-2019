hadoop jar tools/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
-mapper "python /home/lorenzo/git/bigdata-2019/hadoop-mapreduce-exercises-python/a-wordcount/mapper.py" \
-reducer "python /home/lorenzo/git/bigdata-2019/hadoop-mapreduce-exercises-python/a-wordcount/reducer.py" \
-input "hdfs://localhost:9000/user/bigdata/hamlet.txt" \
-output "hdfs://localhost:9000/user/bigdata/out"