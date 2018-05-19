#!/bin/bash
${HADOOP_HOME}/bin/hdfs dfs -rm -r /stream_input
java -jar ./jars/streamgenerator.jar hdfs://master:9000 /samples/stream.tsv  /stream_input 50 10 &
