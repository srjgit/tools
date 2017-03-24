#!/bin/bash

user=$1
skey=$2
sdt=$3
edt=$4
hdfs dfs -rm -r -f /tmp/SearchResult-${user}
spark-submit --conf spark.yarn.executor.memoryOverhead=4000 --driver-memory 4G --driver-cores 4 --executor-memory 4G --executor-cores 4 --master yarn --deploy-mode cluster searchHdfsRaw.py $sdt $edt $skey $user
hdfs dfs -getmerge /tmp/SearchResult-${user}/part-* /tmp/searchresult-${user}.txt

