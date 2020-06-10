#!/bin/bash

HDFS="hadoop fs"

streaming_dir="/spark/streaming"

$HDFS -rm "${streaming_dir}"'/tmp/*' > /dev/null 2>&1
$HDFS -rm "${streaming_dir}"'/*' > /dev/null 2>&1

while [ 1 ]; do
	python ./sample_web_log.py > test.log
	time=$(date "+%Y%m%d%H%M%S")
	tmplog="access.${time}.log"
	
	$HDFS -put test.log ${streaming_dir}/tmp/$tmplog
	$HDFS -mv ${streaming_dir}/tmp/$tmplog ${streaming_dir}/

	echo "`date "+%Y-%m-%d %H:%M:%S"` put $tmplog to HDFS succeed"
	sleep 1
done
