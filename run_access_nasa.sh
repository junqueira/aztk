#!/bin/bash

file="$1"
#wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
#hdfs dfs -put -f $file /nasa/95/

fonte="nasa_access"
config_file=".config/config-hom.properties"

export SPARK_MAJOR_VERSION=2
time spark-submit --driver-memory 16g \
    target/scala-2.11/nasa-test-assembly-1.0.jar \
    --fonte $fonte \
    --config $config_file \
    --file $file