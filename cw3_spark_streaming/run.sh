#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SPARK=spark-submit
ARGS="--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0"

$SPARK $ARGS "$DIR/cw3_spark.py"
