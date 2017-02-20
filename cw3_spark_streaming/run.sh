#!/bin/bash

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 cw3_spark.py
