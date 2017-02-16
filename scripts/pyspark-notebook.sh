#!/bin/bash

# This script will launch pyspark in an ipython notebook

export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip="*"' pyspark

pyspark
