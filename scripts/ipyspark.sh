#!/bin/bash

# This script will uset the env variables used to launch the notebook
# version of pyspark and will instead run it in a command line
# ipython interpreter

export PYSPARK_DRIVER_PYTHON=ipython
unset PYSPARK_DRIVER_PYTHON_OPTS

pyspark
