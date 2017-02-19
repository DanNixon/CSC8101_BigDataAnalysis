import logging
from pyspark import SparkContext


# Configure logging
logging.basicConfig(format="%(asctime)s: [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def run_from_ipython():
    """
    Test if the script was run from IPython.
    """
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


# Assume that if we are running from an IPython context it is one that already
# passed in a SparkContext.
# This allows the script to be run from IPython using : %run -r cw2.py
if not run_from_ipython():
    sc = SparkContext("local", "CW3")
    sc.setLogLevel(WARN)


#
# TASK 1
# Loading Netflix and cannonical movie names and building alias map
#

log.info("Starting task 1")

# TODO

log.info("Completed task 1")

