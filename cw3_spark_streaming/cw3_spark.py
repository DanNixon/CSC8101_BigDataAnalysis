import datetime
import json
from pyspark import SparkContext, SparkConf, RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, Row


def get_spark_session_instance(spark_conf):
    """
    This method ensures that there is only ever one instance of the Spark SQL
    context. This prevents conflicts between executors.

    Code taken from example on this page of the Spark Streaming Guide:

    https://spark.apache.org/docs/latest/
    streaming-programming-guide.html#dataframe-and-sql-operations

    Arguments:
        spark_conf - SparkConf - The spark context configuration object.
        This should be the same as supplied to the StreamingContext object.

    Returns:
        The singleton instance of the Spark SQL SparkSession object.
    """
    if "sparkSessionSingletonInstance" not in globals():
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    return globals()["sparkSessionSingletonInstance"]


def send_to_cassandra(rdd, table_name):
    """
    Converts an RDD of Row instances into a DataFrame and appends it to the
    supplied table name. The Cassandra database address is defined in the
    "spark.cassandra.connection.host" key set in the SparkConf instance given
    to the get_spark_session_instance method.

    Arguments:
        rdd - RDD - An rdd of Row instances with filed names that match those
        in the target cassandra table.
        table_name - str - The name of the table that this rdd should be sent
        to.
    """
    # Get the singleton instance of SparkSession
    spark = get_spark_session_instance(rdd.context.getConf())

    # Convert the supplied rdd of Rows into a dataframe
    rdd_df = spark.createDataFrame(rdd)

    # Print the dataframe to the console for verification
    # rdd_df.show()

    # Write this dataframe to the supplied Cassandra table
    (rdd_df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table=table_name, keyspace="csc8101").save())


# Spark Streaming Setup

# Create a conf object to hold connection information
sp_conf = SparkConf()

# Set the cassandra host address - NOTE: you will have to set this to the
# address of your VM if you run this on the cluster. If you are running
# the streaming job locally on your VM you should set this to "localhost"
sp_conf.set("spark.cassandra.connection.host", "localhost")

# Create the spark context object.
# For local development it is very important to use "local[2]" as Spark
# Streaming needs 2 cores minimum to function properly
sc = SparkContext(master="local[2]", appName="Event Processing", conf=sp_conf)
sc.setLogLevel("OFF")

# Set the batch interval in seconds
BATCH_INTERVAL = 10

# Create the streaming context object
ssc = StreamingContext(sc, BATCH_INTERVAL)


#
# TASK 1
# Kafka setup
#

# Create receiver based stream
topic_name = "dev-stream"
# topic_name = "production"
client_id_for_broker = "120263697"
num_of_partition_to_consume_from = 1
raw_messages = KafkaUtils.createStream(ssc,
        "34.248.5.122:2181",
        client_id_for_broker,
        {topic_name: num_of_partition_to_consume_from})


#
# TASK 2
# Window the incoming batches
#

window = raw_messages.window(120, 60)

# window.pprint(10)


#
# TASK 3
# Convert each message from json into a dictionary
#

def json_to_event(data):
    json_data = json.loads(data[1])
    time = datetime.datetime.strptime(
        json_data["timestamp"], '%Y-%m-%dT%H:%M:%S')
    ts = int((time - datetime.datetime(1970, 1, 1)).total_seconds() * 1e3)
    return (json_data["client_id"], ts, json_data["url"]["topic"], json_data["url"]["page"])

events = window.map(json_to_event)

# events.pprint(10)


#
# TASK 4
# Find the minimum timestamp for window
#

def find_lowest_timestamp(rdd):
    ts = rdd.aggregate(float('inf'), lambda a, x: min(a, x[1]), lambda a, b: min(a, b))
    return sc.parallelize([ts])

timestamps = events.transform(find_lowest_timestamp).map(lambda t: (0, t))

timestamps.pprint(10)

# Record the timestamp in Cassandra table
timestamps_db = timestamps.map(lambda e: Row(dummy_key=1, timestamp=e[1]))
timestamps_db.foreachRDD(lambda rdd: send_to_cassandra(rdd, "timestamps"))

# Replace event timestamps with batch timestamps
events_with_key = events.map(lambda e: (0, (e[0], e[1], e[2], e[3])))
batch_timestamped_events_join = events_with_key.join(timestamps)

# batch_timestamped_events is a DStream of tuple (user ID, timestamp, topic, page)
batch_timestamped_events = batch_timestamped_events_join.map(lambda e: (e[1][0][0], e[1][1], e[1][0][2], e[1][0][3]))

# batch_timestamped_events.pprint(10)

#
# TASK 5
# Calculate the page visits for each client ID
#

# DStream of tuple ((user ID, timestamp, topic, page), count)
views_by_client = batch_timestamped_events.map(lambda e: ((e[0], e[1], e[2], e[3]), 1))

# Count views of each page for each client
client_page_views = views_by_client.reduceByKey(lambda a, b: a + b)

# Convert each element of the DStream into a Row
client_page_views_db = client_page_views.map(lambda e: Row(clientid=e[0][0], timestamp=e[0][1], topic=e[0][2], page=e[0][3], visits=e[1]))

client_page_views_db.pprint(10)

# Convert each RDD of Rows in the DStream into a DataFrame and send to Cassandra
client_page_views_db.foreachRDD(lambda rdd: send_to_cassandra(rdd, "client_pages_visited"))

#
# TASK 6
# Calculate the unique views per page
#

# Count unique views of each page
# DStream of tuple ((timestamp, topic, page), count)
page_unique_views = batch_timestamped_events.transform(lambda rdd: rdd.distinct()).map(lambda e: ((e[1], e[2], e[3]), 1)).reduceByKey(lambda a, b: a + b)

# Convert each element of the DStream into a Row
page_unique_views_db = page_unique_views.map(lambda e: Row(timestamp=e[0][0], topic=e[0][1], visits=e[1], page=e[0][2]))

page_unique_views_db.pprint(10)

# Convert each RDD of Rows in the DStream into a DataFrame and send to Cassandra
page_unique_views_db.foreachRDD(lambda rdd: send_to_cassandra(rdd, "top_pages"))

# Initiate the stream processing
ssc.start()
ssc.awaitTermination()
