from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import datetime


def json_to_event(json_data):
    time = datetime.datetime.strptime(
        json_data["timestamp"], '%Y-%m-%dT%H:%M:%S')
    ts = int((time - datetime.datetime(1970, 1, 1)).total_seconds() * 1e3)
    return (json_data["client_id"], ts, json_data["url"]["topic"], json_data["url"]["page"])


def json_to_timestamp_visists(json_data):
    time = datetime.datetime.strptime(
        json_data["timestamp"], '%Y-%m-%dT%H:%M:%S')
    ts = int((time - datetime.datetime(1970, 1, 1)).total_seconds() * 1e3)
    return (ts, json_data["url"]["topic"], json_data["url"]["page"], int(json_data["visits"]))


class EventDatabase(object):

    def __init__(self, hostname, keyspace, replication_factor):
        self._keyspace = keyspace
        self._repl_factor = replication_factor
        self._cluster = Cluster([hostname])
        self._session = self._cluster.connect()

    def create_keyspace(self):
        self._session.execute("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{}'}}".format(
            self._keyspace, self._repl_factor))

    def drop_keyspace(self):
        self._session.execute("DROP KEYSPACE {}".format(self._keyspace))

    def create_tables(self):
        self._session.set_keyspace(self._keyspace)

        # Create a table with client ID as partition key, this ensures
        # uniform load distribution over all nodes on insertion (assuming mean
        # events generated in a time period is iniform over all active clients)
        # and that read operations only require access to a single node.
        self._session.execute("""
            CREATE TABLE IF NOT EXISTS client_pages_visited (
            clientid text,
            timestamp bigint,
            topic text,
            page text,
            visits counter,
            PRIMARY KEY (clientid, timestamp, topic, page)
            )
            """)

        # Table with timestamp and topic as partition key, ensures single node
        # reads and moderately distributed writes.
        self._session.execute("""
            CREATE TABLE IF NOT EXISTS top_pages (
            timestamp bigint,
            topic text,
            visits int,
            page text,
            PRIMARY KEY ((topic, timestamp), visits, page)
            )
            """)

        # Table used to keep track of timestamps in reverse chronological order
        self._session.execute("""
            CREATE TABLE IF NOT EXISTS timestamps (
            dummy_key int,
            timestamp bigint,
            PRIMARY KEY (dummy_key, timestamp)
            )
            WITH CLUSTERING ORDER BY (timestamp DESC)
            """)

    def record_visit(self, client_id, timestamp, topic, page):
        self._session.set_keyspace(self._keyspace)

        client_visit_insert = self._session.prepare("""
            UPDATE client_pages_visited
            SET visits = visits + 1
            WHERE clientid = ? AND timestamp = ? AND topic = ? AND page = ?
            """)
        self._session.execute(client_visit_insert,
                              (client_id, timestamp, topic, page))

    def record_visits_in_timestamp(self, timestamp, topic, page, visits):
        self._session.set_keyspace(self._keyspace)

        summary_insert = self._session.prepare("""
            INSERT INTO top_pages (timestamp, topic, page, visits)
            VALUES (?, ?, ?, ?)
            """)
        self._session.execute(summary_insert, (timestamp, topic, page, visits))

        timestamp_insert = self._session.prepare("""
            INSERT INTO timestamps (dummy_key, timestamp) VALUES (?, ?)
            """)
        self._session.execute(timestamp_insert, (1, timestamp))

    def query_client_page_visits(self, client_id, timestamp, topic):
        self._session.set_keyspace(self._keyspace)

        visits_query = self._session.prepare("""
            SELECT page, visits FROM client_pages_visited WHERE clientid = ? AND timestamp = ? AND topic = ?
            """)
        return self._session.execute(visits_query, (client_id, timestamp, topic))

    def query_top_pages_in_topic(self, timestamp, topic, count):
        self._session.set_keyspace(self._keyspace)

        top_pages_query = self._session.prepare("""
            SELECT page, visits FROM top_pages WHERE timestamp = ? AND topic = ? ORDER BY visits DESC LIMIT ?
            """)
        return self._session.execute(top_pages_query, (timestamp, topic, count))

    def query_recommend_for_client(self, client_id, topic, count):
        self._session.set_keyspace(self._keyspace)

        # Get last three batch timestamps
        timestamps = map(lambda r: r.timestamp, self._session.execute("""
            SELECT timestamp FROM timestamps LIMIT 3
            """))

        # Get a list of pages that are currently popular in the given topic
        candidate_pages = set(map(lambda r: r.page, self.query_top_pages_in_topic(
            timestamps[0], topic, count)))

        # Get a lit of pages the user has visited recently (in last three time
        # periods)
        visits_query = self._session.prepare("""
            SELECT page FROM client_pages_visited WHERE timestamp IN ? AND clientid = ? AND topic = ?
            """)
        user_pages = set(map(lambda r: r.page, self._session.execute(
            visits_query, (timestamps, client_id, topic))))

        # Generate recommendations
        recommendations = candidate_pages.difference(user_pages)

        return recommendations
