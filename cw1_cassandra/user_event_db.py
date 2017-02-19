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

    def record_visit(self, client_id, timestamp, topic, page):
        self._session.set_keyspace(self._keyspace)

        prepared = self._session.prepare("""
            INSERT INTO client_pages_visited (clientid, timestamp, topic, page)
            VALUES (?, ?, ?, ?)
            """)
        self._session.execute(prepared, (client_id, timestamp, topic, page))

    def record_visits_in_timestamp(self, timestamp, topic, page, visits):
        self._session.set_keyspace(self._keyspace)

        prepared = self._session.prepare("""
            INSERT INTO top_pages (timestamp, topic, page, visits)
            VALUES (?, ?, ?, ?)
            """)
        self._session.execute(prepared, (timestamp, topic, page, visits))

    def query_client_page_visits(self, client_id, timestamp, topic):
        self._session.set_keyspace(self._keyspace)

        prepared = self._session.prepare("""
            SELECT page FROM client_pages_visited WHERE clientid = ? AND timestamp = ? AND topic = ?
            """)
        return self._session.execute(prepared, (client_id, timestamp, topic))

    def query_top_pages_in_topic(self, timestamp, topic, count):
        self._session.set_keyspace(self._keyspace)

        prepared = self._session.prepare("""
            SELECT page, visits FROM top_pages WHERE timestamp = ? AND topic = ? ORDER BY visits DESC LIMIT ?
            """)
        return self._session.execute(prepared, (timestamp, topic, count))

    def query_recommend_for_client(self, client_id, topic, count):
        self._session.set_keyspace(self._keyspace)

        # TODO
        return []
