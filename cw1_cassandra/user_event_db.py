from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from voluptuous import Schema, Required, Any
import datetime


def json_to_event(json_data):
    time = datetime.datetime.strptime(
        json_data["timestamp"], '%Y-%m-%dT%H:%M:%S')
    ts = int((time - datetime.datetime(1970, 1, 1)).total_seconds())
    return {"client_id": json_data["client_id"], "timestamp": ts, "topic": json_data["url"]["topic"], "page": json_data["url"]["page"]}


class EventDatabase(object):

    event_schema = Schema({
        Required('client_id'): Any(str, unicode),
        Required('timestamp'): int,
        Required('topic'): Any(str, unicode),
        Required('page'): Any(str, unicode)
    })

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
            timestamp int,
            topic text,
            page text,
            PRIMARY KEY (clientid, timestamp, topic, page)
            )
            """)

        # Table with timestamp and topic as partition key, ensures single node
        # reads and moderately distributed writes.
        self._session.execute("""
            CREATE TABLE IF NOT EXISTS top_pages (
            clientid text,
            timestamp int,
            topic text,
            page text,
            visit_count counter,
            PRIMARY KEY ((timestamp, topic), page, clientid)
            )
            """)

        # TODO
        # self._session.execute("""
        #     CREATE TABLE IF NOT EXISTS top_pages (
        #     PRIMARY KEY ()
        #     )
        #     """)

    def record_event(self, event):
        # Validation
        self.event_schema(event)

        self._session.set_keyspace(self._keyspace)

        # Client page visits table
        prepared = self._session.prepare("""
            INSERT INTO client_pages_visited (clientid, timestamp, topic, page)
            VALUES (?, ?, ?, ?)
            """)
        self._session.execute(prepared, (event["client_id"], event[
                              "timestamp"], event["topic"], event["page"]))

        # Top pages in topic table
        prepared = self._session.prepare("""
            UPDATE top_pages SET visit_count = visit_count + 1
            WHERE clientid = ? AND timestamp = ? AND topic = ? and page = ?;
            """)
        self._session.execute(prepared, (event["client_id"], event["timestamp"], event[
                              "topic"], event["page"]))

        # Recommendations table
        # TODO

    def query_client_page_visits(self, client_id, timestamp, topic):
        self._session.set_keyspace(self._keyspace)

        prepared = self._session.prepare("""
            SELECT page FROM client_pages_visited WHERE clientid = ? AND timestamp = ? AND topic = ?
            """)
        return self._session.execute(prepared, (client_id, timestamp, topic))

    def query_top_pages_in_topic(self, timestamp, topic, count):
        self._session.set_keyspace(self._keyspace)

        prepared = self._session.prepare("""
            select page, count(*) from top_pages where timestamp = ? and topic = ? group by page LIMIT ?
            """)
        return self._session.execute(prepared, (timestamp, topic, count))

    def query_recommend_for_client(self, client_id, topic, count):
        self._session.set_keyspace(self._keyspace)

        # TODO
        return []
