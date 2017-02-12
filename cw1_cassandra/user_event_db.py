from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from voluptuous import Schema, Required, Any


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
        # TODO
        pass

    def record_event(self, event):
        # Validation
        self.event_schema(event)

        # TODO
        pass

    def query_client_page_visits(self, client_id, topic, timestamp):
        # TODO
        return []

    def query_top_pages_in_topic(self, topic, timestamp, count):
        # TODO
        return []

    def query_recommend_for_client(self, client_id, topic, count):
        # TODO
        return []
