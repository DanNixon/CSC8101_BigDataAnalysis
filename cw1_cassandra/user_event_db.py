from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


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
        pass
