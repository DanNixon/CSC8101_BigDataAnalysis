#!/usr/bin/env python

import unittest
from user_event_db import EventDatabase, json_to_event, json_to_timestamp_visists


class UserEventDBTest(unittest.TestCase):

    def setUp(self):
        self.db = EventDatabase('127.0.0.1', 'csc8101', 1)
        self.db.create_keyspace()
        self.db.create_tables()

    def tearDown(self):
        self.db.drop_keyspace()

    def test_this(self):
        self.assertEqual("yes", "yes")

# TODO


if __name__ == '__main__':
    unittest.main()
