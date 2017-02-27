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

    def test_record_visit(self):
        self.db.record_visit('A', 1, 'T1', 'T1.P1')
        self.db.record_visit('A', 1, 'T1', 'T1.P2')
        self.db.record_visit('A', 1, 'T1', 'T1.P2')
        self.db.record_visit('A', 1, 'T1', 'T1.P3')
        self.db.record_visit('A', 1, 'T2', 'T2.P2')
        self.db.record_visit('B', 1, 'T2', 'T2.P1')
        self.db.record_visit('B', 1, 'T2', 'T2.P2')
        self.db.record_visit('A', 1, 'T1', 'T1.P2')
        self.db.record_visit('A', 1, 'T1', 'T1.P2')

        client_a_visits_topic_1 = self.db.query_client_page_visits('A', 1, 'T1').current_rows
        self.assertEqual(len(client_a_visits_topic_1), 3)

        client_b_visits_topic_1 = self.db.query_client_page_visits('B', 1, 'T1').current_rows
        self.assertEqual(len(client_b_visits_topic_1), 0)

    def test_timestamp_summary(self):
        self.db.record_visits_in_timestamp(1, 'T1', 'T1.P1', 7)
        self.db.record_visits_in_timestamp(1, 'T1', 'T1.P2', 2)
        self.db.record_visits_in_timestamp(1, 'T1', 'T1.P3', 4)
        self.db.record_visits_in_timestamp(1, 'T2', 'T2.P2', 3)
        self.db.record_visits_in_timestamp(2, 'T1', 'T1.P4', 2)
        self.db.record_visits_in_timestamp(2, 'T2', 'T2.P1', 1)
        self.db.record_visits_in_timestamp(2, 'T2', 'T2.P2', 3)
        self.db.record_visits_in_timestamp(3, 'T1', 'T1.P2', 7)
        self.db.record_visits_in_timestamp(3, 'T2', 'T2.P1', 4)
        self.db.record_visits_in_timestamp(3, 'T2', 'T2.P2', 2)
        self.db.record_visits_in_timestamp(3, 'T3', 'T3.P1', 2)
        self.db.record_visits_in_timestamp(3, 'T3', 'T3.P2', 1)
        self.db.record_visits_in_timestamp(4, 'T1', 'T1.P1', 1)
        self.db.record_visits_in_timestamp(4, 'T1', 'T1.P2', 2)
        self.db.record_visits_in_timestamp(4, 'T2', 'T2.P1', 4)
        self.db.record_visits_in_timestamp(4, 'T2', 'T2.P2', 2)
        self.db.record_visits_in_timestamp(4, 'T3', 'T3.P1', 3)

        top_pages_timestamp_1_topic_1 = self.db.query_top_pages_in_topic(1, 'T1', 10).current_rows
        self.assertEquals(len(top_pages_timestamp_1_topic_1), 3)
        self.assertEquals(top_pages_timestamp_1_topic_1[0].page, 'T1.P1')
        self.assertEquals(top_pages_timestamp_1_topic_1[1].page, 'T1.P3')
        self.assertEquals(top_pages_timestamp_1_topic_1[2].page, 'T1.P2')

        top_pages_timestamp_1_topic_1_limit_2 = self.db.query_top_pages_in_topic(1, 'T1', 2).current_rows
        self.assertEquals(len(top_pages_timestamp_1_topic_1_limit_2), 2)

        top_pages_timestamp_2_topic_3 = self.db.query_top_pages_in_topic(2, 'T3', 10).current_rows
        self.assertEquals(len(top_pages_timestamp_2_topic_3), 0)

    def test_recommend_for_client_happy_case(self):
        user = 'A'
        topic = 'T1'

        # Record sample visits (only needed for user that is having pages recommended for them)
        self.db.record_visit(user, 1, topic, 'T1.P1')
        self.db.record_visit(user, 1, topic, 'T1.P3')
        self.db.record_visit(user, 1, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 4, topic, 'T1.P1')
        self.db.record_visit(user, 4, topic, 'T1.P6')
        self.db.record_visit(user, 4, topic, 'T1.P2')

        # Record page visits in timestamp
        self.db.record_visits_in_timestamp(1, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P5', 3)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P6', 5)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P1', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P2', 4)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P4', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P5', 8)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P6', 1)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P1', 9)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P5', 6)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P6', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P2', 7)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P4', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P5', 1)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P6', 1)

        # Test recommendations
        pages = self.db.query_recommend_for_client(user, topic, 3)
        self.assertEqual(len(pages), 2)
        self.assertTrue('T1.P3' in pages)
        self.assertTrue('T1.P4' in pages)

        # Test recommendations (all pages)
        pages = self.db.query_recommend_for_client(user, topic, 10)
        self.assertEqual(len(pages), 3)
        self.assertTrue('T1.P3' in pages)
        self.assertTrue('T1.P4' in pages)
        self.assertTrue('T1.P5' in pages)

    def test_recommend_for_client_when_client_views_no_pages(self):
        user = 'A'
        topic = 'T1'

        # Record page visits in timestamp
        self.db.record_visits_in_timestamp(1, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P5', 3)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P6', 5)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P1', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P2', 4)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P4', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P5', 8)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P6', 1)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P1', 9)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P5', 6)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P6', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P2', 7)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P4', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P5', 1)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P6', 1)

        # Test recommendations
        pages = self.db.query_recommend_for_client(user, topic, 10)
        self.assertEqual(len(pages), 6)
        self.assertTrue('T1.P1' in pages)
        self.assertTrue('T1.P2' in pages)
        self.assertTrue('T1.P3' in pages)
        self.assertTrue('T1.P4' in pages)
        self.assertTrue('T1.P5' in pages)
        self.assertTrue('T1.P6' in pages)

        # Test recommendations (limit to 3)
        pages = self.db.query_recommend_for_client(user, topic, 3)
        self.assertEqual(len(pages), 3)

    def test_recommend_for_client_when_client_views_all_pages(self):
        user = 'A'
        topic = 'T1'

        # Record sample visits (only needed for user that is having pages recommended for them)
        self.db.record_visit(user, 1, topic, 'T1.P1')
        self.db.record_visit(user, 1, topic, 'T1.P3')
        self.db.record_visit(user, 1, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 4, topic, 'T1.P1')
        self.db.record_visit(user, 4, topic, 'T1.P2')
        self.db.record_visit(user, 4, topic, 'T1.P3')
        self.db.record_visit(user, 4, topic, 'T1.P4')
        self.db.record_visit(user, 4, topic, 'T1.P5')
        self.db.record_visit(user, 4, topic, 'T1.P6')

        # Record page visits in timestamp
        self.db.record_visits_in_timestamp(1, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P5', 3)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P6', 5)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P1', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P2', 4)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P4', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P5', 8)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P6', 1)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P1', 9)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P5', 6)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P6', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P2', 7)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P4', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P5', 1)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P6', 1)

        # Test recommendations
        pages = self.db.query_recommend_for_client(user, topic, 10)
        self.assertEqual(len(pages), 0)

    def test_recommend_for_client_when_only_client_has_viewed_pages(self):
        user = 'A'
        topic = 'T1'

        # Record sample visits (only needed for user that is having pages recommended for them)
        self.db.record_visit(user, 1, topic, 'T1.P1')
        self.db.record_visit(user, 1, topic, 'T1.P3')
        self.db.record_visit(user, 1, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 4, topic, 'T1.P2')
        self.db.record_visit(user, 4, topic, 'T1.P4')
        self.db.record_visit(user, 4, topic, 'T1.P6')

        # Record page visits in timestamp
        self.db.record_visits_in_timestamp(1, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P5', 3)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P6', 5)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P1', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P2', 4)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P4', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P5', 8)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P6', 1)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P1', 9)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P5', 6)
        self.db.record_visits_in_timestamp(3, topic, 'T1.P6', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P4', 2)
        self.db.record_visits_in_timestamp(4, topic, 'T1.P6', 1)

        # Test recommendations
        pages = self.db.query_recommend_for_client(user, topic, 10)
        self.assertEqual(len(pages), 0)

    def test_recommend_for_client_at_early_time_in_recording(self):
        user = 'A'
        topic = 'T1'

        # Record sample visits (only needed for user that is having pages recommended for them)
        self.db.record_visit(user, 1, topic, 'T1.P1')
        self.db.record_visit(user, 1, topic, 'T1.P3')
        self.db.record_visit(user, 1, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')
        self.db.record_visit(user, 2, topic, 'T1.P2')

        # Record page visits in timestamp
        self.db.record_visits_in_timestamp(1, topic, 'T1.P1', 1)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P2', 2)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P3', 4)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P4', 5)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P5', 3)
        self.db.record_visits_in_timestamp(1, topic, 'T1.P6', 5)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P1', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P2', 4)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P3', 3)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P4', 7)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P5', 8)
        self.db.record_visits_in_timestamp(2, topic, 'T1.P6', 1)

        # Test recommendations
        pages = self.db.query_recommend_for_client(user, topic, 10)
        self.assertEqual(len(pages), 3)
        self.assertTrue('T1.P4' in pages)
        self.assertTrue('T1.P5' in pages)
        self.assertTrue('T1.P6' in pages)


if __name__ == '__main__':
    unittest.main()
