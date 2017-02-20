#!/usr/bin/env python

import click
import cassandra
import voluptuous
import json
from user_event_db import EventDatabase, json_to_event, json_to_timestamp_visists


class CLIData(object):
    """
    Helper class to store data to be passed between CLI commands.
    """

    def __init__(self):
        self.db = None

pass_cli_data = click.make_pass_decorator(CLIData, ensure=True)


@click.group()
@click.option('-h', '--host', default='127.0.0.1', help='Hostname of Cassandra cluster to connect to')
@click.option('-k', '--keyspace', default='csc8101', help='Database keyspace')
@click.option('-r', '--replication', default=1, help='Replication factor of keyspace (only relevant when creating keyspace)')
@pass_cli_data
def cli(cli_data, host, keyspace, replication):
    """
    CLI for manipulating event database for coursework 1.
    """
    try:
        cli_data.db = EventDatabase(host, keyspace, replication)
    except:
        raise click.ClickException("Database connection failed.")


@cli.command()
@pass_cli_data
def init(cli_data):
    """
    Creates the namespace and tables.
    """
    cli_data.db.create_keyspace()
    cli_data.db.create_tables()


@cli.command()
@pass_cli_data
def drop(cli_data):
    """
    Drops the tables and namespace from the database.
    """
    cli_data.db.drop_keyspace()


@cli.group()
def insert():
    """
    Inserts into the database.
    """
    pass


@insert.group("views")
def insert_views():
    """
    Inserts event enteries into the database.
    """
    pass


@insert_views.command("single")
@click.option('-c', '--clientid', type=str, help='ID of client generting the visit event')
@click.option('-s', '--timestamp', type=int, help='Batch timestamp of visit event')
@click.option('-t', '--topic', type=str, help='Topic assigned to visited page')
@click.option('-p', '--page', type=str, help='Name of visited page')
@pass_cli_data
def insert_views_single(cli_data, clientid, timestamp, topic, page):
    """
    Inserts a single event into the database.
    """
    cli_data.db.record_visit(clientid, timestamp, topic, page)


@insert_views.command("json")
@click.argument("data_file", type=click.File('rb'))
@pass_cli_data
def insert_views_json(cli_data, data_file):
    """
    Inserts multiple events from a JSON file into the database.
    """
    data = json.load(data_file)
    events = map(json_to_event, data)
    for e in events:
        cli_data.db.record_visit(*e)


@insert.group("timestamp_summary")
def insert_timestamp_summary():
    """
    Inserts into the top pages for timestamp table.
    """
    pass


@insert_timestamp_summary.command("single")
@click.option('-s', '--timestamp', type=int, help='Batch timestamp of visit event')
@click.option('-t', '--topic', type=str, help='Topic assigned to visited page')
@click.option('-p', '--page', type=str, help='Name of visited page')
@click.option('-c', '--count', type=int, help='Count of unique page visits')
@pass_cli_data
def insert_timestamp_summary_single(cli_data, timestamp, topic, page, count):
    """
    Inserts a single top page entry.
    """
    cli_data.db.record_visits_in_timestamp(timestamp, topic, page, count)


@insert_timestamp_summary.command("json")
@click.argument("data_file", type=click.File('rb'))
@pass_cli_data
def insert_timestamp_summary_json(cli_data, data_file):
    """
    Inserts multiple top page enteries from a JSON file into the database.
    """
    data = json.load(data_file)
    events = map(json_to_timestamp_visists, data)
    for e in events:
        cli_data.db.record_visits_in_timestamp(*e)


@cli.group()
def query():
    """
    Queries the database.
    """
    pass


@query.command("client_visits")
@click.option('-c', '--clientid', type=str, help='ID of client')
@click.option('-s', '--timestamp', type=int, help='Start of batch timestamp')
@click.option('-t', '--topic', type=str, help='Topic assigned to page')
@pass_cli_data
def query_client_visits(cli_data, clientid, timestamp, topic):
    """
    Gets a list of pages related to a certain topic a client visits in a given batch timestamp.
    """
    results = cli_data.db.query_client_page_visits(
        clientid, timestamp, topic)

    for r in results:
        print "({: 4d})  {}".format(r.visits, r.page)


@query.command("top_pages")
@click.option('-s', '--timestamp', type=int, help='Start of batch timestamp')
@click.option('-t', '--topic', type=str, help='Topic assigned to page')
@click.option('-n', '--count', type=int, default=10, help='Number of results to retrieve')
@pass_cli_data
def query_top_pages(cli_data, timestamp, topic, count):
    """
    Gets the top N pages visited in a given batch timestamp ranked by visit count.
    """
    results = cli_data.db.query_top_pages_in_topic(timestamp, topic, count)

    for r in results:
        print "({: 4d})  {}".format(r.visits, r.page)


@query.command("recommendations")
@click.option('-c', '--clientid', type=str, help='ID of client')
@click.option('-t', '--topic', type=str, help='Topic assigned to page')
@click.option('-n', '--count', type=int, default=10, help='Number of results to retrieve')
@pass_cli_data
def query_recommendations(cli_data, clientid, topic, count):
    """
    Gets recommended pages related to a certain topic for a given client.
    """
    results = cli_data.db. query_recommend_for_client(
        clientid, topic, count)

    for r in results:
        print r.page


if __name__ == '__main__':
    cli()
