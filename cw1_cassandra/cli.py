import click
import cassandra
import voluptuous
import json
from user_event_db import EventDatabase, json_to_event


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
    try:
        cli_data.db.create_keyspace()
        cli_data.db.create_tables()
    except cassandra.protocol.AlreadyExists, e:
        raise click.ClickException(e.message)


@cli.command()
@pass_cli_data
def drop(cli_data):
    """
    Drops the tables and namespace from the database.
    """
    try:
        cli_data.db.drop_keyspace()
    except cassandra.protocol.RequestValidationException, e:
        raise click.ClickException(e.message)


@cli.group()
@pass_cli_data
def insert(cli_data):
    """
    Inserts event enteries into the database.
    """
    pass


@insert.command("single")
@click.option('-c', '--clientid', type=str, help='ID of client generting the visit event')
@click.option('-s', '--timestamp', type=int, help='Batch timestamp of visit event')
@click.option('-t', '--topic', type=str, help='Topic assigned to visited page')
@click.option('-p', '--page', type=str, help='Name of visited page')
@pass_cli_data
def insert_single(cli_data, clientid, timestamp, topic, page):
    """
    Inserts a single event into the database.
    """
    try:
        cli_data.db.record_event(
                {"client_id": clientid, "timestamp": timestamp, "topic": topic, "page": page})
    except voluptuous.error.Invalid, e:
        raise click.ClickException(
                "Input data format error ({})".format(e.msg))


@insert.command("json")
@click.argument("data_file", type=click.File('rb'))
@pass_cli_data
def insert_json(cli_data, data_file):
    """
    Inserts multiple events from a JSON file into the database.
    """
    try:
        data = json.load(data_file)
        events = map(json_to_event, data)
        for e in events:
            cli_data.db.record_event(e)
    except RuntimeError, e:
        raise click.ClickException(e.message)


@cli.group()
@pass_cli_data
def query(cli_data):
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
    try:
        results = cli_data.db.query_client_page_visits(
                clientid, timestamp, topic)

        for r in results:
            print r.page
    except:
        # TODO
        raise click. ClickException("nope")


@query.command("top_pages")
@click.option('-s', '--timestamp', type=int, help='Start of batch timestamp')
@click.option('-t', '--topic', type=str, help='Topic assigned to page')
@click.option('-n', '--count', type=int, default=10, help='Number of results to retrieve')
@pass_cli_data
def query_top_pages(cli_data, timestamp, topic, count):
    """
    Gets the top N pages visited in a given batch timestamp ranked by visit count.
    """
    try:
        results = cli_data.db.query_top_pages_in_topic(timestamp, topic, count)
        # TODO
        print results
    except:
        # TODO
        raise click. ClickException("nope")


@query.command("recommendations")
@click.option('-c', '--clientid', type=str, help='ID of client')
@click.option('-t', '--topic', type=str, help='Topic assigned to page')
@click.option('-n', '--count', type=int, default=10, help='Number of results to retrieve')
@pass_cli_data
def query_recommendations(cli_data, clientid, topic, count):
    """
    Gets recommended pages related to a certain topic for a given client.
    """
    try:
        results = cli_data.db. query_recommend_for_client(
                clientid, topic, count)
        # TODO
        print results
    except:
        # TODO
        raise click. ClickException("nope")


if __name__ == '__main__':
    cli()
