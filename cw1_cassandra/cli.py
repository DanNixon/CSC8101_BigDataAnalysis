import click
import cassandra
from user_event_db import EventDatabase


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
    cli_data.db = EventDatabase(host, keyspace, replication)


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
        click.echo(e.message)


@cli.command()
@pass_cli_data
def drop(cli_data):
    """
    Drops the tables and namespace from the database.
    """
    try:
        cli_data.db.drop_keyspace()
    except cassandra.protocol.RequestValidationException, e:
        click.echo(e.message)


@cli.command()
@pass_cli_data
def insert(cli_data):
    """
    Inserts event enteries into the database.
    """
    # TODO
    click.echo("insert")
    pass


@cli.group()
@pass_cli_data
def query(cli_data):
    """
    Queries the database.
    """
    # TODO
    click.echo("query")
    pass


@query.command("aaa")
@pass_cli_data
def query_aaa(cli_data):
    """
    TODO
    """
    # TODO
    click.echo("aaa")
    pass


@query.command("bbb")
@pass_cli_data
def query_bbb(cli_data):
    """
    TODO
    """
    # TODO
    click.echo("bbb")
    pass


@query.command("ccc")
@pass_cli_data
def query_ccc(cli_data):
    """
    TODO
    """
    # TODO
    click.echo("ccc")
    pass


if __name__ == '__main__':
    cli()
