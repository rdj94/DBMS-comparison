import pg
import mysql_ex
import mongo
import click
from rich import print
import experiment


@click.group()
def cli():
    pass


@cli.command()
def prepare():
    """
    Prepare the experiment log, which is stored as an SQLite database.
    """
    table = "result"
    exists = experiment.table_exists(table)
    if exists:
        row_count = experiment.get_row_count(table)
        if row_count > 0:
            print(
                f"[bold red]WARNING:[/bold red] table '{table}' contains {row_count} rows"
            )
        drop = click.confirm(f"Do you want to drop and recreate the table '{table}'?")
        if drop:
            experiment.initialize()
    else:
        experiment.initialize()


@cli.command()
@click.argument("dbms", type=str)
def init(dbms: str):
    """
    Run script to initialize the experiment schema for the specified DBMS.
    """
    print(f"Initializing experiment table for {dbms=}")
    if dbms == "pg":
        pg.reset_database()
    elif dbms == "mysql":
        mysql_ex.reset_database()
    elif dbms == "mongo":
        mongo.reset_database()
    else:
        raise ValueError(f"Invalid DBMS specified: '{dbms}'")


@cli.command()
@click.option("-i", default=1, help="Number of runs of each experimental setup")
@click.option(
    "-n", default=100000, help="Number of records to insert for each experimental run"
)
@click.option(
    "-p",
    default=1000000,
    help="Number of records to preload before each experimental run",
)
def run(i: int, n: int, p: int):
    for j in range(i):
        experiment.run_experiments(
            experiment_size=n,
            preload_size=p,
            max_iterations=i,
            current_iteration=j,
        )
        break

if __name__ == "__main__":
    cli()
