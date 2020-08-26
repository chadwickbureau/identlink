import click

from . import people
from . import coverage
from . import lint
from . import tidy


@click.group("ident")
def ident():
    """Manage identifications in statistical data."""
    pass


@ident.command("update")
def do_update():
    people.main()


@ident.command("coverage")
@click.argument('year', type=int)
def do_coverage(year):
    coverage.main(year)


@ident.command("lint")
@click.argument('year', type=int)
def do_lint(year):
    lint.main(year)


@ident.command("tidy")
def do_tidy():
    tidy.main()
