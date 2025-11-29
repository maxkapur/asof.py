import argparse
import datetime

from rich.console import Console

import asof
import asof.conda
import asof.db
from asof.canonical_names import CanonicalNames
from asof.conda import get_conda as get_conda
from asof.pypi import get_pypi as get_pypi


def main():
    console = Console()
    options = get_options()

    asof.db.initialize_db()
    freshly_downloaded = asof.db.update_downloads(console)
    if "name_mapping" in freshly_downloaded:
        asof.db.populate_name_mapping_table(console)

    canonical_names = CanonicalNames.from_options(options)
    console.print(f"Query: [bold]{options.query}[/bold]", highlight=False)
    console.print(canonical_names.pretty, highlight=False)

    get_pypi(options.when, canonical_names.pypi_name).log(console)

    if conda_command := asof.conda.get_conda_command():
        get_conda(conda_command, options.when, canonical_names.pypi_name).log(console)


def datetime_fromisoformat_here(s: str) -> datetime.datetime:
    """Parse datetime from ISO format; add current timezone if not present."""
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        tzinfo = datetime.datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=tzinfo)
    return dt


def get_parser():
    parser = argparse.ArgumentParser(prog="asof.py")
    parser.add_argument(
        "when",
        help="Date or time of cutoff (ISO format). Only versions released before this time are considered.",
        type=datetime_fromisoformat_here,
    )
    parser.add_argument(
        "query",
        help='Package name (or import name, if query type is "import") to search for latest version.',
    )

    parser.add_argument(
        "--query-type",
        help='Type of query (default: "pypi"). For example, "pypi" matches packages based on the name registered in PyPI. Many, but not all, packages have identical names for the imported module, PyPI package, and conda package.',
        nargs="?",
        choices=["conda", "import", "pypi"],
        default="pypi",
    )
    return parser


def get_options():
    return get_parser().parse_args()


if __name__ == "__main__":
    main()
