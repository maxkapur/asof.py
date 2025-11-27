import argparse
import datetime
import json
import sqlite3
from operator import itemgetter
from pathlib import Path
from typing import NamedTuple
from zoneinfo import ZoneInfo

import requests
import rich.progress
from packaging.tags import sys_tags
from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    Version,
    parse_sdist_filename,
    parse_wheel_filename,
)
from rich.console import Console

name_mapping_url = "https://github.com/regro/cf-graph-countyfair/raw/refs/heads/master/mappings/pypi/name_mapping.json"

pypi_baseurl = "https://pypi.org"
conda_baseurl = "https://api.anaconda.org"
conda_channels = "defaults conda-forge".split()

con = sqlite3.connect(str(Path(__file__).parent / "cache.db"))

cache_lifetime_seconds = 3600 * 24


def main():
    console = Console()
    options = get_options()

    canonical_names = CanonicalNames.from_options(options)
    console.print(canonical_names.pretty, highlight=False)

    initialize_db()

    if not is_cache_fresh():
        json_data = download_name_mapping_json()
        populate_name_mapping_table(json_data)

    m = get_pypi(options.when, canonical_names.pypi_name)
    console.print(m.pretty, highlight=False)


def datetime_fromisoformat_here(s: str) -> datetime.datetime:
    """Parse datetime from ISO format; add current timezone if not present."""
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("localtime"))
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


def initialize_db():
    with con:
        con.execute(
            "CREATE TABLE IF NOT EXISTS name_mapping_raw(downloaded_at TEXT, json TEXT) STRICT"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS name_mapping(conda_name TEXT, import_name TEXT, pypi_name TEXT) STRICT"
        )
        for col in "conda_name import_name pypi_name".split():
            con.execute(
                f"CREATE INDEX IF NOT EXISTS {col}_index ON name_mapping({col})"
            )


def is_cache_fresh() -> bool:
    fetched = con.execute(
        "SELECT downloaded_at FROM name_mapping_raw ORDER BY downloaded_at DESC LIMIT 1"
    ).fetchone()
    if not fetched:
        print("No cache of package name map file available")
        return False

    downloaded_at = datetime.datetime.fromisoformat(fetched[0])
    delta = datetime.datetime.now() - downloaded_at
    if delta.seconds >= cache_lifetime_seconds:
        print("Package name map file in cache, but outdated")
        return False

    print("Package name map file available in cache")
    return True


def download_name_mapping_json() -> str:
    """Download the name mapping data as JSON and update the cache."""

    print(f"Downloading {name_mapping_url} ...", end="")
    resp = requests.get(name_mapping_url, headers={"Accept": "application/json"})
    resp.raise_for_status()
    json_data = resp.content.decode()
    print(" ok")

    with con:
        con.execute("DELETE FROM name_mapping_raw")
        con.execute(
            "INSERT INTO name_mapping_raw VALUES (?, ?)",
            [datetime.datetime.now().isoformat(), json_data],
        )
    return json_data


def populate_name_mapping_table(json_data: str):
    print("Populating database with data from name mapping")
    values = [
        (r["conda_name"], r["import_name"], r["pypi_name"])
        for r in json.loads(json_data)
    ]
    with con:
        con.execute("DELETE FROM name_mapping")
        con.executemany("INSERT INTO name_mapping VALUES (?, ?, ?)", values)


class CanonicalNames(NamedTuple):
    conda_name: str | None
    pypi_name: str | None

    @property
    def pretty(self) -> str:
        return f"Conda name: [bold]{self.conda_name}[/bold] · PyPI name: [bold]{self.pypi_name}[/bold]"

    @classmethod
    def from_conda_name(cls, s: str) -> "CanonicalNames":
        fetched = con.execute(
            "SELECT pypi_name FROM name_mapping WHERE conda_name LIKE ?", [s]
        ).fetchone()
        if fetched:
            return cls(s, fetched[0])
        else:
            # For the most part, the mapping only tries to record packages where
            # the names are different, so nothing in the database suggests they
            # are the same (but we have to search to find out)
            return cls(s, s)

    @classmethod
    def from_import_name(cls, s: str) -> "CanonicalNames":
        fetched = con.execute(
            "SELECT conda_name, pypi_name FROM name_mapping WHERE import_name LIKE ?",
            [s],
        ).fetchone()
        if fetched:
            return cls(fetched[0], fetched[1])
        else:
            return cls(s, s)

    @classmethod
    def from_pypi_name(cls, s: str) -> "CanonicalNames":
        fetched = con.execute(
            "SELECT conda_name FROM name_mapping WHERE pypi_name LIKE ?", [s]
        ).fetchone()
        if fetched:
            return cls(fetched[0], s)
        else:
            return cls(s, s)

    @classmethod
    def from_options(cls, options: argparse.Namespace) -> "CanonicalNames":
        return getattr(cls, f"from_{options.query_type}_name")(options.query)


class PackageMatch(NamedTuple):
    package_name: str
    version: Version
    datetime: datetime.datetime
    source: str

    @property
    def pretty(self) -> str:
        localized_date = self.datetime.strftime("%a %x %X")
        return f"[bold]{self.package_name}[/bold] [bold green]v{self.version!s}[/bold green] published [bold]{localized_date}[/bold] to [bold yellow]{self.source}[/bold yellow]"


def get_pypi(when: datetime.datetime, package: str) -> PackageMatch | None:
    resp = requests.get(
        f"{pypi_baseurl}/simple/{package}/",
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    resp.raise_for_status()
    json_data = resp.content.decode()

    file_objs = json.loads(json_data)["files"]

    # Filter by date, return max by version number. Process in reverse order
    # since the API tends to put the newest versions last, and thus this will
    # reduce the number of reassignments made inside of max()
    file_objs_filtered = []
    for file_obj in rich.progress.track(reversed(file_objs), "PyPI versions"):
        if file_obj["yanked"]:
            continue

        dt = datetime.datetime.fromisoformat(file_obj["upload-time"])
        if dt > when:
            continue

        version = is_compatible(file_obj)
        if version is None:
            continue
        if version.is_prerelease:
            continue

        file_obj["upload-time-dt"] = dt
        file_obj["version"] = version
        file_objs_filtered.append(file_obj)

    if best := max(file_objs_filtered, key=itemgetter("version"), default=None):
        return PackageMatch(
            package, best["version"], best["upload-time-dt"], pypi_baseurl
        )
    return None


def get_conda(when: datetime.datetime, package: str) -> PackageMatch | None:
    for channel in conda_channels:
        resp = requests.get(
            f"{conda_baseurl}/channels/{channel}/{package}/",
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        json_data = resp.content.decode()

        return json_data


def is_compatible(file_obj: dict) -> Version | None:
    """Inspect the PyPI filename and determine compatibility with my system.

    Return the version if so.
    """
    filename = file_obj["filename"]
    try:
        # sdist filename doesn't contain any compat info, so just assume so
        _, version = parse_sdist_filename(filename)
        return version
    except InvalidSdistFilename:
        pass

    try:
        _, version, _, tags = parse_wheel_filename(filename)
        for t in sys_tags():
            if t in tags:
                return version
        return None
    except InvalidWheelFilename:
        pass

    # Could be an ancient .exe or other obsolete packaging format
    return None


if __name__ == "__main__":
    main()
