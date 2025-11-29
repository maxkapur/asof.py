import argparse
import datetime
import json
import re
import sqlite3
import subprocess
import warnings
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import requests
from packaging.tags import sys_tags
from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import VERSION_PATTERN, Version
from rich.console import Console

VERSION_PATTERN = re.compile(VERSION_PATTERN, re.VERBOSE | re.IGNORECASE)

session = requests.Session()


pypi_baseurl = "https://pypi.org"
conda_baseurl = "https://api.anaconda.org"
conda_channels = "defaults conda-forge".split()

con = sqlite3.connect(str(Path(__file__).parent / "cache.db"))


downloads = {
    "name_mapping": requests.Request(
        "GET",
        "https://github.com/regro/cf-graph-countyfair/raw/refs/heads/master/mappings/pypi/name_mapping.json",
        headers={"Accept": "application/json"},
    ).prepare(),
}
cache_lifetime = datetime.timedelta(days=1)

console = Console()


def main():
    options = get_options()

    initialize_db()
    freshly_downloaded = update_downloads()
    if "name_mapping" in freshly_downloaded:
        populate_name_mapping_table()

    canonical_names = CanonicalNames.from_options(options)
    console.print(f"Query: [bold]{options.query}[/bold]", highlight=False)
    console.print(canonical_names.pretty, highlight=False)

    if matches := get_pypi(options.when, canonical_names.pypi_name):
        for m in matches:
            console.print(m.pretty, highlight=False)
    else:
        console.print("[gray]No matches from PyPI[/gray]")

    if conda_command := get_conda_command():
        get_conda(conda_command, options.when, canonical_names.conda_name)


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


def initialize_db():
    with con:
        con.execute(
            "CREATE TABLE IF NOT EXISTS download(url TEXT, downloaded_at TEXT, content TEXT) STRICT"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS name_mapping(conda_name TEXT, import_name TEXT, pypi_name TEXT) STRICT"
        )
        for col in "conda_name import_name pypi_name".split():
            con.execute(
                f"CREATE INDEX IF NOT EXISTS {col}_index ON name_mapping({col})"
            )


def update_downloads() -> list[str]:
    """Update the downloads table. Return a list of any stale entries."""
    cutoff = datetime.datetime.now() - cache_lifetime
    res = []
    for name, request in downloads.items():
        if (
            con.execute(
                "SELECT downloaded_at FROM download WHERE url = ? AND downloaded_at >= ? ORDER BY downloaded_at DESC LIMIT 1",
                [request.url, cutoff],
            ).fetchone()
            is not None
        ):
            continue

        console.print(f"Downloading {request.url}", end="", highlight=True)
        resp = session.send(request)
        resp.raise_for_status()
        text_received = resp.content.decode()
        console.print(": [green]OK[/green]", highlight=False)

        with con:
            con.execute("DELETE FROM download WHERE url = ?", [request.url])
            con.execute(
                "INSERT INTO download VALUES (?, ?, ?)",
                [request.url, datetime.datetime.now().isoformat(), text_received],
            )
        res.append(name)
    return res


def populate_name_mapping_table():
    fetched = con.execute(
        "SELECT content FROM download WHERE url = ? ORDER BY downloaded_at DESC LIMIT 1",
        [downloads["name_mapping"].url],
    ).fetchone()
    if fetched is None:
        raise ValueError("Missing download")

    console.print("Updating name mapping database", highlight=False)
    values = [
        (r["conda_name"], r["import_name"], r["pypi_name"])
        for r in json.loads(fetched[0])
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


def get_pypi(when: datetime.datetime, package: str) -> list[PackageMatch]:
    resp = requests.get(
        f"{pypi_baseurl}/simple/{package}/",
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    json_data = resp.content.decode()

    file_objs = json.loads(json_data)["files"]

    # To avoid having to parse every entry in full, start by grouping by version
    # string (from the filename)
    grouped = defaultdict(list)
    for file_obj in file_objs:
        if m := VERSION_PATTERN.search(file_obj["filename"]):
            version_str = m.group(0)
            grouped[version_str].append(file_obj)
        else:
            warnings.warn(f"Unable to parse version name {file_obj['filename']}")

    # Now parse only these keys to Version objects and sort from highest to
    # lowest. The API JSON tends to put newer versions toward the end, so the
    # keys are probably *already* almost sorted, so it will be fastest to sort
    # ascending and then reverse:
    version_strs = sorted(grouped.keys(), key=Version)
    version_strs.reverse()

    # Walk backwards through versions and return newest release version and
    # newest prerelease (if available)
    matches = []
    for version_str in version_strs:
        for file_obj in grouped[version_str]:
            if file_obj["yanked"]:
                continue

            dt = datetime.datetime.fromisoformat(file_obj["upload-time"])
            if dt > when:
                continue

            version_obj = is_compatible(file_obj)
            if version_obj is None:
                continue
            if version_obj.is_prerelease and matches:
                # If we already have matches, then we already have a prerelease
                # higher than this one
                continue

            m = PackageMatch(package, version_obj, dt, pypi_baseurl)
            matches.append(m)

            if not version_obj.is_prerelease:
                # Highest non-prerelease match found == done
                return matches

    return matches


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


def get_conda_command() -> str | None:
    for command in "mamba conda".split():
        try:
            subprocess.run([command], capture_output=True)
            return command
        except FileNotFoundError:
            pass
    return None

def get_conda(
    conda_command: str, when: datetime.datetime, package: str
) -> PackageMatch | None:
    cmd = [conda_command, "search", "--json", package]
    for channel in conda_channels:
        cmd.extend(["--channel", channel])

    res = subprocess.run(cmd, capture_output=True)
    if res.statuscode != 0:
        raise RuntimeError(res)
    return res.stdout


if __name__ == "__main__":
    main()
