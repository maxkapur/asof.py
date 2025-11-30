import datetime
import json
import sqlite3
from functools import cache

import requests
from rich.console import Console

import asof

session = requests.Session()


@cache
def get_con(console: Console) -> sqlite3.Connection:
    """Get a connection to the SQLite database.

    Ensure proper initialization.
    """
    con = sqlite3.connect(str(asof.cache_path))

    initialize_tables(con)
    freshly_downloaded = update_downloads(con, console)
    if "name_mapping" in freshly_downloaded:
        asof.db.populate_name_mapping_table(con, console)

    return con


def initialize_tables(con: sqlite3.Connection):
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


def update_downloads(con: sqlite3.Connection, console: Console) -> list[str]:
    """Update the downloads table. Return a list of any stale entries."""
    cutoff = datetime.datetime.now() - asof.cache_lifetime
    res = []
    for name, request in asof.downloads.items():
        if (
            con.execute(
                "SELECT downloaded_at FROM download WHERE url = ? AND downloaded_at >= ? ORDER BY downloaded_at DESC LIMIT 1",
                [request.url, cutoff.isoformat()],
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


def populate_name_mapping_table(con: sqlite3.Connection, console: Console):
    fetched = con.execute(
        "SELECT content FROM download WHERE url = ? ORDER BY downloaded_at DESC LIMIT 1",
        [asof.downloads["name_mapping"].url],
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
