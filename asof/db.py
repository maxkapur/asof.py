import datetime
import json
import sqlite3

import requests

import asof
from asof import cache_path

session = requests.Session()

con = sqlite3.connect(str(cache_path))


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
    cutoff = datetime.datetime.now() - asof.cache_lifetime
    res = []
    for name, request in asof.downloads.items():
        if (
            con.execute(
                "SELECT downloaded_at FROM download WHERE url = ? AND downloaded_at >= ? ORDER BY downloaded_at DESC LIMIT 1",
                [request.url, cutoff],
            ).fetchone()
            is not None
        ):
            continue

        asof.console.print(f"Downloading {request.url}", end="", highlight=True)
        resp = session.send(request)
        resp.raise_for_status()
        text_received = resp.content.decode()
        asof.console.print(": [green]OK[/green]", highlight=False)

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
        [asof.downloads["name_mapping"].url],
    ).fetchone()
    if fetched is None:
        raise ValueError("Missing download")

    asof.console.print("Updating name mapping database", highlight=False)
    values = [
        (r["conda_name"], r["import_name"], r["pypi_name"])
        for r in json.loads(fetched[0])
    ]
    with con:
        con.execute("DELETE FROM name_mapping")
        con.executemany("INSERT INTO name_mapping VALUES (?, ?, ?)", values)
