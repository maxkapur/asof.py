import argparse
from typing import NamedTuple

import asof.db


class CanonicalNames(NamedTuple):
    conda_name: str | None
    pypi_name: str | None

    @property
    def pretty(self) -> str:
        return f"Conda name: [bold]{self.conda_name}[/bold] · PyPI name: [bold]{self.pypi_name}[/bold]"

    @classmethod
    def from_conda_name(cls, s: str) -> "CanonicalNames":
        fetched = asof.db.con.execute(
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
        fetched = asof.db.con.execute(
            "SELECT conda_name, pypi_name FROM name_mapping WHERE import_name LIKE ?",
            [s],
        ).fetchone()
        if fetched:
            return cls(fetched[0], fetched[1])
        else:
            return cls(s, s)

    @classmethod
    def from_pypi_name(cls, s: str) -> "CanonicalNames":
        fetched = asof.db.con.execute(
            "SELECT conda_name FROM name_mapping WHERE pypi_name LIKE ?", [s]
        ).fetchone()
        if fetched:
            return cls(fetched[0], s)
        else:
            return cls(s, s)

    @classmethod
    def from_options(cls, options: argparse.Namespace) -> "CanonicalNames":
        return getattr(cls, f"from_{options.query_type.lower()}_name")(options.query)
