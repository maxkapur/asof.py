import argparse
from typing import NamedTuple

from rich.console import Console

from asof.db import get_con


class CanonicalNames(NamedTuple):
    conda_name: str | None
    pypi_name: str | None

    @property
    def pretty(self) -> str:
        return f"Conda name: [bold]{self.conda_name}[/bold] · PyPI name: [bold]{self.pypi_name}[/bold]"

    @classmethod
    def from_conda_name(cls, name: str, console: Console) -> "CanonicalNames":
        fetched = (
            get_con(console)
            .execute(
                "SELECT pypi_name FROM name_mapping WHERE conda_name LIKE ?", [name]
            )
            .fetchone()
        )
        if fetched:
            return cls(name, fetched[0])
        else:
            # For the most part, the mapping only tries to record packages where
            # the names are different, so nothing in the database suggests they
            # are the same (but we have to search to find out)
            return cls(name, name)

    @classmethod
    def from_import_name(cls, name: str, console: Console) -> "CanonicalNames":
        fetched = (
            get_con(console)
            .execute(
                "SELECT conda_name, pypi_name FROM name_mapping WHERE import_name LIKE ?",
                [name],
            )
            .fetchone()
        )
        if fetched:
            return cls(fetched[0], fetched[1])
        else:
            return cls(name, name)

    @classmethod
    def from_pypi_name(cls, name: str, console: Console) -> "CanonicalNames":
        fetched = (
            get_con(console)
            .execute(
                "SELECT conda_name FROM name_mapping WHERE pypi_name LIKE ?", [name]
            )
            .fetchone()
        )
        if fetched:
            return cls(fetched[0], name)
        else:
            return cls(name, name)

    @classmethod
    def from_options(
        cls, options: argparse.Namespace, console: Console
    ) -> "CanonicalNames":
        return getattr(cls, f"from_{options.query_type.lower()}_name")(
            options.query, console
        )
