import datetime
from typing import NamedTuple

from packaging.version import Version
from rich.console import Console


class PackageMatch(NamedTuple):
    """Details on a compatible package and its source."""

    package_name: str
    version: Version
    datetime: datetime.datetime
    source: str

    @property
    def pretty(self) -> str:
        localized_date = self.datetime.strftime("%a %x %X")
        return f"[bold]{self.package_name}[/bold] [bold green]v{self.version!s}[/bold green] published [bold]{localized_date}[/bold] to [bold yellow]{self.source}[/bold yellow]"


class MatchesOption(NamedTuple):
    """List of package matches, or a status message explaining why none."""

    matches: list[PackageMatch]
    message: str | None

    def log(self, console: Console) -> None:
        if self.message:
            console.print(f"[gray]{self.message}[/gray]", highlight=False)
        for m in self.matches:
            console.print(m.pretty, highlight=False)
