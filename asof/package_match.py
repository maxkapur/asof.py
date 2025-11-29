import datetime
from typing import NamedTuple

from packaging.version import Version


class PackageMatch(NamedTuple):
    package_name: str
    version: Version
    datetime: datetime.datetime
    source: str

    @property
    def pretty(self) -> str:
        localized_date = self.datetime.strftime("%a %x %X")
        return f"[bold]{self.package_name}[/bold] [bold green]v{self.version!s}[/bold green] published [bold]{localized_date}[/bold] to [bold yellow]{self.source}[/bold yellow]"
