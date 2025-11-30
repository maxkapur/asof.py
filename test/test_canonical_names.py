from argparse import Namespace
from typing import Iterator, NamedTuple

import pytest
from rich.console import Console

from asof.canonical_names import CanonicalNames


class Triple(NamedTuple):
    conda_name: str
    import_name: str
    pypi_name: str


console = Console()


@pytest.fixture(
    params=[
        # SELECT * FROM name_mapping WHERE NOT (conda_name LIKE pypi_name OR
        # pypi_name LIKE import_name OR import_name LIKE conda_name);
        # There are about 200 such pairs, mostly aws-cdk stuff; this is just a
        # subset of those that seem recognizable
        "analytics-python|segment|segment-analytics-python",
        "arm_pyart|pyart|arm-pyart",
        "backports.zoneinfo|zoneinfo|backports-zoneinfo",
        "boolean.py|boolean|boolean-py",
        "discord.py|discord|discord-py",
        "dogpile.cache|dogpile|dogpile-cache",
        "fs.webdavfs|webdavfs|fs-webdavfs",
        "github3.py|github3|github3-py",
        "mastodon.py|mastodon|mastodon-py",
        "qt.py|Qt|qt-py",
        "web.py|web|web-py",
        # Ambiguous case, as there are multiple entries with import name "fs"
        # "fs.googledrivefs|fs|fs-googledrivefs",
    ]
)
def triple(request: pytest.FixtureRequest) -> Iterator[Triple]:
    """Valid triples where all three names differ."""
    parts = request.param.split("|")
    yield Triple(*parts)


def test_from_conda_name(triple: Triple):
    names = CanonicalNames.from_conda_name(triple.conda_name, console)
    assert names.pypi_name == triple.pypi_name
    assert names.conda_name == triple.conda_name


def test_from_pypi_name(triple: Triple):
    names = CanonicalNames.from_pypi_name(triple.pypi_name, console)
    assert names.pypi_name == triple.pypi_name
    assert names.conda_name == triple.conda_name


def test_from_import_name(triple: Triple):
    names = CanonicalNames.from_import_name(triple.import_name, console)
    assert names.pypi_name == triple.pypi_name
    assert names.conda_name == triple.conda_name


# Same thing but via argparse options


def test_from_options__conda_name(triple: Triple):
    options = Namespace(query=triple.conda_name, query_type="conda")
    names = CanonicalNames.from_options(options, console)
    assert names.pypi_name == triple.pypi_name
    assert names.conda_name == triple.conda_name


def test_from_options__pypi_name(triple: Triple):
    # NOTE: PyPI is capitalized during options parsing; shouldn't matter
    options = Namespace(query=triple.pypi_name, query_type="PyPI")
    names = CanonicalNames.from_options(options, console)
    assert names.pypi_name == triple.pypi_name
    assert names.conda_name == triple.conda_name


def test_from_options__import_name(triple: Triple):
    options = Namespace(query=triple.import_name, query_type="import")
    names = CanonicalNames.from_options(options, console)
    assert names.pypi_name == triple.pypi_name
    assert names.conda_name == triple.conda_name
