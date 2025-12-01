import datetime

import pytest
from packaging.version import Version

from asof.package_match import PackageMatch
from asof.pypi import get_pypi


@pytest.mark.parametrize(
    "when,package,expected_matches",
    [
        (
            datetime.datetime.fromisoformat("2022-03-04T00:00:00Z"),
            "pandas",
            [
                PackageMatch(
                    "pandas",
                    Version("v1.4.1"),
                    datetime.datetime.fromisoformat("2022-02-12T11:25:08.392839+00:00"),
                    "https://pypi.org",
                )
            ],
        )
    ],
)
def test_get_pypi__ok(
    when: datetime.datetime, package: str, expected_matches: list[PackageMatch]
):
    res = get_pypi(when, package)
    assert res.matches == expected_matches
    assert res.message is None


@pytest.mark.parametrize(
    "when,package",
    [
        (
            datetime.datetime.fromisoformat("2022-03-04T00:00:00Z"),
            "DNE_afdgjkfdslghjkdgfhjdkl",
        )
    ],
)
def test_get_pypi__empty(when: datetime.datetime, package: str):
    res = get_pypi(when, package)
    assert res.matches == []
    assert res.message is not None
    assert res.message.startswith("404: Not Found")
