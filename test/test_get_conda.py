import datetime
import re

import pytest
from packaging.version import Version

from asof.conda import get_conda
from asof.package_match import PackageMatch


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
                    datetime.datetime.fromisoformat("2022-02-12T06:52:53+00:00"),
                    "conda-forge",
                )
            ],
        )
    ],
)
def test_get_pypi__ok(
    when: datetime.datetime, package: str, expected_matches: list[PackageMatch]
):
    res = get_conda(when, package)
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
    res = get_conda(when, package)
    assert res.matches == []
    assert res.message is not None
    assert re.match(
        f"No matches for {package} available from requested conda channels", res.message
    )
