import datetime
import re

import pytest
from packaging.version import Version

from asof.conda import get_conda, get_conda_command
from asof.package_match import PackageMatch

CONDA_INSTALLED = get_conda_command() is not None
MAMBA_INSTALLED = get_conda_command() == "mamba"

# Tests with conda_command="conda"


@pytest.mark.skipif(not CONDA_INSTALLED, reason="conda not installed")
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
                    # NOTE: conda JSON returns down to milliseconds, whereas
                    # mamba just does seconds
                    datetime.datetime.fromisoformat("2022-02-12T12:52:53.028000+00:00"),
                    # NOTE: conda JSON returns a URL like this, whereas mamba
                    # returns just the channel name "conda-forge"
                    "https://conda.anaconda.org/conda-forge/linux-64",
                )
            ],
        )
    ],
)
def test_get_conda__conda__ok(
    when: datetime.datetime,
    package: str,
    expected_matches: list[PackageMatch],
):
    res = get_conda(when, package, conda_command="conda")
    assert res.matches == expected_matches
    assert res.message is None


@pytest.mark.skipif(not CONDA_INSTALLED, reason="conda not installed")
@pytest.mark.parametrize(
    "when,package",
    [
        (
            datetime.datetime.fromisoformat("2022-03-04T00:00:00Z"),
            "DNE_afdgjkfdslghjkdgfhjdkl",
        )
    ],
)
def test_get_conda__conda__empty(when: datetime.datetime, package: str):
    res = get_conda(when, package, conda_command="conda")
    assert res.matches == []
    assert res.message is not None
    assert re.match(
        f"No matches for {package} available from requested conda channels", res.message
    )


# Tests with conda_command="mamba"


@pytest.mark.skipif(not MAMBA_INSTALLED, reason="mamba not installed")
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
                    datetime.datetime.fromisoformat("2022-02-12T12:52:53+00:00"),
                    "conda-forge",
                )
            ],
        )
    ],
)
def test_get_conda__mamba__ok(
    when: datetime.datetime,
    package: str,
    expected_matches: list[PackageMatch],
):
    res = get_conda(when, package, conda_command="mamba")
    assert res.matches == expected_matches
    assert res.message is None


@pytest.mark.skipif(not MAMBA_INSTALLED, reason="mamba not installed")
@pytest.mark.parametrize(
    "when,package",
    [
        (
            datetime.datetime.fromisoformat("2022-03-04T00:00:00Z"),
            "DNE_afdgjkfdslghjkdgfhjdkl",
        )
    ],
)
def test_get_conda__mamba__empty(when: datetime.datetime, package: str):
    res = get_conda(when, package, conda_command="mamba")
    assert res.matches == []
    assert res.message is not None
    assert re.match(
        f"No matches for {package} available from requested conda channels", res.message
    )
