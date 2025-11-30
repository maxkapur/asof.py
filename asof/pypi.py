import datetime
import json
import re
import warnings
from collections import defaultdict

import requests
from packaging.tags import sys_tags
from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import VERSION_PATTERN as version_pattern_str
from packaging.version import Version
from rich.status import Status

import asof
from asof.package_match import MatchesOption, PackageMatch

version_pattern: re.Pattern = re.compile(
    version_pattern_str, re.VERBOSE | re.IGNORECASE
)


def get_pypi(when: datetime.datetime, package: str) -> MatchesOption:
    url = f"{asof.pypi_baseurl}/simple/{package}/"
    with Status(f"Querying PyPI at {url}"):
        resp = requests.get(
            url,
            headers={"Accept": "application/vnd.pypi.simple.v1+json"},
        )
    if not resp.ok:
        return MatchesOption(
            [],
            f"{resp.status_code}: {resp.reason} when attempting to get query PyPI at {url}",
        )

    json_data = resp.content.decode()
    file_objs = json.loads(json_data)["files"]

    # To avoid having to parse every entry in full, start by grouping by version
    # string (from the filename)
    grouped = defaultdict(list)
    for file_obj in file_objs:
        if m := version_pattern.search(file_obj["filename"]):
            version_str = m.group(0)
            grouped[version_str].append(file_obj)
        else:
            warnings.warn(f"Unable to parse version name {file_obj['filename']}")

    # Now parse only these keys to Version objects and sort from highest to
    # lowest. The API JSON tends to put newer versions toward the end, so the
    # keys are probably *already* almost sorted, so it will be fastest to sort
    # ascending and then reverse:
    version_strs = sorted(grouped.keys(), key=Version)
    version_strs.reverse()

    # Walk backwards through versions and return newest release version and
    # newest prerelease (if available)
    def get_matches():
        matches = []
        for version_str in version_strs:
            for file_obj in grouped[version_str]:
                if file_obj["yanked"]:
                    continue

                dt = datetime.datetime.fromisoformat(file_obj["upload-time"])
                if dt > when:
                    continue

                version_obj = is_compatible(file_obj)
                if version_obj is None:
                    continue
                if version_obj.is_prerelease and matches:
                    # If we already have matches, then we already have a
                    # prerelease higher than this one
                    continue

                m = PackageMatch(package, version_obj, dt, asof.pypi_baseurl)
                matches.append(m)

                if not version_obj.is_prerelease:
                    # Highest non-prerelease match found == done
                    return matches

    if matches := get_matches():
        return MatchesOption(matches, None)
    else:
        return MatchesOption(
            [],
            f"No compatible releases or prereleases on PyPI as of {when.isoformat()} for package {package}",
        )


def is_compatible(file_obj: dict) -> Version | None:
    """Inspect the PyPI filename and determine compatibility with my system.

    Return the version if so.
    """
    filename = file_obj["filename"]
    try:
        # sdist filename doesn't contain any compat info, so just assume so
        _, version = parse_sdist_filename(filename)
        return version
    except InvalidSdistFilename:
        pass

    try:
        _, version, _, tags = parse_wheel_filename(filename)
        for t in sys_tags():
            if t in tags:
                return version
        return None
    except InvalidWheelFilename:
        pass

    # Could be an ancient .exe or other obsolete packaging format
    return None
