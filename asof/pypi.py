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
from packaging.version import VERSION_PATTERN, Version

import asof
from asof.package_match import PackageMatch

VERSION_PATTERN = re.compile(VERSION_PATTERN, re.VERBOSE | re.IGNORECASE)


def get_pypi(when: datetime.datetime, package: str) -> list[PackageMatch]:
    resp = requests.get(
        f"{asof.pypi_baseurl}/simple/{package}/",
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    json_data = resp.content.decode()

    file_objs = json.loads(json_data)["files"]

    # To avoid having to parse every entry in full, start by grouping by version
    # string (from the filename)
    grouped = defaultdict(list)
    for file_obj in file_objs:
        if m := VERSION_PATTERN.search(file_obj["filename"]):
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
                # If we already have matches, then we already have a prerelease
                # higher than this one
                continue

            m = PackageMatch(package, version_obj, dt, asof.pypi_baseurl)
            matches.append(m)

            if not version_obj.is_prerelease:
                # Highest non-prerelease match found == done
                return matches

    return matches


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
