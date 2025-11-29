import datetime
import json
import re
import subprocess
import warnings
from collections import defaultdict

from packaging.version import Version
from packaging.version import version_pattern as version_pattern_str

import asof
from asof.package_match import MatchesOption, PackageMatch

version_pattern: re.Pattern = re.compile(
    version_pattern_str, re.VERBOSE | re.IGNORECASE
)


def get_conda_command() -> str | None:
    for command in "mamba conda".split():
        try:
            subprocess.run([command], capture_output=True)
            return command
        except FileNotFoundError:
            pass
    return None


def get_conda(
    conda_command: str, when: datetime.datetime, package: str
) -> MatchesOption:
    cmd = [
        conda_command,
        "search",
        "--json",
        package,
        "--override-channels",  # Use only explicitly named channels
    ]
    if conda_command == "conda":
        cmd.append(
            "--skip-flexible-search"
        )  # Disable retrying search for "*<package>*"
    for channel in asof.conda_channels:
        # TODO: Do we actually want this? Or should we just let conda handle the
        # channel config to avoid having to maintain another setting
        cmd.extend(["--channel", channel])

    res = subprocess.run(cmd, capture_output=True)

    no_matches_msg = f"No matches for {package} available from requested conda channels"
    if res.returncode != 0:
        if "PackagesNotFoundError" in res.stderr.decode():
            return MatchesOption([], no_matches_msg)

        else:
            # TODO: Error output is not strictly structured but we may be able
            # to extract additional common cases with regex
            return MatchesOption(
                [], f"{conda_command} exited with status {res.returncode}"
            )

    parsed = json.loads(res.stdout.decode())
    _, file_objs = parsed.popitem()

    if "pkgs" in file_objs:
        # Seems to be a conda vs. mamba difference
        file_objs = file_objs["pkgs"]

    # To avoid having to parse every entry in full, start by grouping by version
    # string (from the filename)
    grouped = defaultdict(list)
    for file_obj in file_objs:
        if m := version_pattern.match(file_obj["version"]):
            version_str = m.group(0)
            grouped[version_str].append(file_obj)
        else:
            warnings.warn(f"Unable to parse version name {file_obj['version']}")

    # Now parse only these keys to Version objects and sort from highest to
    # lowest. The API JSON tends to put newer versions toward the end, so the
    # keys are probably *already* almost sorted, so it will be fastest to sort
    # ascending and then reverse:
    version_strs = sorted(grouped.keys(), key=Version)
    version_strs.reverse()

    # TODO: Lot of discrepancies between JSON returned by mamba vs. conda, need
    # to create separate functions.

    def get_matches():
        matches = []
        for version_str in version_strs:
            for file_obj in grouped[version_str]:
                # Timestamp is integer milliseconds since Unix epoch. Ancient
                # results have no timestamp, just assume they are old :)
                timestamp = file_obj.get("timestamp", 0) / 1000
                dt = datetime.datetime.fromtimestamp(timestamp)
                dt = dt.replace(tzinfo=datetime.timezone.utc)
                if dt > when:
                    continue

                version_obj = Version(file_obj["version"])
                if version_obj.is_prerelease and matches:
                    # If we already have matches, then we already have a
                    # prerelease higher than this one
                    continue

                m = PackageMatch(package, version_obj, dt, file_obj["channel"])
                matches.append(m)

                if not version_obj.is_prerelease:
                    # Highest non-prerelease match found == done
                    return matches

    if matches := get_matches():
        return MatchesOption(matches, None)
    else:
        return MatchesOption([], no_matches_msg)
