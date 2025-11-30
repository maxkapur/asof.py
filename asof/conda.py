import datetime
import json
import re
import shlex
import subprocess
import warnings
from collections import defaultdict
from typing import Literal

from packaging.version import VERSION_PATTERN as version_pattern_str
from packaging.version import Version
from rich.status import Status

from asof.package_match import MatchesOption, PackageMatch

version_pattern: re.Pattern = re.compile(
    version_pattern_str, re.VERBOSE | re.IGNORECASE
)

CondaCommand = Literal["mamba", "conda"]


def get_conda_command() -> CondaCommand | None:
    """Guess the user's preferred conda command as mamba, conda, or None."""
    with Status("Determining conda command"):
        for command in "mamba", "conda":
            try:
                subprocess.run([command], capture_output=True)
                return command
            except FileNotFoundError:
                pass
        return None


def extract_file_objs(
    conda_command: CondaCommand,
    parsed_json: dict,
) -> list[dict]:
    """Extract the list of package matches from the JSON response.

    Necessary because the mamba and conda commands format their JSON slightly
    differently.
    """
    match conda_command:
        case "mamba":
            return parsed_json["result"]["pkgs"]
        case "conda":
            # JSON has just one key, which is the package name requested
            _, res = parsed_json.popitem()
            return res
        case _:
            raise ValueError


def timestamp_to_datetime(
    conda_command: CondaCommand,
    timestamp: int,
) -> datetime.datetime:
    """Convert the timestamp (integer) to a Python datetime.

    Mamba uses seconds since Unix epoch, while conda uses milliseconds.
    """
    match conda_command:
        case "mamba":
            dt = datetime.datetime.fromtimestamp(timestamp)
        case "conda":
            dt = datetime.datetime.fromtimestamp(timestamp / 1000)
        case _:
            raise ValueError

    # Need to add timezone info to compute difference with "now"
    return dt.replace(tzinfo=datetime.timezone.utc)


def get_conda(
    when: datetime.datetime,
    package: str,
    conda_command: CondaCommand | None = get_conda_command(),
) -> MatchesOption:
    if conda_command is None:  # still
        return MatchesOption(
            [],
            "Unable to query conda repos as neither conda nor mamba command available",
        )

    cmd = [
        conda_command,
        "search",
        "--json",
        package,
    ]
    if conda_command == "conda":
        # Disable retrying search for "*<package>*"; only conda has this feature
        cmd.append("--skip-flexible-search")

    with Status(f"Querying conda repo: {shlex.join(cmd)}"):
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
    file_objs = extract_file_objs(conda_command, parsed)

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

    def get_matches():
        matches = []
        for version_str in version_strs:
            for file_obj in grouped[version_str]:
                # Ancient results have no timestamp, just assume they are old :)
                timestamp = file_obj.get("timestamp", 0)
                dt = timestamp_to_datetime(conda_command, timestamp)
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
