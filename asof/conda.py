import datetime
import json
import subprocess

import asof
from asof.package_match import PackageMatch


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
) -> PackageMatch | None:
    cmd = [conda_command, "search", "--json", package]
    for channel in asof.conda_channels:
        cmd.extend(["--channel", channel])

    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        # TODO: Helpful message
        return None

    # TODO: Parse
    parsed = json.loads(res.stdout.decode())
    return parsed
