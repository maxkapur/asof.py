import datetime
from pathlib import Path

import requests

from asof.conda import get_conda as get_conda
from asof.pypi import get_pypi as get_pypi

repo_root = Path(__file__).parent.parent
assert (repo_root / "pyproject.toml").is_file()

# TODO: Below should be a standalone config file
pypi_baseurl = "https://pypi.org"
conda_baseurl = "https://api.anaconda.org"
conda_channels = "defaults conda-forge".split()

downloads = {
    "name_mapping": requests.Request(
        "GET",
        "https://github.com/regro/cf-graph-countyfair/raw/refs/heads/master/mappings/pypi/name_mapping.json",
        headers={"Accept": "application/json"},
    ).prepare(),
}
cache_path = repo_root / "cache.db"
cache_lifetime = datetime.timedelta(days=1)
