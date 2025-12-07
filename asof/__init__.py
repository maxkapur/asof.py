import datetime

import requests
from platformdirs import user_cache_path

from asof.conda import get_conda as get_conda
from asof.pypi import get_pypi as get_pypi

# TODO: Below should be a standalone config file
pypi_baseurl = "https://pypi.org"
downloads = {
    "name_mapping": requests.Request(
        "GET",
        "https://github.com/regro/cf-graph-countyfair/raw/refs/heads/master/mappings/pypi/name_mapping.json",
        headers={"Accept": "application/json"},
    ).prepare(),
}
cache_path = user_cache_path() / "python-asof" / "cache.db"
cache_lifetime = datetime.timedelta(days=1)
