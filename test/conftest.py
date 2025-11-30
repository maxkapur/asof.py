import pytest

import asof.db


@pytest.fixture(scope="session", autouse=True)
def setup():
    asof.db.initialize_db()
