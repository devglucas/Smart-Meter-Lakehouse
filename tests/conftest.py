import pytest

from src import sessao


@pytest.fixture(scope="session")
def spark():
    return sessao.criar(memoria="2g", particoes=4)
