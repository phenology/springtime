
from contextlib import contextmanager
import pytest

from springtime.config import CONFIG


### Sample data for tests is shipped with the package
TEST_CACHE = "tests/reference_data/"
CONFIG.cache_dir = TEST_CACHE


### Temporarily change the cache dir for regression tests

@pytest.fixture
def temporary_cache_dir(tmp_path):
    """Temporarily change the cache dir in config."""
    @contextmanager
    def context_manager():
        global CONFIG_DIR
        old_cache_dir = CONFIG.cache_dir
        CONFIG.cache_dir = tmp_path
        yield
        CONFIG.cache_dir = old_cache_dir
    return context_manager


### Add marker to skip download tests
### https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option

def pytest_addoption(parser):
    parser.addoption(
        "--include_downloads", action="store_true", default=False, help="Also test download functionality."
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "download: mark test as including downloads")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--include_downloads"):
        # --include_downloads given in cli: do not skip tests with downloads
        return
    skip_download = pytest.mark.skip(reason="need --include_downloads option to run")
    for item in items:
        if "download" in item.keywords:
            item.add_marker(skip_download)


### Test areas

@pytest.fixture
def germany():
    return {
        "name": "Germany",
        "bbox": [
            5.98865807458,
            47.3024876979,
            15.0169958839,
            54.983104153,
        ],
    }
