
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


### Add markers to skip download tests and for updating reference data
### https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option

def pytest_addoption(parser):
    # store_true sets option to True if options is passed, default is False
    # https://stackoverflow.com/a/8203679
    parser.addoption(
        "--include-downloads", action="store_true", help="Also test download functionality."
    )
    parser.addoption(
        "--update-reference", action="store_true", help="Regenerate reference data used in tests."
    )
    parser.addoption(
        "--redownload", action="store_true", help="Remove test cache before updating reference data."
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "download: mark test as including downloads")
    config.addinivalue_line("markers", "update: mark test as function to update reference data")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--update-reference"):
        update_items = []
        for item in items:
            if "update" in item.keywords:
                update_items.append(item)

        items[:] = update_items
        return

    if not config.getoption("--include-downloads"):
        skip_download = pytest.mark.skip(reason="need --include-downloads option to run")
        for item in items:
            if "download" in item.keywords:
                item.add_marker(skip_download)

@pytest.fixture()
def redownload(pytestconfig):
    """Return the value of redownload from command-line options."""
    return pytestconfig.getoption("redownload")


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
