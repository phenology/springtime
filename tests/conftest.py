
import pytest


# Add marker to skip download tests
# https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option

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
