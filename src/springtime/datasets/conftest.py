import pandas as pd
import pytest

# See https://github.com/pytest-dev/pytest/issues/4030

@pytest.fixture(autouse=True, scope='session')
def pandas_terminal_width():
    # pd.set_option('display.width', None)
    pd.set_option("display.max_columns", 7)
    # pd.set_option("display.max_colwidth", 20)
