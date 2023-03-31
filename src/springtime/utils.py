# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
import signal
import subprocess
import time
from functools import wraps
from logging import getLogger
from typing import NamedTuple, Sequence

import geopandas as gpd
from pydantic import BaseModel, validator
from shapely.geometry import Polygon

logger = getLogger(__name__)


class BoundingBox(NamedTuple):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


class NamedArea(BaseModel):
    # TODO generalize
    # perhaps use https://github.com/developmentseed/geojson-pydantic
    name: str
    bbox: BoundingBox

    @validator("bbox")
    def _parse_bbox(cls, values):
        xmin, ymin, xmax, ymax = values
        assert xmax > xmin, "xmax should be larger than xmin"
        assert ymax > ymin, "ymax should be larger than ymin"
        assert ymax < 90 and ymin > 0, "Latitudes should be in [0, 90]"
        assert (
            xmin > -180 and xmax < 180
        ), "Longitudes should be in [-180, 180]."
        return values

    @property
    def polygon(self):
        return Polygon.from_bounds(*self.bbox)


class NamedIdentifiers(BaseModel):
    name: str
    items: Sequence[int]


# Decorators copied from https://wiki.python.org/moin/PythonDecoratorLibrary


def retry(timeout=10, max_tries=3, delay=1, backoff=2):
    """Decorator to retry function with timeout.

    The decorator will call the function up to max_tries times if it raises
    an exception.

    Note:
        Decorating function which calls rpy2 does not work. Please use
        :func:run_r_script method instead.

    Args:
        timeout: Maximum mumber of seconds the function may take.
        max_tries: Maximum number of times to execute the function.
        delay: Sleep this many seconds * backoff * try number after failure
        backoff: Multiply delay by this factor after each failure

    Raises:
        TimeoutError: When max tries is reached and last try timedout.

    """

    def dec(function):
        def _handle_timeout(signum, frame):
            raise TimeoutError("Function call timed out")

        @wraps(function)
        def f2(*args, **kwargs):
            mydelay = delay
            tries = list(range(max_tries))
            tries.reverse()
            for tries_remaining in tries:
                oldsignal = signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(timeout)
                try:
                    return function(*args, **kwargs)
                except TimeoutError:
                    if tries_remaining > 0:
                        logger.warn(
                            f"Function call took more than {timeout} seconds, retrying"
                        )
                        time.sleep(mydelay)
                        mydelay = mydelay * backoff
                    else:
                        raise
                else:
                    break
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, oldsignal)

        return f2

    return dec


class TimeoutError(Exception):
    """Timeout Exception.

    When a function takes too long to run when using the :func:retry decorator.

    """

    pass


def run_r_script(script: str, timeout=30, max_tries=3):
    """Run R script with retries and timeout logic.

    Args:
        script: The R script to run
        timeout: Maximum mumber of seconds the function may take.
        max_tries: Maximum number of times to execute the function.
    """
    result = retry(timeout=timeout, max_tries=max_tries)(subprocess.run)(
        ["R", "--vanilla", "--no-echo"],
        input=script.encode(),
        stderr=subprocess.PIPE,
    )
    result.check_returncode()


def transponse_df(df, index=("year", "geometry"), columns=("doy",)):
    """Many dataset do not have rows per year and geometry,
    but more frequent like daily.

    This method pivots the rows to columns.

    Args:
        df: _description_
        index: _description_. Defaults to ('year', 'geometry').
        columns: _description_. Defaults to ('doy',).

    Returns:
        Data frame with year and geometry column and
        columns named `<original column name>_<doy>`.
    """
    pdf = df.pivot(index=index, columns=columns).reset_index()
    pdf.columns = [
        "_".join(map(str, filter(lambda x: x != "", i)))
        for i in pdf.columns.values
    ]
    return gpd.GeoDataFrame(pdf)


def rolling_mean(
    df,
    over,
    groupby=("year", "geometry"),
    window_sizes=(3, 7, 15, 30, 90, 365),
):
    """Group by `groupby` columns and calculate rolling mean
    for `over` columns with different window sizes.

    Args:
        df: _description_
        over: _description_. Defaults to ('measurementx',).
        groupby: _description_. Defaults to ('year', 'geometry').
        window_sizes: _description_. Defaults to [3,7,15,30,90,365].

    Returns:
        _description_
    """
    # TODO implement
    return df
