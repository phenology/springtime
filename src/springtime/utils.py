# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
from logging import getLogger
import time
import signal
from functools import wraps

from typing import Sequence, Tuple
from pydantic import BaseModel, validator
from shapely.geometry import Polygon

logger = getLogger(__name__)


class NamedArea(BaseModel):
    # TODO generalize
    # perhaps use https://github.com/developmentseed/geojson-pydantic
    name: str
    bbox: Tuple[float, float, float, float]

    @validator("bbox")
    def _parse_bbox(cls, values):
        xmin, ymin, xmax, ymax = values
        assert xmax > xmin, "xmax should be larger than xmin"
        assert ymax > ymin, "ymax should be larger than ymin"
        assert ymax < 90 and ymin > 0, "Latitudes should be in [0, 90]"
        assert xmin > -180 and xmax < 180, "Longitudes should be in [-180, 180]."
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

    Args:
        timeout: _description_. Defaults to 10.
        max_tries: _description_. Defaults to 3.
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
