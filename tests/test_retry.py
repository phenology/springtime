# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

import subprocess
import time

import pytest

from springtime.utils import TimeoutError, retry


@retry(timeout=3, max_tries=3)
def long_function():
    time.sleep(5)


@retry(timeout=3, max_tries=3)
def long_r_subprocess():
    subprocess.run(["R", "--no-save"], input="Sys.sleep(5)".encode())


def test_long_function():
    with pytest.raises(TimeoutError):
        long_function()


def test_r_subprocess():
    with pytest.raises(TimeoutError):
        long_r_subprocess()
