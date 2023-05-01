# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from springtime.datasets.pyphenology import PyPhenologyDataset


def test_api():
    ds = PyPhenologyDataset(name="aspen", phenophase="budburst", years=[2010, 2011])
    ds.location
    ds.download()
    assert ds.exists_locally() is True
    _ = ds.load()
