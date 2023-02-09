from springtime.datasets.pyphenology import PyPhenologyDataset


def test_api():
    ds = PyPhenologyDataset(name="aspen", phenophase="budburst")
    ds.location
    ds.download()
    assert ds.exists_locally() == True
    _ = ds.load()
