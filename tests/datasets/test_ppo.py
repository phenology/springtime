from textwrap import dedent

import geopandas as gpd
import pandas as pd
import pytest

from springtime.config import CONFIG
from springtime.datasets import RPPO, load_dataset

"""
To update reference data, run one of the following:

    pytest tests/test_ppo.py --update-reference
    pytest tests/test_ppo.py --update-reference --redownload


To include download, run:

    pytest tests/test_ppo.py --include-downloads
"""

REFERENCE_DATA = CONFIG.cache_dir / "ppo_load_reference.geojson"
REFERENCE_RECIPE = dedent(
    """\
      dataset: rppo
      years:
      - 1990
      - 2020
      genus: Syringa
      termID: obo:PPO_0002032
      limit: 10000
      timeLimit: 60
      exclude_terms:
      - obo:PPO_0002335
      infer_event: first_yes_day
        """
)


@pytest.fixture
def reference_args():
    return dict(
        years=[1990, 2020],
        genus="Syringa",
        termID="obo:PPO_0002032",  # flowers present
        exclude_terms=["obo:PPO_0002335"],  # senesced flowers present
        infer_event="first_yes_day",
        limit=10000,
    )


def test_load_reference(reference_args):
    RPPO(**reference_args).load()


def test_to_recipe(reference_args):
    dataset = RPPO(**reference_args)
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe(reference_args):
    original = RPPO(**reference_args)
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload(reference_args):
    original = RPPO(**reference_args)
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.download
def test_download(temporary_cache_dir, reference_args):
    """Check download hasn't changed; also uses raw_load"""
    dataset = RPPO(**reference_args)

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.raw_load()

    with temporary_cache_dir():
        dataset.download()
        new_data = dataset.raw_load()

    pd.testing.assert_frame_equal(new_data, reference)


@pytest.mark.download
def test_load_with_area(temporary_cache_dir, germany, reference_args):
    with temporary_cache_dir():
        RPPO(**reference_args, area=germany).load()


def test_load(reference_args):
    """Compare loaded (i.e. processed) data with stored reference."""
    dataset = RPPO(**reference_args)
    loaded_data = dataset.load()
    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values])


@pytest.mark.update
def test_update_reference_data(redownload, reference_args):
    """Update the reference data for these tests."""
    dataset = RPPO(**reference_args)
    if redownload and dataset._path().exists():
        dataset._path().unlink()
    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)
