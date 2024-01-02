from springtime.datasets import PEP725Phenor, load_dataset

import pytest


@pytest.fixture
def area():
    return {
    "name": "Germany",
    "bbox": [
        5.98865807458,
        47.3024876979,
        15.0169958839,
        54.983104153,
    ],
}



def test_simple_recipe():
    original = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


def test_with_area(area)
    original = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002], area=area)
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.skip(reason="Don't want to access network during tests.")
def test_raw_load():
    pass


def test_load():
    # TODO mock raw_load output to verify that load does the right thing.
    pass
