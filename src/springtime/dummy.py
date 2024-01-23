import geopandas as gpd
import numpy as np
import pandas as pd


def observations(n=10):
    """Generate random observation data for testing."""
    return gpd.GeoDataFrame(
        data={
            "year": np.arange(2000, 2000 + n),
            "geometry": gpd.GeoSeries.from_xy(*np.random.randn(2, n)),
            "spring onset (DOY)": np.random.randint(120, 180, size=n),
        },
    ).set_index(["year", "geometry"])


def generate_predictor(n=365, name="temperature"):
    return pd.Series(np.random.randn(n), index=np.arange(1, n + 1), name=name)


def generate_predictors(observations, name="temperature"):
    """Given a year and location, generate random temperatures for each DOY."""
    predictors = observations.apply(lambda row: generate_predictor(), axis=1)
    return pd.concat([observations[["year", "geometry"]], predictors], axis=1)
    # return predictors.assign(year=observations.year, geometry=observations.geometry)


def pycaret_ready(n=100):
    """Generate dummy data for use with pycaret."""
    obs = observations(n=n)
    combined = obs.assign(temperature=[generate_predictor() for row in obs.iterrows()])
    features = combined.temperature.apply(lambda s: s.agg(["min", "mean", "max"]))
    return (
        pd.concat([combined, features], axis=1)
        .drop("temperature", axis=1)
        .rename(
            columns=dict(
                mean="mean temperature",
                max="maximum temperature",
                min="minimum temperature",
            )
        )
    )


# def generate_predictor(observations, name="temperature"):
#     """Given a year and location, generate random temperatures for each DOY."""
#     dummy = observations.apply(
#         lambda row: pd.Series(np.random.randn(365),index=np.arange(1, 366),name=name),
#         axis=1,
#     )
#     # return dummy
#     return observations[["year", "geometry"]].assign(
#         temperature=[pd.Series(v) for v in dummy.values]
#     )
