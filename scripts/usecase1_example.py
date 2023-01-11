"""
Train a (ML) model with day of first bloom from pheno observations as target and
weather data as predictors. The observations are available for a few locations,
whereas the weather data is available on a full raster. So, we want to apply the
model for the full raster.

output: 69.767
"""

from springtime import build_workflow, run_workflow

# ppo specifications
ppo_options = {
    "genus": "Syringa",
    "source": "USA-NPN", # because daymet covers USA
    "year": "[2019 TO 2020]",
    "latitude": "[35 TO 38]",
    "longitude": "[-80 TO -75]",
    "termID": "obo:PPO_0002313", # true leaves present
}

# daymet specifications
daymet_options = {
    "longitudes": [-80, -79.5], # very high resolution data, so small area
    "latitudes": [35, 35.1],
    "var_names": ['tmax', 'tmin', 'prcp'],
    "years": [2019, 2020],
    "statistics": "annual monlthly average",
}

# create options for workflow
options = {
    "obs_options": ppo_options,
    "eo_options": daymet_options,
    "model_name": "LinearRegression",
    "train_test_strategy": "ShuffleSplit",
    "metric_name": "RMSE",
    "usecase_id": "1",
}

workflow = build_workflow(options, name="usecase1_daymet_ppo")
results = run_workflow(workflow)
print(results["metric_value"])
