from springtime import build_workflow, run_workflow

# dataset and phenophases to test
dataset = "daymet_ppo"

# ppo specifications
ppo = {
    "genus": "Syringa",
    "source": "USA-NPN",
    "year": "[2019 TO 2020]",
    "latitude": "[35 TO 38]",
    "longitude": "[-80 TO -75]",
    "termID": "obo:PPO_0002313",
}

# daymet specifications
daymet = {
    "longitudes" : [-80, -79.5],
    "latitudes" : [35, 35.1],
    "var_names" : ['tmax', 'tmin', 'prcp'],
    "years" : [2019, 2020],
}

# create options
options = {
    "obs_options": ppo,
    "eo_options": daymet,
    "model_name": ["LinearRegression"],
    "train_test_strategy": "ShuffleSplit",
    "metric_name": "RMSE",
    "usecase_id": "1",
}

workflow_name = f"usecase1_{dataset}"
# TODO not working yet
workflow = build_workflow(options, name=workflow_name)
