"""
Train a (ML) model with day of first bloom from pheno observations as target and
weather data as predictors. The observations are available for a few locations,
whereas the weather data is available on a full raster. So, we want to apply the
model for the full raster.

output: 45.55
"""

from springtime import build_workflow, run_workflow

# ppo specifications
ppo_options = {
    "genus": "Syringa",
    "source": "USA-NPN", # because daymet covers USA
    "year": "[2019 TO 2020]",
    "latitude": "[39.5 TO 40.1]",
    "longitude": "[-86.5 TO -86]",
    "termID": "obo:PPO_0002313", # true leaves present
}

# daymet specifications
daymet_options = {
    "longitudes": [-86.5, -86], # very high resolution data, so small area
    "latitudes": [39.5, 40.1],
    "var_names": ['tmax', 'tmin', 'prcp'], # features
    "years": [2019, 2020],
    "statistics": "annual seasonal average", # convert daily to seasonal
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

# run workflow: prepare data, train model
workflow = build_workflow(options, name="usecase1_daymet_ppo")
results = run_workflow(workflow)
print(results["metric_value"])

# prediction
## select data
x_predict = results["data"]["train_test"]["predictors"]
x_predict = x_predict.query("year == 2019")

model = results["model"]
y_predict = model.predict(x_predict)

### Predicted truth as xarray
import xarray as xr
predictors = results["data"]["eo_data"]
predicted_doy = y_predict.reshape(predictors.sizes['x'], predictors.sizes['y'], 1)

lat = predictors.lat.values[0,:,:]
lat = lat.reshape(1, predictors.sizes['y'], predictors.sizes['x'])
lon = predictors.lon.values[0,:,:]
lon = lon.reshape(1, predictors.sizes['y'], predictors.sizes['x'])
da = xr.DataArray(
    data=predicted_doy,
    dims=["x", "y", "year"],
    coords=dict(
        x=("x", predictors.x.values),
        y=("y", predictors.y.values),
        year=[2019],
        lat=(["year", "y", "x"], lat),
        lon = (["year", "y", "x"], lon),
    ),
    attrs=dict(
        description="Day of year.",
    ),
)
da.to_netcdf("DayOfYear_2019_predcited.nc")