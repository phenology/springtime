"""
Train a (ML) model with day of first bloom from pheno observations as target and
weather data as predictors. The observations are available for a few locations,
whereas the weather data is available on a full raster. So, we want to apply the
model for the full raster.

output: 49.238
"""

from springtime import build_workflow, run_workflow

# ppo specifications
ppo_options = {
    "genus": "Syringa",
    "source": "USA-NPN", # because daymet covers USA
    "year": "[2019 TO 2020]",
    "latitude": "[35 TO 41]",
    "longitude": "[-88 TO -84]",
    "termID": "obo:PPO_0002313", # true leaves present
}

# daymet specifications
daymet_options = {
    "longitudes": [-88, -84], # very high resolution data, so small area
    "latitudes": [35, 41],
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

## visualization
import matplotlib.pyplot as plt
from cartopy import crs
from springtime import data_utils

### Field observations (truth) = PPO 
obs = results["data"]["obs_data"]
unique_idx = ['latitude', 'longitude', 'year']
event_df = obs.groupby(unique_idx).dayOfYear.mean()
event = event_df.reset_index()
fig = plt.figure()
ax = fig.add_subplot(projection=crs.PlateCarree())
ax.coastlines()
ax.set_xlim(-80, -78)
ax.set_ylim(35, 37)
sc = ax.scatter(event.longitude, event.latitude, c=event.dayOfYear)
fig.savefig('observations', bbox_inches='tight')
plt.colorbar(sc)

### Raster Weather data
# TODO add plotting
eo = results["data"]["eo_data"]

## Weather/ Satellite data (masked by observations)
# TODO add plotting
eo_obs = data_utils.merge_eo_obs(eo, obs)

### Predicted truth
import xarray as xr
predictors = results["data"]["eo_data"]
predicted_doy = y_predict.reshape(predictors.sizes['x'], predictors.sizes['y'], 1)

da = xr.DataArray(
    data=predicted_doy,
    dims=["x", "y", "year"],
    coords=dict(
        x=("x", predictors.x.values),
        y=("y", predictors.y.values),
        year=[2019],
    ),
    attrs=dict(
        description="Day of year.",
    ),
)
da.plot()