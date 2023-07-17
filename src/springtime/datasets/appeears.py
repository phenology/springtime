# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timezone
from hashlib import sha1
import json
from pathlib import Path
from time import sleep
from typing import Literal, Optional, Sequence, Tuple, Union

import requests
import geopandas
import pandas as pd
from shapely import to_geojson
import xarray as xr
from pydantic import BaseModel, Field, conset
import logging
from springtime.config import CONFIG, CONFIG_DIR
from springtime.datasets.abstract import Dataset
from springtime.utils import (
    NamedArea,
    Points,
    PointsFromOther,
    YearRange,
    points_from_cube,
)

logger = logging.getLogger(__name__)


class TokenInfo(BaseModel):
    token_type: str
    token: str
    expiration: datetime


# TODO add ability to load data from a task identifier,
# now you need to run the dowlnoad() completely online
# would be nice to submit tasks and once they are done,
# download the data


class Appeears(Dataset):
    product: str
    """An AppEEARS product name.
    
    Use `products()` to get a list of currently available in AppEEARS.
    """
    # TODO make optional, if not given, use latest version
    version: str
    """An AppEEARS product version."""
    layers: conset(str, min_items=1)  # type: ignore
    # TOD rename to variables or bands?
    """Layers of a AppEEARS product.

    Use `layers(product)` to get list of available layers for a product.
    """
    _token: Optional[TokenInfo] = None

    # TODO drop when pydantic v2 is used, as it will be default
    class Config:
        underscore_attrs_are_private = True

    def _check_token(self):
        token_fn = CONFIG.cache_dir / "appeears" / "token.json"
        if token_fn.exists():
            body = json.loads(token_fn.read_text())
            self._token = TokenInfo(**body)
        if self._token is None:
            print("L:ogin")
            username, password = _read_credentials()
            self._token = _login(username, password)
            token_fn.write_text(self._token.json())
        if self._token.expiration < datetime.now(timezone.utc):
            username, password = _read_credentials()
            self._token = _login(username, password)
            logger.info("Token expired, logging in again")
            token_fn.write_text(self._token.json())

    @property
    def output_dir(self):
        """Output directory for downloaded data."""
        d = CONFIG.cache_dir / "appeears"
        d.mkdir(exist_ok=True, parents=True)
        return d


class AppeearsArea(Appeears):
    """MODIS land products subsets from AppEEARS by an area of interest.

    https://appeears.earthdatacloud.nasa.gov/

    Credentials are read from `~/.config/springtime/credentials.json`.
    JSON file should look like `{"username": "foo", "password": "bar"}`.
    """

    dataset: str = "appeears_area"
    area: NamedArea

    @property
    def _path(self):
        # MCD15A2H.061_500m_aid0001.nc
        resolution = list(self.layers)[0].split("_")[1]
        return f"{self.product}.{self.version}_{resolution}_aid0001.nc"

    @property
    def output_dir(self):
        d = super().output_dir / self.area.name
        d.mkdir(exist_ok=True, parents=True)
        return d

    def download(self):
        fn = self.output_dir / self._path
        if (fn).exists():
            logger.warning(f"File {fn} exists, not downloading again")
            return
        self._check_token()
        task = _submit_area_task(
            product=self.product,
            version=self.version,
            area=self.area,
            layers=self.layers,
            years=self.years,
            token=self._token,
        )
        _poll_task(task, token=self._token)
        logger.warning(f"Task {task} completed")
        files = _list_files(task, token=self._token)
        for file in files:
            if file.name == self._path:
                file.download(task, self.output_dir, token=self._token)

    def load(self):
        ds = xr.open_dataset(self.output_dir / self._path)
        return ds


class AppeearsPointsFromArea(AppeearsArea):
    """MODIS land products subsets using AppEEARS by points from an area of interest.

    First the bounding box of area is downloaded and
    then points are selected from the area.

    This class could be quicker then AppeearsPoints
    if the amount of points is large and in a small area.

    https://appeears.earthdatacloud.nasa.gov/

    Credentials are read from `~/.config/springtime/credentials.json`.
    JSON file should look like `{"username": "foo", "password": "bar"}`.
    """

    dataset: Literal["appeears_points_from_area"] = "appeears_points_from_area"
    # TODO when area is not given use bounding box of points
    points: Union[Sequence[Tuple[float, float]], PointsFromOther]

    def load(self):
        ds = super().load()
        # Convert cftime.DatetimeJulian to datetime.datetime
        datetimeindex = ds.indexes["time"].to_datetimeindex()
        ds["time"] = datetimeindex
        # Coords are in geometric projection no need for crs variable
        ds = ds.drop_vars(["crs"])
        df = points_from_cube(ds, self.points)
        # Drop QC variables
        df = df.filter(regex="^(?!.*_QC$)")
        df.rename({"time": "datetime"}, axis=1, inplace=True)
        return df


class AppeearsPoints(Appeears):
    """MODIS land products subsets using AppEEARS.

    https://appeears.earthdatacloud.nasa.gov/

    Credentials are read from `~/.config/springtime/credentials.json`.
    JSON file should look like `{"username": "foo", "password": "bar"}`.
    """

    dataset: Literal["appeears_points"] = "appeears_points"
    points: Union[Sequence[Tuple[float, float]], PointsFromOther]

    def _points_hash(self, points: Points):
        return sha1(json.dumps(points).encode("utf-8")).hexdigest()

    def _task_name(self, points: Points):
        return _generate_task_name(
            product=self.product,
            points=self._points_hash(points),
            layers=self.layers,
            years=self.years,
        )

    def _path(self, points: Points):
        chunk = f"{self._task_name(points)}-{self.product}-{self.version}"
        return f"{chunk}-results.csv".replace("_", "-")

    def download(self):
        # api can not handle more than 500 points (of 1 product, 2 layers)
        # at once so we split the points into chunks
        points_chunks = [
            self.points[i : i + 500] for i in range(0, len(self.points), 500)
        ]
        for points_chunk in points_chunks:
            # TODO wait for tasks to complete in parallel
            self._run_task(points_chunk)

    def _run_task(self, points):
        task_name = _generate_task_name(
            product=self.product,
            points=self._points_hash(points),
            layers=self.layers,
            years=self.years,
        )
        fn = self.output_dir / self._path(points)
        if fn.exists():
            logger.warning(f"File {fn} already downloaded")
            return
        self._check_token()
        task = _submit_point_task(
            product=self.product,
            version=self.version,
            points=points,
            layers=self.layers,
            years=self.years,
            token=self._token,
            name=task_name,
        )
        logger.warning(f"Waiting for task {task} to complete")
        _poll_task(task, token=self._token)
        logger.warning(f"Task {task} completed")
        files = _list_files(task, token=self._token)

        logger.warning(f"Looking for file {self._path(points)}")
        for file in files:
            if file.name == self._path(points):
                logger.warning(f"Downloading {file.name}")
                file.download(task, self.output_dir, token=self._token)

    def load(self):
        # TODO load all files in output_dir
        points_chunks = [
            self.points[i : i + 500] for i in range(0, len(self.points), 500)
        ]
        dfs = []
        renames = {
            "Date": "datetime",
        }
        for layer in self.layers:
            # Only keep requested layers
            renames[f"{self.product}_{self.version}_{layer}"] = layer
            # MCD12Q2.061 returned 2 columns per layer
            renames[f"{self.product}_{self.version}_{layer}_0"] = f"{layer}_0"
            renames[f"{self.product}_{self.version}_{layer}_1"] = f"{layer}_1"
            # Handle when layer consists out of even more columns

        raw_columns2keep = ["Latitude", "Longitude"] + list(renames.keys())

        for points_chunk in points_chunks:
            file = self.output_dir / self._path(points_chunk)
            if not file.exists():
                raise FileNotFoundError(file)
            df = pd.read_csv(file, parse_dates=["Date"])
            columms2keep = filter(lambda x: x in raw_columns2keep, df.columns)
            df = df[columms2keep]
            df = df.rename(columns=renames)
            dfs.append(df)
        df = pd.concat(dfs)
        gdf = geopandas.gpd.GeoDataFrame(
            df,
            geometry=geopandas.points_from_xy(
                df.pop("Longitude"),
                df.pop("Latitude"),
            ),
        )
        return gdf


def _read_credentials():
    # TODO get config dir from session
    config_dir = CONFIG_DIR
    config_file = config_dir / "appeears.json"
    if not config_file.exists():
        raise FileNotFoundError(f"{config_file} config file not found")
    body = json.load(config_file.open())
    return body["username"], body["password"]


def _login(username, password):
    response = requests.post(
        "https://appeears.earthdatacloud.nasa.gov/api/login",
        auth=(username, password),
    )
    response.raise_for_status()
    return TokenInfo(**response.json())


class ProductInfo(BaseModel):
    Product: str
    Platform: str
    Description: str
    Resolution: str
    Version: str
    ProductAndVersion: str
    DOI: str
    Available: bool
    RasterType: str
    TemporalGranularity: str
    DocLink: str
    Source: str
    TemporalExtentStart: str
    TemporalExtentEnd: str
    Deleted: bool


def products() -> list[ProductInfo]:
    """Get list of products

    Returns:
        list of products

    """
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/product",
    )
    response.raise_for_status()
    return [ProductInfo(**p) for p in response.json()]


class LayerInfo(BaseModel):
    AddOffset: str
    Available: bool
    DataType: str
    Description: str
    Dimensions: list[str]
    FillValue: int
    IsQA: bool
    Layer: str
    OrigDataType: str
    OrigValidMax: int
    OrigValidMin: int
    QualityLayers: str
    QualityProductAndVersion: str
    ScaleFactor: str
    Units: str
    ValidMax: int
    ValidMin: int
    XSize: int
    YSize: int


def layers(product: str) -> dict[str, LayerInfo]:
    """Get layers for a product

    Args:
        product: product name and version.

    Returns:
        list of layers

    Example:

        ```python
        import springtime.datasets.appeears as ma
        products = ma.products()
        product = next(filter(
            lambda p: p.Product == 'MOD15A2H' and p.Version == '061',
            products
        ))
        layers = ma.layers(product.ProductAndVersion)
        {k:v.Description for k,v in layers.items()}
        ```

        Outputs:

        ```python
        {'FparExtra_QC': 'Extra detail Quality for Lai and Fpar',
        'FparLai_QC': 'Quality for Lai and Fpar',
        'FparStdDev_500m': 'Standard deviation of Fpar',
        'Fpar_500m': 'Fraction of photosynthetically active radiation',
        'LaiStdDev_500m': 'Standard deviation of Lai',
        'Lai_500m': 'Leaf area index'}
        ```
    """
    response = requests.get(
        f"https://appeears.earthdatacloud.nasa.gov/api/product/{product}",
    )
    response.raise_for_status()
    body = response.json()
    return {k: LayerInfo(**v) for k, v in body.items()}


def _generate_task_name(
    product: str,
    points: str,
    layers: Sequence[str],
    years: YearRange,
):
    return f"{product}_{years.start}_{years.end}_{'_'.join(sorted(layers))}_{points}"


def _submit_point_task(
    product: str,
    version: str,
    points: Points,
    layers: Sequence[str],
    years: YearRange,
    name: str,
    token: TokenInfo,
):
    if len(points) > 500:
        raise ValueError("Maximum number of points is 500.")
    task = {
        "task_type": "point",
        "task_name": name,
        "params": {
            "dates": [
                {
                    "startDate": "01-01",
                    "endDate": "12-31",
                    "recurring": True,
                    "yearRange": [years.start, years.end],
                }
            ],
            "layers": [
                {"product": f"{product}.{version}", "layer": layer} for layer in layers
            ],
            "coordinates": [
                {
                    "longitude": p[0],
                    "latitude": p[1],
                }
                for p in points
            ],
            "output": {"projection": "geographic", "format": {"type": "netcdf4"}},
        },
    }
    response = requests.post(
        "https://appeears.earthdatacloud.nasa.gov/api/task",
        json=task,
        headers={"Authorization": "Bearer {0}".format(token.token)},
    )
    response.raise_for_status()
    task_response = response.json()
    return task_response["task_id"]


def _submit_area_task(
    product: str,
    version: str,
    area: NamedArea,
    layers: Sequence[str],
    years: YearRange,
    token: TokenInfo,
):
    geometry = json.loads(to_geojson(area.polygon))
    task = {
        "task_type": "area",
        "task_name": area.name,
        "params": {
            "dates": [
                {
                    "startDate": "01-01",
                    "endDate": "12-31",
                    "recurring": True,
                    "yearRange": [years.start, years.end],
                }
            ],
            "layers": [
                {"product": f"{product}.{version}", "layer": layer} for layer in layers
            ],
            "output": {"projection": "geographic", "format": {"type": "netcdf4"}},
            "geo": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": {},
                    }
                ],
            },
        },
    }
    response = requests.post(
        "https://appeears.earthdatacloud.nasa.gov/api/task",
        json=task,
        headers={"Authorization": "Bearer {0}".format(token.token)},
    )
    response.raise_for_status()
    task_response = response.json()
    logger.warning(
        "Submitted task https://appeears.earthdatacloud.nasa.gov/view/%s",
        task_response["task_id"],
    )
    return task_response["task_id"]


def _get_task(task: str, token: TokenInfo):
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/task/{0}".format(task),
        headers={"Authorization": "Bearer {0}".format(token.token)},
    )
    response.raise_for_status()
    task_response = response.json()
    return task_response


def _get_task_status(task: str, token: TokenInfo):
    # TODO get more detailed status
    # from https://appeears.earthdatacloud.nasa.gov/api/#task-status
    return _get_task(task, token)["status"]


# TODO make interval and tries settable when called. From config?
def _poll_task(task: str, token: TokenInfo, interval=30, tries=2 * 60 * 24):
    """Poll task every interval seconds until it completes or timeout is reached.

    Args:
        task: task id
        token: The token.
        interval: Time between getting status in seconds. Defaults to 30.
        tries: Maximum number of tries.
            Defaults to 2*60*24 aka every 30 seconds for 24 hours.

    Raises:
        TimeoutError: When task does not complete within tries*interval seconds.
    """
    for _i in range(tries):
        # TODO check that token is still valid, refresh if not
        status = _get_task_status(task, token)
        logging.warning("Task %s has status %s", task, status)
        if status == "done":
            return
        # TODO also bailout for error status
        sleep(interval)
    raise TimeoutError(
        f"Task {task} did not complete within {tries * interval} seconds"
    )


class _BundleFile(BaseModel):
    id: str = Field(alias="file_id")
    name: str = Field(alias="file_name")
    size: int = Field(alias="file_size")
    type: str = Field(alias="file_type")
    sha256: str

    def download(self, task, output_dir: Path, token: TokenInfo):
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/bundle/{0}/{1}".format(
                task, self.id
            ),
            headers={"Authorization": "Bearer {0}".format(token.token)},
            allow_redirects=True,
            stream=True,
        )
        response.raise_for_status()
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_dir / self.name, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.warning("Downloaded %s to %s", self.name, output_dir)


def _list_files(task: str, token: TokenInfo) -> list[_BundleFile]:
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/bundle/{0}".format(task),
        headers={"Authorization": "Bearer {0}".format(token.token)},
    )
    response.raise_for_status()
    bundle_response = response.json()
    raw_files = bundle_response["files"]
    return [_BundleFile(**f) for f in raw_files]
