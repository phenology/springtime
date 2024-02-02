"""
This module contains functionality to download and load MODIS land products
subsets from AppEEARS

See <https://appeears.earthdatacloud.nasa.gov/>

Credentials are read from `~/.config/springtime/appeears.json`.
JSON file should look like `{"username": "foo", "password": "bar"}`.
"""
from functools import partial
import json
import logging
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from time import sleep
from typing import Literal, Optional, Sequence
from pydantic import model_validator
import geopandas as gpd
import pandas as pd
import requests
import xarray as xr
from pydantic import BaseModel, Field
from shapely import to_geojson

from springtime.config import CONFIG, CONFIG_DIR
from springtime.datasets.abstract import Dataset
from springtime.utils import (
    NamedArea,
    Points,
    ResampleConfig,
    YearRange,
    points_from_cube,
    resample,
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
    """Download and load MODIS data using AppEEARS.

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.
        product: An AppEEARSp product name. Use `products()` to get a list of
            currently available in AppEEARS.
        version: An AppEEARS product version.
        layers: Layers of a AppEEARS product. Use `layers(product)` to get list
            of available layers for a product.
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.
        points: List of points as [[longitude, latitude], ...], in WGS84
            projection.
        infer_date_offset: for yearly variables given as day of year, transform
            value to day of year.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling. Else, should be a dictonary of the form {frequency:
            'M', operator: 'mean', **other_options}. Currently supported
            operators are 'mean', 'min', 'max', 'sum', 'median'. For valid
            frequencies see [1]. Other options will be passed directly to
            xr.resample [2]

    """

    dataset: Literal["appears"] = "appears"
    product: str
    version: str  # TODO make optional, if not given, use latest version
    layers: list[str]
    # TODO rename to variables or bands?
    area: NamedArea | None = None
    points: Points | None = None
    _token: Optional[TokenInfo] = None
    infer_date_offset: bool = True
    resample: Optional[ResampleConfig] = None

    @model_validator(mode="after")
    def check_points_or_area(self):
        if not self.points and not self.area:
            raise ValueError("Either points or area (or both) is required")

        return self

    def download(self):
        """Download data if necessary and return file paths."""
        if self.area:
            return self.download_area()
        return self.download_points()

    def raw_load(self):
        """Load dataset into memory, including pre-processing."""
        if self.area:
            return self.raw_load_area()
        return self.raw_load_points()

    def load(self):
        """Load dataset into memory, including pre-processing."""
        if self.points and self.area:
            return self.load_points_from_area()
        if self.points:
            return self.load_points()
        return self.raw_load_area()

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
            token_fn.write_text(self._token.model_dump_json())

    @property
    def _root_dir(self):
        """Output directory for downloaded data."""
        d = CONFIG.cache_dir / "appeears"
        d.mkdir(exist_ok=True, parents=True)
        return d

    @property
    def _area_path(self):
        # MCD15A2H.061_500m_aid0001.nc
        # aid = area id? Assumes one area per request.
        product = next(filter(lambda p: p.Product == self.product, products()))
        resolution = product.Resolution

        return f"{self.product}.{self.version}_{resolution}_aid0001.nc"

    @property
    def _area_dir(self):
        assert self.area, "Area dir requires area input"

        d = self._root_dir / self.area.name
        d.mkdir(exist_ok=True, parents=True)
        return d

    def download_area(self):
        """Download the data."""
        assert self.area, "Download area requires area input"
        logger.info("Looking for data...")

        fn = self._area_dir / self._area_path
        if (fn).exists() and not CONFIG.force_override:
            logger.info(f"Found {fn}")
        else:
            logger.info(f"Downloading {fn}")
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
            logger.info(f"Task {task} completed")
            files = _list_files(task, token=self._token)

            for file in files:
                if file.name == self._area_path:
                    file.download(task, self._area_dir, token=self._token)

        return fn

    def raw_load_area(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        assert self.area, "Load area requires area input"

        path = self.download_area()
        ds = xr.open_dataset(path)
        return ds

    def load_points_from_area(self):
        """Load the dataset from disk into memory.

        First the bounding box of area is downloaded and then points are
        selected from the area. This could be quicker then directly downloading
        points, if the amount of points is large and in a small area.
        """
        assert self.points, "Load points from area requires points input"
        assert self.area, "Load points from area requires area input"
        ds = self.raw_load_area()

        # Convert cftime.DatetimeJulian to datetime.datetime
        datetimeindex = ds.indexes["time"].to_datetimeindex()
        ds["time"] = datetimeindex

        # Drop second growing cycle if present
        # TODO: convert to two variables instead? Like DF?
        if "Num_Modes" in ds.coords:
            ds = ds.sel(Num_Modes=0, drop=True)

        # Coords are in geometric projection no need for crs variable
        ds = ds.drop_vars(["crs"])
        df = points_from_cube(ds, self.points)

        # Drop QC & QA variables
        df = df.filter(regex="^(?!.*_QC$)")  # exclude ending with _QC
        df = df.filter(regex="^(?!QA_).*$")  # exclude starting with QA_

        if self.infer_date_offset:
            # For yearly data: express value as dayofyear and extract year
            for column in df.columns:
                if column not in ["time", "geometry"]:
                    value_as_timedelta = df[column] - df["time"]
                    df[column] = value_as_timedelta.dt.days

            # Convert datetime to 'year'
            df["year"] = df["time"].dt.year
            df = df.drop(columns="time")

        else:
            if self.resample:
                df = resample(
                    df, freq=self.resample.frequency, operator=self.resample.operator
                )

            # (Sub)daily data: Split datetime in DOY and year, and pivot
            df["year"] = df["time"].dt.year
            df["DOY"] = df["time"].dt.dayofyear
            df = df.drop(columns="time")

            # Pivot
            df = df.set_index(["year", "geometry", "DOY"]).unstack("DOY")
            df.columns = df.columns.map("{0[0]}|{0[1]}".format)
            df = df.reset_index()

        return gpd.GeoDataFrame(df)

    def _points_hash(self, points: Points):
        """Encode coordinates to a more manageable string."""
        return sha1(json.dumps(points).encode("utf-8")).hexdigest()

    def _point_task_name(self, points: Points):
        """Generate unique name for point download task"""
        assert self.years, "self.years should be defined"  # type narrowing

        return _generate_task_name(
            product=self.product,
            points=self._points_hash(points),
            layers=self.layers,
            years=self.years,
        )

    def _point_path(self, points: Points):
        chunk = f"{self._point_task_name(points)}-{self.product}-{self.version}"
        return f"{chunk}-results.csv".replace("_", "-")

    def _run_point_task(self, points):
        assert self.years, "years should be defined for download"  # type narrowing
        task_name = _generate_task_name(
            product=self.product,
            points=self._points_hash(points),
            layers=self.layers,
            years=self.years,
        )
        fn = self._root_dir / self._point_path(points)
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

        logger.warning(f"Looking for file {self._point_path(points)}")
        for file in files:
            if file.name == self._point_path(points):
                logger.warning(f"Downloading {file.name}")
                file.download(task, self._root_dir, token=self._token)

    def download_points(self):
        """Download point files if necessary and return paths."""

        # api can not handle more than 500 points (of 1 product, 2 layers)
        # at once so we split the points into chunks
        # TODO load all files in output_dir
        points_chunks = [
            self.points[i : i + 500] for i in range(0, len(self.points), 500)
        ]

        files = []
        for points_chunk in points_chunks:
            file = self._root_dir / self._point_path(points_chunk)
            if file.exists():
                logger.info(f"Found {file}")
            else:
                # TODO wait for tasks to complete in parallel
                logger.info(f"Downloading points to {file}")
                self._run_point_task(points_chunk)

            files.append(file)

        return files

    def raw_load_points(self):
        files = self.download_points()
        dfs = []
        for file in files:
            df = pd.read_csv(file, parse_dates=["Date"])
            dfs.append(df)

        df = pd.concat(dfs)

        return df

    def load_points(self):
        df = self.raw_load_points()

        # Filter and rename columns
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
        columms2keep = filter(lambda x: x in raw_columns2keep, df.columns)
        df = df[columms2keep]
        df = df.rename(columns=renames)

        # Mask fill values
        for layer in self.layers:
            layer_props = layers("MCD12Q2.061")
            fill_value = layer_props["Greenup"].FillValue
            for column in df.columns:
                if layer in column:
                    df[column] = df[column].mask(df[column] == fill_value)

        # Drop columns with are completely filled with NaN
        df = df.dropna(axis=1, how="all")

        # Convert to geodataframe
        lat = df.pop("Latitude")
        lon = df.pop("Longitude")
        df["geometry"] = gpd.points_from_xy(lon, lat)

        # Deduct date offset
        if self.infer_date_offset:
            for column in df.columns:
                if column not in ["datetime", "geometry"]:
                    value_as_datetime = df[column].map(partial(pd.Timestamp, unit="D"))
                    value_as_timedelta = value_as_datetime - df["datetime"]
                    df[column] = value_as_timedelta.dt.days
            df["year"] = df["datetime"].dt.year
            df = df.drop(columns="datetime")

        else:
            if self.resample:
                df = resample(
                    df, freq=self.resample.frequency, operator=self.resample.operator
                )

            # Daily data: Split datetime in DOY and year, and pivot
            df["year"] = df["datetime"].dt.year
            df["DOY"] = df["datetime"].dt.dayofyear
            df = df.drop(columns="datetime")

            # Pivot
            df = df.set_index(["year", "geometry", "DOY"]).unstack("DOY")
            df.columns = df.columns.map("{0[0]}|{0[1]}".format)
            df = df.reset_index()

        return gpd.GeoDataFrame(df)


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
    """Product information"""

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
    """Layer information"""

    AddOffset: str | float  # different for different products
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
    ScaleFactor: float | str  # different for different products
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
