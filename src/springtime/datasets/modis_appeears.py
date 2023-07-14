from datetime import datetime
from hashlib import sha1
import json
from pathlib import Path
from time import sleep
from typing import Literal, Sequence, Tuple, Union

import requests
import geopandas
import pandas as pd
import xarray as xr
from pydantic import BaseModel, Field, conset
import logging
from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import (
    BoundingBox,
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


class ModisAppeears(Dataset):
    dataset: Literal["modis_appeears"] = "modis_appeears"
    """Points as longitude, latitude in WGS84 projection."""
    product: str
    """a MODIS product. Use `modis_products()` to get list of available products."""
    bands: conset(str, min_items=1)  # type: ignore
    """MODIS product bands.

    Use `modis_bands(product)` to get list of available bands for a product.
    """
    _token: TokenInfo = None

    def _check_token(self):
        if self._token is None:
            username, password = read_auth()
            self._token = self._login(username, password)
        if self._token.expiration < datetime.now():
            self._token = self._login(username, password)

    @property
    def output_dir(self):
        d = CONFIG.data_dir / "modis_appeears"
        d.mkdir(exist_ok=True, parents=True)
        return d


class ModisAppeearsArea(ModisAppeears):
    dataset: Literal["modis_appeears_area"] = "modis_appeears_area"
    area: NamedArea

    @property
    def _path(self):
        return f"{self.area.name}-results.nc"

    def download(self):
        self._check_token()
        if (self.output_dir / self._path).exists():
            return
        task = submit_area_task(
            product=self.product,
            area=self.area,
            layers=self.bands,
            years=self.years,
            token=self._token,
        )
        poll_task(task, token=self._token)
        logger.warning(f"Task {task} completed")
        files = list_files(task, token=self._token)
        for file in files:
            if self.area.name == self._path:
                file.download(task, self.output_dir, token=self._token)

    def load(self):
        return xr.open_dataset(self.output_dir / self._path)

class ModisAppeearsPointsFromArea(ModisAppeears):
    dataset: Literal["modis_appeears_points_from_area"] = "modis_appeears_points_from_area"
    points: Union[Sequence[Tuple[float, float]], PointsFromOther]

    @property
    def _points_hash(self):
        return sha1(json.dumps(self.area.dict()).encode("utf-8")).hexdigest()

    @property
    def _area(self):
        return NamedArea(
            name=self._points_hash,
            bbox=BoundingBox.from_points(self.points),
        )

    @property
    def _path(self):
        return f'{self._area.name}*-results.nc'

    @property
    def source(self):
        return ModisAppeearsArea(
            product=self.product,
            area=self._area,
            bands=self.bands,
            years=self.years,
        )

    def download(self):
        self.source.download()

    def load(self):
        ds = self.source.load()
        return points_from_cube(ds, self.points)

class ModisAppeearsPoints(ModisAppeears):
    dataset: Literal["modis_appeears_points"] = "modis_appeears_points"
    points: Union[Sequence[Tuple[float, float]], PointsFromOther]

    def _points_hash(self, points: Points):
        return sha1(json.dumps(points).encode("utf-8")).hexdigest()

    def _task_name(self, points: Points):
        return generate_task_name(
            product=self.product,
            points=self._points_hash(points),
            layers=self.bands,
            years=self.years,
        )

    def _path(self, points: Points):
        return f"{self._task_name(points)}-results.csv"

    def download(self):
        self._check_token()

        # api can not handle more than 500 points (of 1 product, 2 layers)
        # at once so we split the points into chunks
        points_chunks = [
            self.points[i : i + 500] for i in range(0, len(self.points), 500)
        ]
        for points_chunk in points_chunks:
            # TODO wait for tasks to complete in parallel
            self._run_task(points_chunk)

    def _run_task(self, points):
        task_name = generate_task_name(
            product=self.product,
            points=self._points_hash(points),
            layers=self.bands,
            years=self.years,
        )
        if (self.output_dir / self._path(points)).exists():
            return
        task = submit_point_task(
            product=self.product,
            points=points,
            layers=self.bands,
            years=self.years,
            token=self._token,
            name=task_name,
        )
        logger.warning(f"Waiting for task {task} to complete")
        poll_task(task, token=self._token)
        logger.warning(f"Task {task} completed")
        files = list_files(task, token=self._token)
        logger.warning(f"Downloading {files} files")

        for file in files:
            if task_name in file.name and file.name.endswith("-results.csv"):
                file.download(task, self.output_dir, token=self._token)

    def load(self):
        # TODO load all files in output_dir
        points_chunks = [
            self.points[i : i + 500] for i in range(0, len(self.points), 500)
        ]
        dfs = []
        renames = {
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
        for points_chunk in points_chunks:
            file = self.output_dir / self._path(points_chunk)
            if file is None:
                raise FileNotFoundError(file)
            df = pd.read_csv(file)
            df = df[df.renames.keys()]
            df = df.rename(columns=renames)
            dfs.append(df)
        df = pd.concat(dfs)
        return geopandas.gpd.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
        )


def read_auth():
    # TODO get config dir from session
    config_dir = Path("~/.config/springtime").expanduser()
    config_file = config_dir / "appeears.json"
    if not config_file.exists():
        raise FileNotFoundError(f"{config_file} config file not found")
    body = json.load(config_file.open())
    return body["username"], body["password"]


def login(username, password):
    response = requests.post(
        "https://appeears.earthdatacloud.nasa.gov/api/login",
        auth=(username, password),
    )
    response.raise_for_status()
    return TokenInfo(response.json())


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
    Resolution: str
    TemporalGranularity: str
    DocLink: str
    Source: str
    TemporalExtentStart: str
    TemporalExtentEnd: str
    Deleted: bool


def products():
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/product",
    )
    response.raise_for_status()
    return [ProductInfo(p) for p in response.json()]


def layers(product):
    response = requests.get(
        f"https://appeears.earthdatacloud.nasa.gov/api/product/{product}",
    )
    response.raise_for_status()
    return response.json()


def generate_task_name(
    product: str,
    points: str,
    layers: Sequence[str],
    years: YearRange,
    type: str = "point",
):
    return f"{type}_{product}_{years.start}_{years.end}_{layers}_{points}"
      

def submit_point_task(
    product: str,
    points: Union[Sequence[Tuple[float, float]], PointsFromOther],
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
            "dates": {
                "startDate": "01-01",
                "endDate": "12-31",
                "recurring": True,
                "yearRange": [years.start, years.end],
            },
            "layers": [{"product": product, "layer": l} for l in layers],
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
        headers={"Authorization": "Bearer {0}".format(token)},
    )
    response.raise_for_status()
    task_response = response.json()
    return task_response["task_id"]


def submit_area_task(
    product: str,
    area: NamedArea,
    layers: Sequence[str],
    years: YearRange,
    token: TokenInfo,
):
    task = {
        "task_type": "area",
        "task_name": area.name,
        "params": {
            "dates": {
                "startDate": "01-01",
                "endDate": "12-31",
                "recurring": True,
                "yearRange": [years.start, years.end],
            },
            "layers": [{"product": product, "layer": l} for l in layers],
            "output": {"projection": "geographic", "format": {"type": "netcdf4"}},
            "geo": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Polygon", "coordinates": area.polygon},
                    }
                ],
            },
        },
    }
    response = requests.post(
        "https://appeears.earthdatacloud.nasa.gov/api/task",
        json=task,
        headers={"Authorization": "Bearer {0}".format(token)},
    )
    response.raise_for_status()
    task_response = response.json()
    return task_response["task_id"]


def get_task(task: str, token: TokenInfo):
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/task/{0}".format(task_id),
        headers={"Authorization": "Bearer {0}".format(token.token)},
    )
    response.raise_for_status()
    task_response = response.json()
    return task_response


def get_task_status(task: str, token: TokenInfo):
    get_task(task, token)["status"]


def poll_task(task: str, token: TokenInfo, interval=30, timeout=2 * 60 * 24):
    for i in range(timeout):
        status = get_task_status(task, token.token)
        if status == "done":  # TODO: check for error status
            return
        sleep(interval)
    raise TimeoutError(
        f"Task {task} did not complete within {timeout * interval} seconds"
    )


class BundleFile(BaseModel):
    id: str = Field(alias="file_id")
    name: str = Field(alias="file_name")
    size: int = Field(alias="file_size")
    type: str = Field(alias="file_type")
    sha256: str

    def download(self, task, output_dir: Path, token: TokenInfo):
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/bundle/{0}/file/{1}".format(
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
        logger.warning("Downloaded %s to %s", self.name, self.output_dir)


def list_files(task: str, token: TokenInfo):
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/bundle/{0}".format(task),
        headers={"Authorization": "Bearer {0}".format(token.token)},
    )
    response.raise_for_status()
    bundle_response = response.json()
    raw_files = bundle_response["files"]
    return [BundleFile(**f) for f in raw_files]
