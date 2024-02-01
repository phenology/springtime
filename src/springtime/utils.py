import signal
import subprocess
import time
from functools import wraps
from logging import getLogger
from typing import NamedTuple, Sequence

import geopandas as gpd
import xarray as xr
from pydantic import (
    BaseModel,
    PositiveInt,
    PrivateAttr,
    field_validator,
    model_validator,
)
from shapely.geometry import Polygon

logger = getLogger(__name__)

# TODO move the types to types.py


germany = {
    "name": "Germany",
    "bbox": [
        5.98865807458,
        47.3024876979,
        15.0169958839,
        54.983104153,
    ],
}


class Point(NamedTuple):  # TODO use shapely point instead?
    """Single point with x and y coordinate."""

    x: float
    y: float


class PointsFromOther(BaseModel):
    """Points from another dataset.

    Attributes:
        source: Name of dataset to get points from.
    """

    source: str
    _xy: Sequence[Point] = PrivateAttr(default=[])
    _points: gpd.GeoSeries | None = PrivateAttr(default=None)
    _records: gpd.GeoSeries | None = PrivateAttr(default=None)

    def get_points(self, other):
        # TODO: refactor to generic utility function
        self._xy = list(map(lambda p: Point(p.x, p.y), other.geometry.unique()))
        self._points = other.geometry.unique()
        self._records = other[["year", "geometry"]]

    def __iter__(self):
        for item in self._xy:
            yield item

    def __len__(self):
        return len(self._xy)


Points = Sequence[Point] | PointsFromOther
"""Points can be a list of (lon, lat) tuples or a PointsFromOther object."""


class BoundingBox(NamedTuple):
    xmin: float
    ymin: float
    xmax: float
    ymax: float

    @classmethod
    def from_points(cls, points: Points):
        return cls(
            xmin=min(map(lambda p: p.x, points)),
            ymin=min(map(lambda p: p.y, points)),
            xmax=max(map(lambda p: p.x, points)),
            ymax=max(map(lambda p: p.y, points)),
        )


class NamedArea(BaseModel):
    """Named area with bounding box."""

    # TODO generalize
    # perhaps use https://github.com/developmentseed/geojson-pydantic
    name: str
    bbox: BoundingBox

    @field_validator("bbox")
    def _parse_bbox(cls, values):
        xmin, ymin, xmax, ymax = values
        assert xmax > xmin, "xmax should be larger than xmin"
        assert ymax > ymin, "ymax should be larger than ymin"
        assert ymax < 90 and ymin > 0, "Latitudes should be in [0, 90]"
        assert xmin > -180 and xmax < 180, "Longitudes should be in [-180, 180]."
        return values

    @property
    def polygon(self):
        return Polygon.from_bounds(*self.bbox)


class NamedIdentifiers(BaseModel):
    """List of identifiers with a name."""

    name: str
    items: Sequence[int]


# date range of years
class YearRange(NamedTuple):
    """Date range in years.

    Example:

        >>> YearRange(2000, 2005)
        YearRange(start=2000, end=2005)
        >>> YearRange(start=2000, end=2005).range
        range(2000, 2006)
        >>> YearRange(2000, 2000)
        YearRange(start=2000, end=2000)

    """

    start: PositiveInt
    end: PositiveInt
    """The end year is inclusive."""

    @model_validator(mode="after")
    def _must_increase(self):
        assert (
            self.start <= self.end
        ), f"start year ({self.start}) should be smaller than end year ({self.end})"
        return self

    @property
    def range(self) -> range:
        """Return the range of years."""
        # +1 as range() is exclusive while YearRange is inclusive
        return range(self.start, self.end + 1)


# Decorators copied from https://wiki.python.org/moin/PythonDecoratorLibrary


def retry(timeout: int = 10, max_tries: int = 3, delay: int = 1, backoff: int = 2):
    """Decorator to retry function with timeout.

    The decorator will call the function up to max_tries times if it raises
    an exception.

    Note:
        Decorating function which calls rpy2 does not work. Please use
        :func:run_r_script method instead.

    Args:
        timeout: Maximum mumber of seconds the function may take.
        max_tries: Maximum number of times to execute the function.
        delay: Sleep this many seconds * backoff * try number after failure
        backoff: Multiply delay by this factor after each failure

    Raises:
        TimeoutError: When max tries is reached and last try timedout.

    """

    def dec(function):
        def _handle_timeout(signum, frame):
            raise TimeoutError("Function call timed out")

        @wraps(function)
        def f2(*args, **kwargs):
            mydelay = delay
            tries = list(range(max_tries))
            tries.reverse()
            for tries_remaining in tries:
                oldsignal = signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(timeout)
                try:
                    return function(*args, **kwargs)
                except TimeoutError:
                    if tries_remaining > 0:
                        logger.warn(
                            f"Function call took more than {timeout} seconds, retrying"
                        )
                        time.sleep(mydelay)
                        mydelay = mydelay * backoff
                    else:
                        raise
                else:
                    break
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, oldsignal)

        return f2

    return dec


class TimeoutError(Exception):
    """Timeout Exception.

    When a function takes too long to run when using the :func:retry decorator.

    """

    pass


def run_r_script(script: str, timeout: int = 30, max_tries: int = 3):
    """Run R script with retries and timeout logic.

    Args:
        script: The R script to run
        timeout: Maximum mumber of seconds the function may take.
        max_tries: Maximum number of times to execute the function.
    """
    logger.debug(f"Executing R code:\n{script}")

    result = retry(timeout=timeout, max_tries=max_tries)(subprocess.run)(
        ["R", "--vanilla", "--no-echo"],
        input=script.encode(),
        stderr=subprocess.PIPE,
    )
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        logger.error(result.stderr)
        raise


def transponse_df(df, index=("year", "geometry"), columns=("doy",)):
    """Ensure features are in columns not in rows

    ML records are characterized by a unique combination of location and year.
    Predictor variables like (daily/monthly) temperature may have multiple
    values for the same location and year.

    This function reorganizes the data such that multiple predictor values for
    the same records occur in separate columns.

    For example:

    ```
           year                 geometry  doy   temperature
        0  2000  POINT (1.00000 1.00000)    1             14
        1  2000  POINT (1.00000 1.00000)    2             15
    ```

    becomes

    ```
           year                 geometry  temperature_1  temperature_2
        0  2000  POINT (1.00000 1.00000)             14             15
    ```

    Args:
        df: The raw data in "long form"
        index: Columns to use as unique record identifiers.
        columns: Columns that contain the index for the repeated predictors.

    Returns:
        "Wide form" data frame with year and geometry column and
        columns named `<original column name>_<doy>`.
    """
    pdf = df.pivot(index=index, columns=columns).reset_index()
    pdf.columns = [
        "_".join(map(str, filter(lambda x: x != "", i))) for i in pdf.columns.values
    ]
    return gpd.GeoDataFrame(pdf)


def rolling_mean(
    df,
    over,
    groupby=("year", "geometry"),
    window_sizes=(3, 7, 15, 30, 90, 365),
):
    """Group by `groupby` columns and calculate rolling mean
    for `over` columns with different window sizes.
    """
    # TODO implement
    raise NotImplementedError()


class ResampleConfig(BaseModel):
    frequency: str = "month"
    """See
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.html
    for allowed values."""
    operator: str = "mean"
    """See
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.agg.html
    and
    https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html#built-in-aggregation-methods
    for allowed values."""


def resample(df, freq="month", operator="mean", column="datetime"):
    """Resample data on year, geometry, and given frequency.

    Options for freq (properties of df.time.dt):
    - 'month'
    - 'week'
    - 'day'
    - 'dayofyear'
    - ...
    """
    groups = [
        "geometry",
        getattr(df[column].dt, "year").rename("year"),
        getattr(df[column].dt, freq).rename(freq),
    ]

    # Can't sort when grouping on geometry
    new_df = (
        df.groupby(groups, sort=False).agg(operator, numeric_only=True).reset_index()
    )

    # TODO: could this make the frequency more flexible?
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    # df = df.set_index(["geometry", "datetime"]).groupby(
    #     [pd.Grouper(level='geometry'), pd.Grouper(level='datetime', freq=frequency)]
    #     ).agg(operator)

    return gpd.GeoDataFrame(new_df)


def points_from_cube(
    ds: xr.Dataset,
    points: Points,
    xdim: str = "lon",
    ydim: str = "lat",
) -> gpd.GeoDataFrame:
    """From a cube, extract the values at the given points.

    Args:
        ds: Xarray datset with latitude and longitude dimensions
        points: List of points as (lon, lat) tuples

    Returns:
        Dataframe with columns for each point and each variable in the dataset.
        The points are in the geometry column.
    """
    lons = xr.DataArray([p[0] for p in points], dims="points_index")
    lats = xr.DataArray([p[1] for p in points], dims="points_index")
    points_df = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lons, lats)).reset_index(
        names="points_index"
    )
    df = (
        ds.sel(**{xdim: lons, ydim: lats, "method": "nearest"})  # type: ignore
        .to_dataframe()
        .reset_index()
    )
    df = df.merge(points_df, on="points_index", how="right").drop(
        ["points_index", xdim, ydim], axis=1
    )
    return gpd.GeoDataFrame(df, geometry=df.geometry)


# TODO merge with points_from_cube from above?
def get_points_from_raster(points: Point | Points, ds: xr.Dataset) -> xr.Dataset:
    """Extract points from area."""
    if isinstance(points, Point):
        x = [points.x]
        y = [points.y]
        geometry = gpd.GeoSeries(gpd.points_from_xy(x, y), name="geometry")
        ds = extract_points(ds, geometry)
    elif isinstance(points, Sequence):
        x = [p.x for p in points]  # type: ignore  # Mypy struggles in CI
        y = [p.y for p in points]  # type: ignore  # Mypy struggles in CI
        geometry = gpd.GeoSeries(gpd.points_from_xy(x, y), name="geometry")
        ds = extract_points(ds, geometry)
    else:
        # only remaining option is PointsFromOther
        records = points._records
        ds = extract_records(ds, records)
    return ds


def extract_points(ds, points: gpd.GeoSeries, method="nearest"):
    """Extract list of points from gridded dataset."""
    x = xr.DataArray(points.unique().x, dims=["geometry"])
    y = xr.DataArray(points.unique().y, dims=["geometry"])
    geometry = xr.DataArray(points.unique(), dims=["geometry"])
    return (
        ds.sel(longitude=x, latitude=y, method=method)
        .drop_vars(["latitude", "longitude"])
        .assign_coords(geometry=geometry)
    )


def extract_records(ds, records: gpd.GeoDataFrame):
    """Extract list of year/geometry records from gridded dataset."""
    x = records.geometry.x.to_xarray()
    y = records.geometry.y.to_xarray()
    year = records.year.to_xarray()
    geometry = records.geometry.to_xarray()
    # TODO ensure all years present before allowing 'nearest' on year
    # TODO also work when there is no year column (static variables)?
    return (
        ds.sel(longitude=x, latitude=y, year=year, method="nearest")
        .drop(["latitude", "longitude"])
        .assign_coords(year=year, geometry=geometry)
    )


def join_dataframes(dfs, index_cols=["year", "geometry"]):
    """Join dataframes by index cols.

    Assumes incoming data is a geopandas dataframe with a geometry column. Not
    as index.
    """
    others = []
    for df in dfs:
        df = gpd.GeoDataFrame(df)  # TODO should not be necessary
        df = df.to_wkt()
        df.set_index(index_cols, inplace=True)
        others.append(df)

    main_df = others.pop(0)

    df = main_df.join(others, how="outer")
    df.reset_index(inplace=True)
    geometry = gpd.GeoSeries.from_wkt(df.pop("geometry"))

    return gpd.GeoDataFrame(df, geometry=geometry).set_index(index_cols)


def split_time(ds, freq="daily"):
    """Split datetime coordinate into year and dayofyear or month."""
    year = ds.time.dt.year.data

    if freq in ["daily", "day", "D"]:
        freqdim = ds.time.dt.dayofyear.data
    elif freq in ["monthly", "month", "M"]:
        freqdim = ds.time.dt.month.data
    else:
        raise ValueError("Unknown frequency. Choose daily or monthly.")

    return (
        ds.assign_coords(year=("time", year), timeinyear=("time", freqdim))
        .set_index(time=("year", "timeinyear"))
        .unstack("time")
    )
