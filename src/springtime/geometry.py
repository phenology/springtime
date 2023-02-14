from typing import Literal, Union

from pydantic import BaseModel, confloat
from shapely import Point

Longitude = confloat(ge=-180, le=180)
Latitude = confloat(ge=-90, le=90)
# TODO: validation errors could be nicer
# assert -90 <= y and y <=90, "latitude must be in [-90, 90]"
# assert -180 <= x and x <=180, "longitude must be in [-180, 180]"

class Point(BaseModel):
    x: Longitude
    y: Latitude
    _crs: Literal["epsg:4326"] = "epsg:4326"

    def __init__(self, x, y, **kwargs) -> None:
        """Accept positional arguments."""
        super(Point, self).__init__(x=x, y=y, **kwargs)


class Bounds(BaseModel):
    xmin: Longitude
    ymin: Latitude
    xmax: Longitude
    ymax: Latitude
    _crs: Literal["epsg:4326"] = "epsg:4326"

    def __init__(self, xmin, ymin, xmax, ymax, **kwargs) -> None:
        """Accept positional arguments."""
        super(Bounds, self).__init__(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax, **kwargs)


Location = Dict[str, Union[Point, Bounds]]  # would be nice if it understands that the key is the "name" field
Locations = Iterable[Location]

Point(10, 40)
Bounds(0, 10, 20, 30)
# Point(-100, 100)  # fails but error could be nicer
