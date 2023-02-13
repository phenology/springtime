from pydantic import BaseModel, validator

from shapely import Point, Polygon
from typing import Tuple, Union


# # class Point(BaseModel):
# #     x: float
# #     y: float

# class Bbox(BaseModel):
#     xmin: float
#     ymin: float
#     xmax: float
#     ymax: float

class Geometry(BaseModel):
    p: Union[Tuple[float, float], Tuple[float, float, float, float]]

    @validator("p")
    def parse_point(value):
        """Parse tuple of floats as x, y pair"""
        if len(value) == 2:
            return Point(*value)

        return Polygon.from_bounds(*value)


point = Point(10, 40)
print(repr(point))

geom = Geometry(p=(0, 40))
print(repr(geom))

geom = Geometry(p=(200, 10, 20, 40))
print(repr(geom))
print(geom.is_valid)
