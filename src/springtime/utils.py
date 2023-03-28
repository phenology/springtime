

from typing import Tuple
from pydantic import BaseModel, validator
from shapely.geometry import Polygon


class NamedArea(BaseModel):
    # TODO generalize
    # perhaps use https://github.com/developmentseed/geojson-pydantic
    name: str
    bbox: Tuple[float, float, float, float]

    @validator("bbox")
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


