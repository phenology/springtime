from collections.abc import Callable
from dataclasses import dataclass

from thermaltime import thermaltime


@dataclass
class PhenologyModel():
    predict: Callable
    params_names: tuple[str, ...]
    params_defaults: tuple[float, ...]
    params_bounds: tuple[tuple[float, float], ...]


thermaltime = PhenologyModel(
    predict = thermaltime,
    params_names = ('t1', 'T', 'F'),
    params_defaults = (1, 5, 500),
    params_bounds = ((-67, 298), (-25, 25), (0, 1000)),
)


CORE_MODELS = {
    'thermaltime': thermaltime
}
