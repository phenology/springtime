"""Common interface for phenology models."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class PhenologyModel:
    predict: Callable
    params_names: tuple[str, ...]
    params_defaults: tuple[float, ...]
    params_bounds: tuple[tuple[float, float], ...]
