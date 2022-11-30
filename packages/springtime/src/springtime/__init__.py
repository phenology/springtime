from springtime.main import *
from springtime.database import Dataset

from pathlib import Path as _Path


PROJECT_ROOT = _Path(__file__).parent.parent.parent.parent.parent

print(
    "Loading springtime module for project "
    f"folder located at {PROJECT_ROOT}"
)
