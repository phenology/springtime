from pathlib import Path

from pydantic import BaseModel, field_validator
from xdg_base_dirs import xdg_cache_home, xdg_config_home

CONFIG_DIR: Path = xdg_config_home() / "springtime"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)


class Config(BaseModel, validate_assignment=True, validate_default=True):
    cache_dir: Path = xdg_cache_home() / "springtime"
    output_root_dir: Path = Path(".")
    pep725_credentials_file: Path = CONFIG_DIR / "pep725_credentials.txt"
    force_override: bool = False

    @field_validator("cache_dir")
    def _make_dir(cls, path):
        """Create dirs if they don't exist yet."""
        if not path.exists():
            print(f"Creating folder {path}")
            path.mkdir(parents=True)
        return path


CONFIG = Config()
