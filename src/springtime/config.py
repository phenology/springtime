# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from xdg_base_dirs import xdg_cache_home, xdg_config_home
from pydantic import BaseModel, validator

CONFIG_DIR: Path = xdg_config_home() / "springtime"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)


class Config(BaseModel):
    cache_dir: Path = xdg_cache_home() / "springtime"
    output_root_dir: Path = Path(".")
    pep725_credentials_file: Path = CONFIG_DIR / "pep725_credentials.txt"
    force_override: bool = False

    @validator("cache_dir")
    def _make_dir(cls, path):
        """Create dirs if they don't exist yet."""
        if not path.exists():
            print(f"Creating folder {path}")
            path.mkdir(parents=True)
        return path

    class Config:
        validate_all = True


CONFIG = Config()
