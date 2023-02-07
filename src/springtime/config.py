from pathlib import Path
from tempfile import gettempdir

from pydantic import BaseModel, validator


class Config(BaseModel):
    data_dir: Path = Path(gettempdir()) / "data"
    force_override: bool = False

    @validator("data_dir")
    def _make_dir(cls, path):
        """Create dirs if they don't exist yet."""
        if not path.exists():
            print(f"Creating folder {path}")
            path.mkdir(parents=True)
        return path

    class Config:
        validate_all = True


CONFIG = Config()
