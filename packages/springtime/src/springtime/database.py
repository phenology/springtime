"""Keep track of datasets, tasks, flows, experiments.

Data are stored in a local SQLite database file.

Designed after OpenML Python API (https://www.openml.org/apis)
"""
import pandas as pd
from sqlmodel import Field, SQLModel, create_engine, Session, select

from pathlib import Path


# Springtime project setup should have a database in the folder ./data
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
DATABASE_ENGINE = create_engine(
    f"sqlite:///{PROJECT_ROOT}/data/project_data.sqlite"
)


class Client(SQLModel, table=False):
    """Extends SQLModel with methods to interact with database."""
    def add_to_database(self):
        """Add an entry to the database."""
        with Session(DATABASE_ENGINE) as session:
            session.add(self)
            session.commit()
            session.refresh(self)

    def remove_from_database(self):
        """Remove an entry from the database."""
        with Session(DATABASE_ENGINE) as session:
            session.delete(self)
            session.commit()

    def update_in_database(self):
        """Update an entry in the database."""
        self.add_to_database()

    @classmethod
    def list_all(cls):
        with Session(DATABASE_ENGINE) as session:
            query = select(cls)
            entries = session.exec(query).all()

        for entry in entries:
            print(repr(entry), '\n')

    @classmethod
    def get_by_id(cls, id):
        with Session(DATABASE_ENGINE) as session:
            entry = session.get(cls, id)

        return entry


class Dataset(Client, table=True):
    """Dataset metadata object modelled after OpenMLDataset."""
    id: int | None = Field(default=None, primary_key=True)
    name: str
    path: str
    description: str | None = None
    licence: str | None = None
    provenance: str | None = None
    reference: str | None = None

    def load_data(self):
        """Read data from disk."""
        return pd.read_csv(self.path)


# More ORM classes can be added here, e.g. RegressionTask


def initialize_database():
    """First time setup of database."""
    print(f"Creating database in {DATABASE_ENGINE.url}")
    SQLModel.metadata.create_all(DATABASE_ENGINE)


def clear_database():
    """Clear the database file of all its content."""
    SQLModel.metadata.drop_all(DATABASE_ENGINE)
