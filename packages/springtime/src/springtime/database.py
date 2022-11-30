"""Keep track of datasets, tasks, flows, experiments.

Data are stored in a local SQLite database file.

Designed after OpenML Python API (https://www.openml.org/apis)
"""
import pandas as pd
from sqlmodel import Field, SQLModel, create_engine, Session


sqlite_file_name = "test.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"
sqlite_url = f"sqlite:///"  # for now an in-memory database (development)

engine = create_engine(sqlite_url)


class Dataset(SQLModel, table=True):
    """Dataset metadata object modelled after OpenMLDataset."""
    id: int | None = Field(default=None, primary_key=True)
    name: str
    path: str
    description: str | None = None
    licence: str | None = None
    provenance: str | None = None
    reference: str | None = None

    def get_data(self):
        """Read data from disk."""
        return pd.read_csv(self.path)

    def register(self):
        with Session(engine) as session:
            session.add(self)
            session.commit()
            session.refresh(self)


class RegressionTask(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    description: str | None = None
    dataset_id: int = Field(foreign_key="dataset.id")


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def create_example_dataset_entry():
    dummy_dataset = Dataset(name="Example", path="./path/to/example.csv", )
    with Session(engine) as session:
        session.add(dummy_dataset)
        session.commit()
        session.refresh(dummy_dataset)

    dummy_task = RegressionTask(name="lilac", dataset_id=1)
    with Session(engine) as session:
        session.add(dummy_task)
        session.commit()
        session.refresh(dummy_task)


def main():
    create_db_and_tables()
    create_example_dataset_entry()


# TODO: eventually we want to do the database creation only once.
SQLModel.metadata.create_all(engine)


if __name__ == "__main__":
    main()
