import springtime.database as db
import pytest
from sqlmodel import create_engine


@pytest.fixture(autouse=True)
def test_engine(monkeypatch):

    # Setup: create test engine with in-memory database
    mocked_engine = create_engine("sqlite:///")
    monkeypatch.setattr("springtime.database.DATABASE_ENGINE", mocked_engine)
    db.initialize_database()
    ds = db.Dataset(name='foo', path='.')
    ds.add_to_database()

    # Execute test with test_engine
    yield

    # Teardown: clear test engine
    db.clear_database()


def test_list_all(capsys):
    db.Dataset.list_all()
    captured = capsys.readouterr()
    assert "name='foo'" in captured.out


def test_get_by_id():
    ds = db.Dataset.get_by_id(id=1)
    assert ds.name == "foo"


def test_add_to_database():
    ds = db.Dataset(name='bar', path='.')
    ds.add_to_database()
    assert ds.id is not None


def test_update_in_database():
    ds = db.Dataset.get_by_id(id=1)
    ds.name = 'updated'
    ds.update_in_database()
    dsupdated = db.Dataset.get_by_id(id=1)
    assert dsupdated.name == 'updated'


def test_delete_from_database():
    ds = db.Dataset.get_by_id(id=1)
    ds.remove_from_database()
    assert db.Dataset.get_by_id(id=1) is None
