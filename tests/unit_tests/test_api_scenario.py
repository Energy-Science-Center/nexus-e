import logging
import os
from pathlib import Path
import pytest
import shutil
from sqlalchemy_utils import database_exists

from nexus_e_interface.scenario import Scenario, DataContext
from nexus_e_interface.tables import Base

LOGGER = logging.getLogger(__name__)

TEST_DATA_FOLDER = Path() / "tests" / "tmp"
"""Everything in this folder is deleted after running the tests"""

@pytest.fixture(scope="module", autouse=True)
def cleanup(request: pytest.FixtureRequest):
    os.makedirs(TEST_DATA_FOLDER, exist_ok=True)
    def remove_test_directory():
        shutil.rmtree(TEST_DATA_FOLDER)
    request.addfinalizer(remove_test_directory)

class TestScenario:

    def test_create_new_sqlite_database(self):
        # Arrange
        database_path = str(TEST_DATA_FOLDER / "test.db")
        test_sqlite_database = DataContext(
            type="sqlite",
            name=database_path
        )
        sut = Scenario(test_sqlite_database)
        # Act
        sut.create_new_database()
        # Assert
        assert database_exists(f"sqlite:///{database_path}")

    def test_database_comply_to_scenario_data_model(self):
        # Arrange
        database_path = str(TEST_DATA_FOLDER / "test.db")
        test_sqlite_database = DataContext(
            type="sqlite",
            name=database_path
        )
        sut = Scenario(test_sqlite_database)
        sut.create_new_database()
        # Act
        try:
            for table in Base.metadata.tables:
                getattr(sut, table)
        # Assert
        except Exception as e:
            raise pytest.fail(f"Table not in database: {e}")
        
    def test_log_warning_when_trying_to_create_existing_database(self, caplog):
        # Arrange
        database_path = str(TEST_DATA_FOLDER / "test.db")
        test_sqlite_database = DataContext(
            type="sqlite",
            name=database_path
        )
        sut = Scenario(test_sqlite_database)
        sut.create_new_database()
        # Act
        with caplog.at_level(logging.WARNING):
            sut.create_new_database()
        # Assert
        assert f"Database {database_path} already exists." in caplog.text