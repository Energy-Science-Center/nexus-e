import os
from pathlib import Path
import shutil
import pytest
import tomli
import src.nexus_e.config as config

TEST_DATA_FOLDER = Path() / "tests" / "tmp"
"""Everything in this folder is deleted after running the tests"""

@pytest.fixture(scope="function", autouse=True)
def cleanup(request: pytest.FixtureRequest):
    os.makedirs(TEST_DATA_FOLDER, exist_ok=True)
    def remove_test_directory():
        shutil.rmtree(TEST_DATA_FOLDER)
    request.addfinalizer(remove_test_directory)

class TestConfig:
    def test_load_dict_from_toml_file(self):
        # Arrange
        test_file_name = TEST_DATA_FOLDER / "test_to_delete.toml"
        with open(test_file_name, "w") as fid:
            fid.writelines(
                [
                    "[any_dict]\n",
                    'any_key = "any_value"\n',
                    'another_key = "another_value"\n',
                ]
            )
        expected_result = {
            "any_dict": {
                "any_key": "any_value",
                "another_key": "another_value",
            }
        }
        sut = config.TomlFile(file_path=test_file_name)

        # Act
        result = sut.load()

        # Assert
        assert result == expected_result

    def test_write_dict_in_toml_file(self):
        # Arrange
        any_config = {
            "any_dict": {
                "any_key": "any_value",
                "another_key": "another_value",
            }
        }
        test_file_name = TEST_DATA_FOLDER / "test_to_delete.toml"
        sut = config.TomlFile(file_path=test_file_name)
        expected_result = [
            "[any_dict]\n",
            'any_key = "any_value"\n',
            'another_key = "another_value"\n',
        ]

        # Act
        sut.write(config=any_config)
        with open(test_file_name, "r") as fid:
            result = fid.readlines()

        # Assert
        assert result == expected_result

    def test_load_config_from_toml_file(self):
        # Arrange
        test_file_name = TEST_DATA_FOLDER / "test_to_delete.toml"
        with open(test_file_name, "w") as fid:
            fid.writelines(
                [
                    "[logging]\n",
                    'filename = "any filename"\n',
                    'format = "any format"\n',
                ]
            )
        config_file = config.TomlFile(test_file_name)
        expected_result = config.Config(
            logging= config.Logging(
                filename="any filename",
                format="any format",
            )
        )

        # Act
        result = config.load(config_file)

        # Assert
        assert result == expected_result

    def test_write_config_in_toml_file(self):
        # Arrange
        any_config = config.Config()
        any_config.logging.filename = "any filename"
        any_config.logging.format = "any format"
        test_file_name = TEST_DATA_FOLDER / "test_to_delete.toml"

        # Act
        config.write(any_config, config.TomlFile(test_file_name))
        with open(test_file_name, "rb") as fid:
            result = tomli.load(fid)

        # Assert
        assert result["logging"]["filename"] == any_config.logging.filename
        assert result["logging"]["format"] == any_config.logging.format

    def test_common_modules_parameters_is_flexible_dict(self):
        # Arrange
        any_config = config.Config()
        any_config.modules.commons = {
            "any_parameter": "any_value",
            "another_parameter": "another_value",
        }
        test_file_name = TEST_DATA_FOLDER / "test_to_delete.toml"

        # Act
        config.write(any_config, config.TomlFile(test_file_name))
        result = config.load(config.TomlFile(test_file_name))

        # Assert
        assert result.modules.commons["any_parameter"] == "any_value"
        assert result.modules.commons["another_parameter"] == "another_value"