from argparse import ArgumentParser
import os
from pathlib import Path
import shutil
import pytest
import tomli
import src.nexus_e.config as config

TEST_DATA_FOLDER = Path() / "tests" / "tmp"
"""Everything in this folder is deleted after running the tests"""
TEST_FILE = TEST_DATA_FOLDER / "test_to_delete.toml"

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


class TestConfigCli:
    @pytest.mark.parametrize(("raw_args", "expected_config"),
        [
            (
                [
                    "config",
                    "--data_context", "name", "any_name",
                    "--data_context", "host", "any_host",
                ],
                {
                    "data_context": {
                        "name": "any_name",
                        "host": "any_host"
                    }
                }
            ),
            (
                [
                    "config",
                    "--modules", "playlist_name", "any_name",
                    "--modules-commons", "any_parameter", "any_value",
                ],
                {
                    "modules": {
                        "playlist_name": "any_name",
                        "commons": {
                            "any_parameter": "any_value"
                        }
                    }
                }
            ),
            (
                [
                    "config",
                    "--modules-commons", "any_integer", "1",
                    "--modules-commons", "any_float", "1.2",
                    "--modules-commons", "any_boolean", "true",
                    "--modules-commons", "another_boolean", "false",
                ],
                {
                    "modules": {
                        "commons": {
                            "any_integer": 1,
                            "any_float": 1.2,
                            "any_boolean": True,
                            "another_boolean": False,
                        }
                    }
                }
            ),
        ]
    )
    def test_update_config_parameters_through_cli(
        self,
        raw_args: list[str],
        expected_config: dict
    ):
        # Arrange
        config_file_path = TEST_DATA_FOLDER / "test_to_delete.toml"
        config_file = config.TomlFile(config_file_path)
        default_config = config.Config()
        config.write(default_config, config_file)
        raw_args.append("--file")
        raw_args.append(str(config_file_path.absolute()))
        parser = ArgumentParser()
        config.Cli.add_to_parser(parser.add_subparsers())
        sut = parser.parse_args(raw_args)
        expected_results = config.Config()
        expected_results.parse(**expected_config)
                
        # Act
        sut.start_execution_mode(sut.__dict__)
        results = config.load(config_file)

        # Assert

        # check that expected_results.modules.commons dict is a subset of the
        # results.modules.commons dict
        # Necessary because the dict given as a pytest parameter overwrites
        # the default items in Config.modules.commons dict
        assert expected_results.modules.commons.items() <= results.modules.commons.items()
        expected_results.modules.commons = results.modules.commons

        assert results == expected_results