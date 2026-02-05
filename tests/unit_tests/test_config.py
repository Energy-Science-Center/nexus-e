import os
import tomli
import src.nexus_e.config as config


class TestConfig:
    def test_load_dict_from_toml_file(self):
        # Arrange
        test_file_name = "test_to_delete.toml"
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
        try:
            result = sut.load()
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

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
        test_file_name = "test_to_delete.toml"
        sut = config.TomlFile(file_path=test_file_name)
        expected_result = [
            "[any_dict]\n",
            'any_key = "any_value"\n',
            'another_key = "another_value"\n',
        ]

        # Act
        try:
            sut.write(config=any_config)
            with open(test_file_name, "r") as fid:
                result = fid.readlines()
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

        # Assert
        assert result == expected_result

    def test_load_config_from_toml_file(self):
        # Arrange
        test_file_name = "test_to_delete.toml"
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
        try:
            result = config.load(config_file)
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

        # Assert
        assert result == expected_result

    def test_write_config_in_toml_file(self):
        # Arrange
        any_config = config.Config()
        any_config.logging.filename = "any filename"
        any_config.logging.format = "any format"
        test_file_name = "test_to_delete.toml"

        # Act
        try:
            config.write(any_config, config.TomlFile(test_file_name))
            with open(test_file_name, "rb") as fid:
                result = tomli.load(fid)
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

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
        test_file_name = "test_to_delete.toml"

        # Act
        try:
            config.write(any_config, config.TomlFile(test_file_name))
            result = config.load(config.TomlFile(test_file_name))
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

        # Assert
        assert result.modules.commons["any_parameter"] == "any_value"
        assert result.modules.commons["another_parameter"] == "another_value"