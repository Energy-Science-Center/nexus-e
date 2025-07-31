import os
import tomli
import nexus_e.config


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
        sut = nexus_e.config.TomlFile(file_path=test_file_name)

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
        sut = nexus_e.config.TomlFile(file_path=test_file_name)
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
        config_file = nexus_e.config.TomlFile(test_file_name)
        expected_result = nexus_e.config.Config(
            logging=nexus_e.config.Logging(
                filename="any filename",
                format="any format",
            )
        )

        # Act
        try:
            result = nexus_e.config.load(config_file)
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

        # Assert
        assert result == expected_result

    def test_write_config_in_toml_file(self):
        # Arrange
        any_config = nexus_e.config.Config()
        any_config.logging.filename = "any filename"
        any_config.logging.format = "any format"
        test_file_name = "test_to_delete.toml"

        # Act
        try:
            nexus_e.config.write(
                any_config, nexus_e.config.TomlFile(test_file_name)
            )
            with open(test_file_name, "rb") as fid:
                result = tomli.load(fid)
        except Exception as e:
            raise e
        finally:
            os.remove(test_file_name)

        # Assert
        assert result["logging"]["filename"] == any_config.logging.filename
        assert result["logging"]["format"] == any_config.logging.format
