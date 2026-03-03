"""Handle config files in Nexus-e.

This module's purpose is to provide an easy access to the variables stored in a
config file. The ConfigFile interface declare methods to load and write a
dictionnary from a given file. The TomlFile class implements this interface for
.toml files.

To avoid having to use this dictionnary directly, the module provides
a Config class that can conveniently parse and be exported to a dictionnary.
The config variables can then be accessed via the Config class attributes.

The load(), write() and write_default_config_file() functions of this module
can be used to handle a Config object with a pre-defined ConfigFile object.

Example:
    Write a default config file and update a config variable value::

        import config
        config.write_default_config_file()
        settings = config.load()
        settings.logging.filename = "alternative.log"
        config.write(settings)

Running this script asks the user if a default config file should be written.
"""

import os
from typing import Any, List
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
import tomli
import tomli_w

from nexus_e_interface import DataContext

CONFIG_FILE_NAME = "config.toml"
"""The default config file name."""


class ConfigFile(ABC):
    """Interface for loading from and writing in a config file."""

    @abstractmethod
    def __init__(self, file_path: str):
        """Store the file path in an object's attribute.

        Args:
            file_path: Full path to the config file.
        """
        pass

    @abstractmethod
    def load(self) -> dict:
        """Load the full content of the config file as a dictionnary."""
        pass

    @abstractmethod
    def write(self, config: dict):
        """Fill the config file with the content of a dictionnary.

        Args:
            config: structured config variables
        """
        pass


class TomlFile(ConfigFile):
    """Implement the ConfigFile interface for .toml files."""

    def __init__(self, file_path: str):
        self.__config_file_path = file_path

    def load(self) -> dict:
        with open(self.__config_file_path, "rb") as fid:
            return tomli.load(fid)

    def write(self, config: dict):
        with open(self.__config_file_path, "wb") as fid:
            tomli_w.dump(config, fid, multiline_strings=True)


@dataclass
class Logging:
    """Config section about the logger's parameters.
    https://docs.python.org/3/library/logging.html#logging.basicConfig"""

    filename: str = "nexus-e.log"
    filemode: str = "w"
    format: str = "%(asctime)s %(levelname)s %(message)s"
    """See how to build a log record format string here:
    https://docs.python.org/3/library/logging.html#logrecord-attributes"""
    date_format: str = "%Y-%m-%d %H:%M:%S"
    """See how to build a date format string here:
    https://docs.python.org/3/library/time.html#time.strftime"""
    level: str = "INFO"
    """The logging level can be given as a string, to be chosen among:
    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    https://docs.python.org/3/library/logging.html#levels"""


@dataclass
class Module:
    name: str = ""
    parameters: dict = field(default_factory=dict)


@dataclass
class Results:
    base_folder: str = "Results"
    create_new_simulation_results_folder: bool = True


@dataclass
class Modules:
    commons: dict[str, Any] = field(default_factory=lambda: 
        {
            "resolution_in_days": 1,
            "single_electric_node": False,
        }
    )
    playlist_name: str = "nothing"


@dataclass
class Config:
    """Define the config variables' structure.

    To add a new config variable you can add it as an attribute of an existing
    or a new dataclass that is itself an attribute of this Config class.

    Example:
        Add a new config variable and a new dataclass::

            @dataclass
            class NewConfigSection:
                new_config_variable: str = "default_variable_value"

            @dataclass
            class Config:
                new_config_section: NewConfigSection = field(default_factory=NewConfigSection)

    The default value of the config variable is given by the default value of
    its corresponding dataclass attribute.
    """

    logging: Logging = field(default_factory=Logging)
    results: Results = field(default_factory=Results)
    data_context: DataContext = field(default_factory=DataContext)
    modules: Modules = field(default_factory=Modules)

    def parse(self, **config: dict):
        """Populate the Config class with an input dictionnary."""
        for key in config.keys():
            if isinstance(config[key], dict):
                config_class_name = "".join(
                    word.capitalize() for word in key.split("_")
                )
                setattr(self, key, eval(config_class_name)(**config[key]))


def load(config_file: ConfigFile = TomlFile(CONFIG_FILE_NAME)) -> Config:
    """Parse the content of a full file in a Config object."""
    config_as_dict = config_file.load()
    output = Config()
    output.parse(**config_as_dict)
    return output


def load_playlist(playlist_name: str) -> List[Module]:
    playlist_file = TomlFile(
        os.path.join("modules_playlists", f"{playlist_name}.toml"))
    playlist_as_dict = playlist_file.load()
    modules = playlist_as_dict["modules"]
    if not any(modules):
        return []
    else:
        return [Module(**module) for module in modules]


def write(
    config: Config, config_file: ConfigFile = TomlFile(CONFIG_FILE_NAME)
):
    """(Over)write a file with the content of a Config object."""
    config_file.write(asdict(config))


def write_default_config_file(
    config_file: ConfigFile = TomlFile(CONFIG_FILE_NAME),
):
    """The default config is created by directly instantiating a Config() object."""
    default_config = Config()
    write(default_config, config_file)


if __name__ == "__main__":
    if input("Write default config file? [Y/n]: ") in "Yy":
        write_default_config_file()
