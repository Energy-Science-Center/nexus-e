"""Handle config files in Nexus-e.

This module's purpose is to provide an easy access to the variables stored in a
config file. The ConfigFile interface declare methods to load and write a
dictionary from a given file. The TomlFile class implements this interface for
.toml files.

To avoid having to use this dictionary directly, the module provides
a Config class that can conveniently parse and be exported to a dictionary.
The config variables can then be accessed via the Config class attributes.

The load(), write() and write_default_config_file() functions of this module
can be used to handle a Config object with a pre-defined ConfigFile object.

Example:
    Write a default config file and update a config variable value:

    ```python
    import config
    config.write_default_config_file()
    settings = config.load()
    settings.logging.filename = "alternative.log"
    config.write(settings)
    ```
"""

import os
from pathlib import Path
from typing import Any, List, Literal
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
from nexus_e.execution_mode import ExecutionMode
import tomli
import tomli_w

from nexus_e_interface import DataContext

CONFIG_FILE_NAME = "config.toml"
"""The default config file name."""


class ConfigFile(ABC):
    """Interface for loading from and writing in a config file."""

    @abstractmethod
    def __init__(self, file_path: str | Path):
        """Store the file path in an object's attribute."""
        pass

    @abstractmethod
    def load(self) -> dict:
        """Load the full content of the config file as a dictionary."""
        pass

    @abstractmethod
    def write(self, config: dict):
        """Fill the config file with the content of a dictionary."""
        pass


class TomlFile(ConfigFile):
    """Implement the ConfigFile interface for .toml files."""

    def __init__(self, file_path: str | Path):
        self.__config_file_path = file_path

    def load(self) -> dict:
        with open(self.__config_file_path, "rb") as fid:
            return tomli.load(fid)

    def write(self, config: dict):
        with open(self.__config_file_path, "wb") as fid:
            tomli_w.dump(config, fid, multiline_strings=True)


@dataclass
class Logging:
    """
    Config section about the [logger's parameters](https://docs.python.org/3/library/logging.html#logging.basicConfig).
    """

    filename: str = "nexus-e.log"
    filemode: str = "w"
    format: str = "%(asctime)s %(levelname)s %(message)s"
    """See how to build a log record format string
    [here](https://docs.python.org/3/library/logging.html#logrecord-attributes)."""
    date_format: str = "%Y-%m-%d %H:%M:%S"
    """See how to build a date format string
    [here](https://docs.python.org/3/library/time.html#time.strftime)."""
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    """The logging level can be given as a string. More
    [here](https://docs.python.org/3/library/logging.html#levels)."""


@dataclass
class Module:
    """
    Represents a module configuration with its name and its associated parameters.
    """
    name: str = ""
    """The name of the module to import."""
    parameters: dict = field(default_factory=dict)
    """A dictionary containing the module's specific parameters."""


@dataclass
class Results:
    """
    Represents configuration settings for simulation results management.
    """
    base_folder: str = "Results"
    """The base directory where simulation results are stored."""
    create_new_simulation_results_folder: bool = True
    """Determines whether a new folder should be created for each simulation's results."""


@dataclass
class Modules:
    """
    Represents configuration settings for all modules.
    """
    commons: dict[str, Any] = field(default_factory=lambda: 
        {
            "resolution_in_days": 8,
            "single_electric_node": True,
        }
    )
    """The extra parameters passed to every Module."""
    playlist_name: str = "end_to_end_test"
    """The file defining the modules to load and their execution order."""


@dataclass
class Config:
    """Define the config variables' structure.

    To add a new config variable you can add it as an attribute of an existing
    or a new dataclass that is itself an attribute of this Config class.

    Example:
        Add a new config variable and a new dataclass:

        ```python
        @dataclass
        class NewConfigSection:
            new_config_variable: str = "default_variable_value"

        @dataclass
        class Config:
            new_config_section: NewConfigSection = field(default_factory=NewConfigSection)
        ```

    The default value of the config variable is given by the default value of
    its corresponding dataclass attribute.
    """

    logging: Logging = field(default_factory=Logging)
    results: Results = field(default_factory=Results)
    data_context: DataContext = field(default_factory=DataContext)
    modules: Modules = field(default_factory=Modules)

    def parse(self, **config: dict):
        """Populate the Config class with an input dictionary."""
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

    if Path("config.toml").exists():
        if input("Overwrite config file? [Y/n]: ") not in "Yy":
            return

    default_config = Config()
    print()
    print("Please enter the input database server credentials you want to use.")
    print("If you're part of the Nexus-e team, you can use our MySQL server: https://unlimited.ethz.ch/spaces/WikiNexusE/pages/406917252/MySQL+server")
    default_config.data_context.host = input("  host: ")
    default_config.data_context.port = input("  port: ")
    default_config.data_context.user = input("  user: ")
    default_config.data_context.password = input("  password: ")
    default_config.data_context.name = input("  database: ")
    write(default_config, config_file)


class Cli(ExecutionMode):
    command: str = "config"
    help: str = (
        "Modify config parameters. "
        "Use the following format '--table parameter value'. "
        "See the available TOML tables with 'uv run nexus-e config --help'"
    )

    @classmethod
    def add_arguments(cls):

        cls.parser.add_argument(
            "--file",
            help="The name of the config file to modify.",
        )
        cls.parser.add_argument(
            "--logging",
            help=(
                "Modify parameters in [logging]. "
                "e.g. 'uv run nexus-e config --logging filemode a'"
            ),
            action="append",
            nargs=2
        )
        cls.parser.add_argument(
            "--results",
            help=(
                "Modify parameters in [results]. "
                "e.g. 'uv run nexus-e config --results base_folder project_x'"
            ),
            action="append",
            nargs=2
        )
        cls.parser.add_argument(
            "--data_context",
            help=(
                "Modify parameters in [data_context]. "
                "e.g. 'uv run nexus-e config --data_context name new_name'"
            ),
            action="append",
            nargs=2
        )
        cls.parser.add_argument(
            "--modules",
            help=(
                "Modify parameters in [modules]. "
                "e.g. 'uv run nexus-e config --modules playlist_name new_name'"
            ),
            action="append",
            nargs=2
        )
        cls.parser.add_argument(
            "--modules-commons",
            help=(
                "Modify parameters in [modules.commons]. "
                "e.g. 'uv run nexus-e config --modules-commons resolution_in_days 1'"
            ),
            action="append",
            nargs=2
        )

    @classmethod
    def start(cls, args: dict):
        if args["file"] is not None:
            config_file_path = args.pop("file")
        else:
            config_file_path = CONFIG_FILE_NAME
        config_file = TomlFile(config_file_path)

        def is_valid_float(string: str) -> bool:
            return (
                "." in string
                and string.count(".") == 1
                and all([part.isdigit() for part in string.split(".")])
            )
        
        settings = load(config_file)
        new_settings = asdict(settings)

        for arg_name, arg_values in args.items():
            if arg_name in [
                cls.command,
                "start_execution_mode",
            ]:
                continue
            if arg_values is None:
                continue

            for key, value in arg_values:
                if value.lower() in ["true", "false"]:
                    value = eval(value.capitalize())
                elif value.isdigit():
                    value = int(value)
                elif is_valid_float(value):
                    value = float(value)
                if arg_name == "modules_commons":
                    new_settings["modules"]["commons"][key] = value
                else:
                    new_settings[arg_name][key] = value

        settings.parse(**new_settings)
        write(settings, config_file)
            

