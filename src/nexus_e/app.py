"""Serve as the python entry point of Nexus-e.
"""

from abc import ABC
from argparse import ArgumentParser, _SubParsersAction
import logging
import os
import sys

from . import config
from .simulation import Simulation, CorePluginFactory


class App:
    @classmethod
    def main(cls):
        settings = cls.__get_settings()
        cls.__setup(settings)
        cls.__run_simulation(settings)

    @classmethod
    def __get_settings(cls):
        if not os.path.isfile(config.CONFIG_FILE_NAME):
            config.write_default_config_file()
        return config.load()

    @classmethod
    def __setup(cls, settings: config.Config):
        logging.basicConfig(
            filename=os.path.expanduser(settings.logging.filename),
            filemode=settings.logging.filemode,
            format=settings.logging.format,
            datefmt=settings.logging.date_format,
            level=settings.logging.level,
        )

    @classmethod
    def __run_simulation(cls, settings: config.Config):
        error = None
        try:
            plugin_factory = CorePluginFactory(
                settings=settings
            )
            simulation = Simulation(settings=settings)
            simulation.run(module_factory=plugin_factory)

        except Exception as e:
            error = e  # store the exception
            logging.error(
                f"An error occurred during the simulation: {e}",
                exc_info=True
            )
            raise e

        # The error is handled this way because on the Euler cluster,
        # I want the error to still appear in the SLURM log and error files.
        # By storing the exception and re-raising it after the `finally` block,
        # I ensure that cleanup runs first, but the error is not silently swallowed.
        if error:
            raise error
        

class ExecutionMode(ABC):
    command: str
    help: str

    @classmethod
    def add_to_parser(cls, execution_modes: _SubParsersAction):
        cls.parser = execution_modes.add_parser(
            name=cls.command,
            help=cls.help
        )
        cls.add_arguments()
        cls.parser.set_defaults(start_execution_mode=cls.start)

    @classmethod
    def add_arguments(cls): ...

    @classmethod
    def start(cls, args: dict): ...


class SetupMode(ExecutionMode):
    command: str = "setup"
    help: str = (
        "Use to create your first config file, or overwrite the "
        "existing config.toml file."
    )

    @classmethod
    def start(cls, args: dict):
        config.write_default_config_file()


class InitMode(ExecutionMode):
    command: str = "init"
    help: str = "Initialize current directory with config and playlist files."

    @classmethod
    def start(cls, args: dict):
        pass


class ConfigMode(ExecutionMode):
    command: str = "config"
    help: str = ""

    @classmethod
    def start(cls, args: dict):
        pass


class RunMode(ExecutionMode):
    command: str = "run"
    help: str = "Run a Nexus-e simulation as defined in the config.toml file."

    @classmethod
    def start(cls, args: dict):
        App.main()
    

def main():
    parser = ArgumentParser()

    execution_modes = parser.add_subparsers()
    SetupMode.add_to_parser(execution_modes)
    InitMode.add_to_parser(execution_modes)
    ConfigMode.add_to_parser(execution_modes)
    RunMode.add_to_parser(execution_modes)

    no_cli_argument = len(sys.argv) == 1
    if no_cli_argument:
        parser.print_help()
        return
    
    args = parser.parse_args()
    args.start_execution_mode(args.__dict__)

if __name__ == "__main__":
    main()