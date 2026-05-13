from abc import ABC
from argparse import _SubParsersAction, ArgumentParser


class ExecutionMode(ABC):
    command: str
    help: str

    @classmethod
    def add_to_parser(cls, execution_modes: _SubParsersAction):
        cls.parser: ArgumentParser = execution_modes.add_parser(
            name=cls.command,
            help=cls.help
        )
        cls.add_arguments()
        cls.parser.set_defaults(start_execution_mode=cls.start)

    @classmethod
    def add_arguments(cls): ...

    @classmethod
    def start(cls, args: dict): ...