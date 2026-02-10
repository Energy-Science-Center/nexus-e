"""Serve as the python entry point of Nexus-e.

Currently Nexus-e is executed by calling the MATLAB script
Run_nexuse/run_Nexuse_platform.m. This process will be replaced by the present
module and executing Nexus-e will be possible by running App.main().
"""

import logging
import os

from . import config
from .simulation import Simulation, CoreModuleFactory


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
            module_factory = CoreModuleFactory(
                settings=settings
            )
            simulation = Simulation(settings=settings)
            simulation.run(module_factory=module_factory)

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
        
def main():
    App.main()

if __name__ == "__main__":
    main()