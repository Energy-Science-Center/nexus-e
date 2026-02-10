from abc import ABC, abstractmethod
from datetime import datetime
import importlib
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typing import Protocol

from nexus_e_interface import Plugin, Scenario

from . import config


class Module(Protocol):
    def run(self) -> None: ...


class UnknownModule(Exception): ...


class ModuleFactory(ABC):
    @abstractmethod
    def __init__(self, settings: config.Config): ...

    @abstractmethod
    def get_module(self, module_config: config.Module) -> Module: ...


class CorePluginFactory(ModuleFactory):
    def __init__(self, settings: config.Config):
        self.settings = settings

    def get_module(self, module_config: config.Module) -> Plugin:
        # Dynamically import plugin module
        try:
            plugin_module = importlib.import_module(
                name=f"plugins.{module_config.name}.nexus_e_plugin",
            )
        except ModuleNotFoundError:
            raise UnknownModule(module_config.name)
        plugin: Plugin = plugin_module.NexusePlugin

        # Prepare plugin parameters
        parameters = {}
        parameters.update(self.settings.modules.commons)
        parameters.update(module_config.parameters)
        parameters = {
            key: value
            for key, value in parameters.items()
            if key in plugin.get_default_parameters()
        }

        # Create database session
        engine = create_engine(
            "mysql+pymysql://"
            f"{self.settings.modules.commons['input_data_user']}"
            f":{self.settings.modules.commons['input_data_password']}"
            f"@{self.settings.modules.commons['input_data_host']}"
            f":{self.settings.modules.commons['input_data_port']}"
            f"/{self.settings.modules.commons['input_data_name']}"
        )
        scenario = Scenario(Session(engine))

        output = object.__new__(plugin_module.NexusePlugin)
        output.__init__(
            parameters=parameters,
            scenario=scenario
        )
        return output


class Simulation:
    def __init__(self, settings: config.Config):
        self.settings = settings

    def run(self, module_factory: ModuleFactory):
        self.settings.modules.commons["execution_date"] = datetime.now().strftime(
            "%Y-%m-%dT%H-%M-%S"
        )
        logging.warning(
            "Update common parameter execution_date "
            f"with value {self.settings.modules.commons['execution_date']}"
        )
        config.write(self.settings)

        if self.settings.results.create_new_simulation_results_folder:
            self.__setup_results_folder()

        for module_config in config.load_playlist(self.settings.modules.playlist_name):
            logging.info(f"Run module: {module_config}")
            module: Module = module_factory.get_module(module_config)
            logging.info("Module created")
            updated_common_parameters: dict | None = module.run()
            if updated_common_parameters:
                updated_common_parameters = {
                    k: v
                    for k, v in updated_common_parameters.items()
                    # None is not TOML serializable
                    if v is not None
                }
                for k, v in updated_common_parameters.items():
                    logging.warning(f"Update common parameter {k} with value: {v}")
                self.settings.modules.commons.update(updated_common_parameters)
                config.write(self.settings)

    def __setup_results_folder(self):
        timestamp = self.settings.modules.commons["execution_date"]
        results_folder_name = (
            f"{self.settings.modules.commons['input_data_name']}_{timestamp}"
        )
        results_folder_path = os.path.join(
            self.settings.results.base_folder, results_folder_name
        )
        logging.info(f"Setup results folder: {results_folder_path}")
        os.makedirs(results_folder_path, exist_ok=True)

        self.settings.modules.commons["results_path"] = results_folder_path
        logging.warning(
            "Update common parameter results_path "
            f"with value {results_folder_path}"
        )
        config.write(self.settings)