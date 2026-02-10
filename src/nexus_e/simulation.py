import importlib
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Protocol

from nexus_e_interface import Plugin, Scenario
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from plugins.centiv.cgep import create_scenario_fast as centiv
from plugins.copy_database.nexus_e_plugin import NexusePlugin as CopyDatabase
from plugins.postprocess.nexus_e_plugin import NexusePlugin as PostProcess
from plugins.update_investments.nexus_e_plugin import NexusePlugin as UpdateInvestments
from plugins.upload_scenario.nexus_e_plugin import NexusePlugin as ScenarioUploader
from plugins.upload_res_data import RESDataUploader

from . import config


class Module(Protocol):
    def run(self) -> None: ...


class UnknownModule(Exception): ...


class ModuleFactory(ABC):
    @abstractmethod
    def __init__(self, settings: config.Config): ...

    @abstractmethod
    def get_module(self, module_config: config.Module) -> Module: ...


class CoreModuleFactory(ModuleFactory):
    def __init__(self, settings: config.Config):
        self.settings = settings

    def get_module(self, module_config: config.Module) -> Module | Plugin:
        if module_config.name == "centiv":
            # First add module-wide parameters to avoid rewriting them in
            # the config file
            parameters = {}
            parameters["DB_host"] = self.settings.modules.commons["input_data_host"]
            parameters["DB_name"] = self.settings.scenario.copy_name
            parameters["DB_user"] = self.settings.modules.commons["input_data_user"]
            parameters["DB_pwd"] = self.settings.modules.commons["input_data_password"]
            parameters["tpResolution"] = (
                self.settings.modules.commons["resolution_in_days"]
            )
            parameters["single_electric_node"] = (
                self.settings.modules.commons["single_electric_node"]
            )
            parameters["results_path"] = self.settings.modules.commons["results_path"]
            parameters.update(module_config.parameters)
            return centiv.CentIvModule(parameters)
        elif module_config.name == "postprocess":
            parameters = {}
            parameters["results_path"] = self.settings.modules.commons["results_path"]
            parameters["input_data_name"] = self.settings.scenario.copy_name
            parameters["execution_date"] = self.settings.modules.commons["execution_date"]
            parameters["single_electric_node"] = self.settings.modules.commons["single_electric_node"]
            parameters["input_host"] = self.settings.modules.commons["input_data_host"]
            parameters["input_user"] = self.settings.modules.commons["input_data_user"]
            parameters["input_password"] = self.settings.modules.commons["input_data_password"]
            parameters.update(module_config.parameters)
            return PostProcess(parameters)
        elif module_config.name == "update_investments":
            parameters = {}
            parameters["host"] = self.settings.modules.commons["input_data_host"]
            parameters["user"] = self.settings.modules.commons["input_data_user"]
            parameters["password"] = self.settings.modules.commons["input_data_password"]
            parameters["dbName"] = self.settings.scenario.copy_name
            parameters["simulation_results_folder"] = self.settings.modules.commons["results_path"]
            parameters.update(module_config.parameters)
            return UpdateInvestments(parameters)
        elif module_config.name == "upload_scenario":
            parameters = {}
            parameters["host"] = self.settings.modules.commons["input_data_host"]
            parameters["user"] = self.settings.modules.commons["input_data_user"]
            parameters["password"] = self.settings.modules.commons["input_data_password"]
            parameters.update(module_config.parameters)
            return ScenarioUploader(parameters)
        elif module_config.name == "update_inv_costs":
            from plugins.update_inv_costs import InvCostDataUpdater
            parameters = {}
            parameters["host"] = self.settings.modules.commons["input_data_host"]
            parameters["user"] = self.settings.modules.commons["input_data_user"]
            parameters["password"] = self.settings.modules.commons["input_data_password"]
            parameters["dbName"] = self.settings.scenario.copy_name
            parameters.update(module_config.parameters)
            return InvCostDataUpdater(config=parameters)
        elif module_config.name == "upload_res_data":
            parameters = {}
            parameters["host"] = self.settings.modules.commons["input_data_host"]
            parameters["user"] = self.settings.modules.commons["input_data_user"]
            parameters["password"] = self.settings.modules.commons["input_data_password"]
            parameters["dbName"] = self.settings.scenario.copy_name
            parameters.update(module_config.parameters)
            return RESDataUploader(config=parameters)
        elif module_config.name == "copy_database":
            parameters = {}
            parameters.update(self.settings.modules.commons)
            parameters.update(module_config.parameters)
            return CopyDatabase(parameters=parameters)
        else:
            raise UnknownModule(module_config.name)


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
        parameters: dict = plugin.get_default_parameters()
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
            f"/{self.settings.scenario.copy_name}"
        )
        scenario = Scenario(Session(engine))

        output = plugin(scenario, parameters)
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
        results_folder_name = f"{self.settings.scenario.original_name}_{timestamp}"
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