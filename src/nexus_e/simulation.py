from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime
import importlib
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typing import Protocol

from . import config

from nexus_e_interface import Plugin, Scenario

from plugins.centiv.cgep import create_scenario_fast as centiv
from plugins.postprocess.postprocess import PostProcess
from plugins.update_investments.nexus_e_plugin import NexusePlugin as UpdateInvestments
from plugins.upload_scenario.upload_scenario import ScenarioUploader
from plugins.upload_res_data import RESDataUploader

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

    def get_module(self, module_config: config.Module) -> Module:
        if module_config.name == "centiv":
            # First add module-wide parameters to avoid rewriting them in
            # the config file
            centiv_parameters = {}
            centiv_parameters["DB_host"] = (
                f"{self.settings.input_database_server.host}"
            )
            centiv_parameters["DB_name"] = self.settings.scenario.copy_name
            centiv_parameters["DB_user"] = self.settings.input_database_server.user
            centiv_parameters["DB_pwd"] = self.settings.input_database_server.password
            centiv_parameters["tpResolution"] = (
                self.settings.modules.commons.resolution_in_days
            )
            centiv_parameters["single_electric_node"] = (
                self.settings.modules.commons.single_electric_node
            )
            centiv_parameters["results_folder"] = (
                os.path.join(
                    self.settings.results.base_folder,
                    self.settings.results.simulation_folder
                )
            )
            centiv_parameters.update(module_config.parameters)
            return centiv.CentIvModule(centiv.Config(**centiv_parameters))
        elif module_config.name == "postprocess":
            return PostProcess(settings=self.settings)
        elif module_config.name == "update_investments":
            parameters = {}
            parameters["host"] = self.settings.input_database_server.host
            parameters["user"] = self.settings.input_database_server.user
            parameters["password"] = self.settings.input_database_server.password
            parameters["dbName"] = self.settings.scenario.copy_name
            parameters["simulation_results_folder"] = os.path.join(
                self.settings.results.base_folder,
                self.settings.results.simulation_folder
            )
            parameters.update(module_config.parameters)
            return UpdateInvestments(parameters=parameters)
        elif module_config.name == "upload_scenario":
            parameters = {}
            parameters["host"] = self.settings.input_database_server.host
            parameters["user"] = self.settings.input_database_server.user
            parameters["password"] = self.settings.input_database_server.password
            parameters.update(module_config.parameters)
            return ScenarioUploader(config=parameters)
        elif module_config.name == "update_inv_costs":
            from plugins.update_inv_costs import InvCostDataUpdater
            parameters = {}
            parameters["host"] = self.settings.input_database_server.host
            parameters["user"] = self.settings.input_database_server.user
            parameters["password"] = self.settings.input_database_server.password
            parameters["dbName"] = self.settings.scenario.copy_name
            parameters.update(module_config.parameters)
            return InvCostDataUpdater(config=parameters)
        elif module_config.name == "upload_res_data":
            parameters = {}
            parameters["host"] = self.settings.input_database_server.host
            parameters["user"] = self.settings.input_database_server.user
            parameters["password"] = self.settings.input_database_server.password
            parameters["dbName"] = self.settings.scenario.copy_name
            parameters.update(module_config.parameters)
            return RESDataUploader(config=parameters)
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

        # Prepare plugin parameters
        parameters: dict = plugin_module.NexusePlugin.get_default_config()
        parameters.update(asdict(self.settings.modules.commons))
        parameters.update(module_config.parameters)
        parameters = {
            key: value
            for key, value in parameters.items()
            if key in plugin_module.NexusePlugin.get_default_config()
        }

        # Create database session
        engine = create_engine(
            "mysql+pymysql://" \
            f"{self.settings.input_database_server.user}" \
            f":{self.settings.input_database_server.password}" \
            f"@{self.settings.input_database_server.host}" \
            f":{self.settings.input_database_server.port}" \
            f"/{self.settings.scenario.copy_name}" \
        )
        scenario = Scenario(Session(engine))

        output = plugin_module.NexusePlugin(
            scenario,
            parameters
        )
        return output

class Simulation:
    def __init__(self, settings: config.Config):
        self.settings = settings

    def run(self, module_factory: ModuleFactory):
        self.settings.simulation.execution_date = datetime.now().strftime(
            "%Y-%m-%dT%H-%M-%S"
        )
        logging.info("Save simulation execution date in settings")
        config.write(self.settings)

        if self.settings.results.create_new_simulation_results_folder:
            self.__setup_results_folder()

        for module_config in config.load_playlist(self.settings.modules.playlist_name):
            logging.info(f"Run module: {module_config}")
            module = module_factory.get_module(module_config)
            logging.info("Module created")
            module.run()

    def __setup_results_folder(self):
        timestamp = self.settings.simulation.execution_date
        results_folder_name = (
            f"{self.settings.scenario.original_name}_{timestamp}"
        )
        results_folder_path = os.path.join(
            self.settings.results.base_folder,
            results_folder_name
        )
        logging.info(f"Setup results folder: {results_folder_path}")
        os.makedirs(results_folder_path, exist_ok=True)
        
        self.settings.results.simulation_folder = results_folder_name
        logging.info("Save results folder in settings")
        config.write(self.settings)