from abc import ABC, abstractmethod
from datetime import datetime
import logging
import os
from typing import Protocol

from . import config

from plugins.centiv.cgep import create_scenario_fast as centiv
from plugins.postprocess.postprocess import PostProcess
from plugins.update_investments.update_investments import UpdateInvestments
from plugins.upload_scenario.upload_scenario import ScenarioUploader


class Module(Protocol):
    def run() -> None: ...


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
        else:
            raise UnknownModule(module_config.name)


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