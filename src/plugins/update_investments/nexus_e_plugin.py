from dataclasses import dataclass, asdict
import os

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from nexus_e_interface import Plugin, Scenario
import nexus_e_interface.tables as tables

@dataclass
class Config:
    startyear: int = 2020
    endyear: int = 2020
    scenYear: int = 2
    results_path: str = ""
    matlab_engine: str = "matlab"


class NexusePlugin(Plugin):
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Config())

    def __init__(self, parameters: dict, scenario: Scenario):
        self.scenario = scenario
        self.config = Config(**parameters)
        self.__data_context = scenario.get_data_context()
        if self.__data_context.type != "mysql":
            raise ValueError("update_investments only works with a MySQL database")

    def run(self) -> None:
        engine = self.__get_matlab_engine(self.config.matlab_engine)
        engine.addpath(engine.genpath("."))
        engine.workspace["parameters"] = { 
            "scenId": self.__get_scenario_id(),
            "startyear": self.config.startyear,
            "endyear": self.config.endyear,
            "scenYear": self.config.scenYear,
            "dbName": self.__data_context.name
        }
        engine.update_investments(
            engine.workspace["parameters"],
            engine.database(
                self.__data_context.name,
                self.__data_context.user,
                self.__data_context.password,
                "Vendor",
                "MySQL",
                "Server",
                self.__data_context.host,
            ),
            os.path.join(
                self.config.results_path,
                f"CentIv_{self.config.scenYear}"
            ),
            nargout=0
        )
        engine.quit()

    def __get_matlab_engine(self, engine_name: str):
        if engine_name == "matlab":
            import matlab.engine
            return matlab.engine.start_matlab()
        else:
            raise ValueError(
                f"Unsupported engine: {engine_name}. Only 'matlab' is supported."
            )
        
    def __get_scenario_id(self):
        session=Session(create_engine(
                "mysql+pymysql://"
                + f"{self.__data_context.user}:{self.__data_context.password}"
                + f"@{self.__data_context.host}/{self.__data_context.name}"
        ))
        statement = (
            select(tables.ScenarioConfiguration.idScenario)
            .where(tables.ScenarioConfiguration.Year == self.config.scenYear)
        )
        result = session.scalar(statement)
        session.close()
        if result is None:
            raise ValueError(
                f"No scenario found for year {self.config.scenYear}."
            )
        return result