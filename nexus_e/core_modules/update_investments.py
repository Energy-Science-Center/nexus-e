from dataclasses import dataclass
import nexus_e_interface.tables as tables
import os
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


@dataclass
class Config:
    host: str
    user: str
    password: str
    dbName: str
    startyear: int
    endyear: int
    scenYear: int
    simulation_results_folder: str
    matlab_engine: str = "matlab"


class UpdateInvestments:
    def __init__(self, parameters: dict):
        self.config = Config(**parameters)

    def run(self):
        engine = self.__get_matlab_engine(self.config.matlab_engine)
        engine.addpath(engine.genpath("."))
        engine.workspace["parameters"] = { 
            "scenId": self.__get_scenario_id(),
            "startyear": self.config.startyear,
            "endyear": self.config.endyear,
            "scenYear": self.config.scenYear,
            "dbName": self.config.dbName
        }
        engine.update_investments(
            engine.workspace["parameters"],
            engine.database(
                self.config.dbName,
                self.config.user,
                self.config.password,
                "Vendor",
                "MySQL",
                "Server",
                self.config.host,
            ),
            os.path.join(
                self.config.simulation_results_folder,
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
                + f"{self.config.user}:{self.config.password}"
                + f"@{self.config.host}/{self.config.dbName}"
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


