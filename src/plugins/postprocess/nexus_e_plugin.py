from dataclasses import dataclass, asdict
from datetime import datetime
import logging
import os
import pandas as pd
import shutil

from nexus_e_interface import Plugin, Scenario
from .centiv import CentivPostprocess
from .cascades import CascadesPostprocess
from .legacy import moveToMysql

@dataclass
class Parameters:
    results_path: str = ""
    input_data_name: str = ""
    plot_config_file_path: str = ""
    centiv: bool = False
    cascades: bool = False
    move_to_mysql: bool = False
    scenario_description: str = ""
    execution_date: str = ""
    input_data_host: str = ""
    input_data_user: str = ""
    input_data_password: str = ""
    output_name: str = ""
    output_host: str = ""
    output_port: str = "3307"
    output_user: str = ""
    output_password: str = ""
    single_electric_node: bool = False

class NexusePlugin(Plugin):
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Parameters())

    def __init__(self, parameters: dict, scenario: Scenario | None = None):
        if "output_host" not in parameters:
            parameters["output_host"] = parameters["input_data_host"]
        if "output_port" not in parameters:
            logging.warning("Default output database port is set to 3307 for legacy reasons.")
        if "output_user" not in parameters:
            parameters["output_user"] = parameters["input_data_user"]
        if "output_password" not in parameters:
            parameters["output_password"] = parameters["input_data_password"]
        if "output_name" not in parameters:
            parameters["output_name"] = parameters["input_data_name"]
            logging.warning(
                (
                    "No output_name given, defaulting to input_data_name: "
                    + f"{parameters['output_name']}"
                )
            )
        self.__parameters = Parameters(**parameters)
        self.__simulation_results = SimulationResults(
            results_path = self.__parameters.results_path,
        )

    def run(self) -> None:
        self.write_metadata()
        if os.path.exists(self.__parameters.plot_config_file_path):
            self.__simulation_results.copy_plot_config(
                self.__parameters.plot_config_file_path
            )
        if self.__parameters.centiv:
            centiv_postprocess = CentivPostprocess(
                results_path=self.__parameters.results_path,
                input_data_name=self.__parameters.input_data_name,
                input_host=self.__parameters.input_data_host,
                input_user=self.__parameters.input_data_user,
                input_password=self.__parameters.input_data_password,
                single_electric_node=self.__parameters.single_electric_node,
            )
            centiv_postprocess.run()
        if self.__parameters.cascades:
            cascades_postprocess = CascadesPostprocess(
                self.__simulation_results.postprocess_path
            )
            cascades_postprocess.run()
        if self.__parameters.move_to_mysql:
            self.move_to_mysql()
        logging.info("Postprocess finished")

    def write_metadata(self):
        metadata = self.__simulation_results.get_metadata(
            scenario_description = self.__parameters.scenario_description,
        )
        self.__simulation_results.write_csv_in_postprocess(
            data_to_write = metadata,
            file_name = "metadata.csv"
        )
    
    def move_to_mysql(self):
        """ Here should be re-implemented the following logic taken from 
        postProcess.m:

            disp('---------------- MySQL -----------------')
                try
                    cd(workingDir);
                    command = ['python'...
                        ' moveToMysql.py'...
                        ' --simu-name ' simuName...
                        ' --scen-name "' scenShortName...
                        '" --version-wv ' wspace.v_wv];
                    disp(command)
                    system(ACTIVATE_ENVIRONMENT + command);
                catch
                    disp("FAILED to upload results to the output database.")
                end
        """
        logging.info("Moving results to the output database")
        moveToMysql.main(
            simulation_postprocess_path = self.__simulation_results.postprocess_path,
            simulation_execution_date = self.__parameters.execution_date,
            scenario = self.__parameters.output_name,
            webviewer_version = "results",
            host=self.__parameters.output_host,
            port=self.__parameters.output_port,
            user=self.__parameters.output_user,
            password=self.__parameters.output_password
        )
        logging.info("DONE")
        

class SimulationResults():

    def __init__(self, results_path: str):
        self.__path = results_path
        self.__postprocess_output_folder = os.path.join(
            self.__path,
            "postprocess"
        )

    @property
    def path(self):
        return self.__path

    @property
    def postprocess_path(self):
        return self.__postprocess_output_folder

    def get_metadata(
        self,
        scenario_description: str = "",
    ) -> pd.DataFrame:
        """
            Metadata fields: 
            - simulation_submission_time (Temporarily removed because this
                info is unknown if Nexus-e is executed locally)
            - reference_year (placeholder)
            - simulated_years
            - scenario_name
            - scenario_short_name
            Some fields are just placeholders, where we need to fill it up manually.
        """
        simulated_years = self.get_simulated_years()
        output = pd.DataFrame.from_dict(
            {
                # "simulation_submission_time": submission_time,
                "reference_year": "2015", # could be left empty
                "simulated_years": ",".join(map(str, simulated_years)),
                "scenario_description": scenario_description,
                # scenario_short_name doesn't seem to be used but must exist
                "scenario_short_name": ""
            },
            orient='index',
            columns=["info"]
        )
        output.index.name = "name"
        return output

    def get_simulated_years(self) -> list[int]:
        if not os.path.isdir(self.__path):
            return []
        results_folders = [
            element 
            for element in os.listdir(self.__path)
            if os.path.isdir(
                os.path.join(self.__path, element)
            )
        ]
        simulated_years = [
            int(yearly_result_folder)
            for yearly_result_folder in [
                result_folder.split("_")[-1]
                for result_folder in results_folders
            ]
            if yearly_result_folder.isdigit()
        ]
        unique_simulated_years = list(set(simulated_years))
        unique_simulated_years.sort()
        return unique_simulated_years

    def write_csv_in_postprocess(
        self,
        data_to_write: pd.DataFrame,
        file_name: str
    ) -> None:
        self.create_postprocess_folder()
        data_to_write.to_csv(
            os.path.join(self.__postprocess_output_folder, file_name)
        )
        logging.info(f"Writing file {os.path.join(self.__postprocess_output_folder, file_name)}")

    def create_postprocess_folder(self) -> None:
        os.makedirs(self.__postprocess_output_folder, exist_ok=True)

    def copy_plot_config(self, plot_config_file_path: str) -> None:
        self.create_postprocess_folder()
        shutil.copyfile(
            src=plot_config_file_path,
            dst=os.path.join(
                self.__postprocess_output_folder,
                os.path.basename(plot_config_file_path)
            )
        )