import logging
import os
import shutil
import pandas as pd
import pathlib

import nexus_e.config as config
from .centiv import CentivPostprocess
from .cascades import CascadesPostprocess
from .legacy import moveToMysql

class PostProcess:
    def __init__(self, settings: config.Config):
        self.__settings = settings
        self.__simulation_results = SimulationResults(
            results_folder = self.__settings.results.base_folder,
            simulation_name = self.__settings.results.simulation_folder
        )

    def run(self):
        self.write_metadata()
        if os.path.exists(self.__settings.results.plot_config_file_path):
            self.__simulation_results.copy_plot_config(
                self.__settings.results.plot_config_file_path
            )
        if self.__settings.postprocess.centiv:
            centiv_postprocess = CentivPostprocess(
                self.__settings,
                self.__simulation_results.path
            )
            centiv_postprocess.run()
        if self.__settings.postprocess.cascades:
            cascades_postprocess = CascadesPostprocess(
                self.__settings,
                self.__simulation_results.postprocess_path
            )
            cascades_postprocess.run()
        if self.__settings.postprocess.move_to_mysql:
            self.move_to_mysql()
        logging.info("Postprocess finished")

    def write_metadata(self):
        metadata = self.__simulation_results.get_metadata(
            scenario_description = self.__settings.scenario.description
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
            simulation_execution_date = self.__settings.simulation.execution_date,
            scenario = self.__settings.scenario.output_name,
            webviewer_version = "results",
            host=self.__settings.output_database_server.host,
            port=self.__settings.output_database_server.port,
            user=self.__settings.output_database_server.user,
            password=self.__settings.output_database_server.password
        )
        logging.info("DONE")
        

class SimulationResults():

    def __init__(self, results_folder: str, simulation_name: str):
        self.__results_folder = results_folder
        self.__simulation_name = simulation_name
        self.__path = os.path.join(
            pathlib.Path(),
            self.__results_folder,
            self.__simulation_name
        )
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

    def get_metadata(self, scenario_description: str) -> pd.DataFrame:
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
        scenario_short_name = self.__simulation_name
        output = pd.DataFrame.from_dict(
            {
                # "simulation_submission_time": submission_time,
                "reference_year": "2015", # could be left empty
                "simulated_years": ",".join(map(str, simulated_years)),
                "scenario_description": scenario_description,
                "scenario_short_name": scenario_short_name
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
        
    
def main():
    settings = config.load()
    post_process = PostProcess(settings)
    post_process.run()

if __name__ == "__main__":
    main()