import logging
import os
import shutil
import pandas as pd
import pathlib

import nexus_e.config as config
from nexus_e.postprocess.centiv import CentivPostprocess
from nexus_e.postprocess.cascades import CascadesPostprocess
import moveToMysql

class PostProcess:
    def __init__(self, settings: config.Config):
        self.settings = settings
        self.simulation_results = SimulationResults(
            results_folder = self.settings.results.base_folder,
            simulation_name = self.settings.results.simulation_folder
        )

    def run(self):
        self.write_metadata()
        if os.path.exists(self.settings.results.plot_config_file_path):
            self.simulation_results.copy_plot_config(
                self.settings.results.plot_config_file_path
            )
        if self.settings.postprocess.centiv:
            centiv_postprocess = CentivPostprocess(self.settings)
            centiv_postprocess.run()
        if self.settings.postprocess.cascades:
            cascades_postprocess = CascadesPostprocess(self.settings)
            cascades_postprocess.run()
        if self.settings.postprocess.move_to_mysql:
            self.move_to_mysql()
        logging.info("Postprocess finished")

    def write_metadata(self):
        metadata = self.simulation_results.get_metadata(
            scenario_description = self.settings.scenario.description
        )
        self.simulation_results.write_csv_in_postprocess(
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
        original_directory = os.getcwd()
        postprocess_scripts_directory = os.path.join(
            "Shared", "resultPostProcess"
        )
        os.chdir(postprocess_scripts_directory)
        try:
            logging.info("Moving results to the output database")
            moveToMysql.main(
                simulation = self.settings.results.simulation_folder,
                simulation_execution_date = self.settings.simulation.execution_date,
                scenario = self.settings.scenario.output_name,
                webviewer_version = "results",
                host=self.settings.output_database_server.host,
                port=self.settings.output_database_server.port,
                user=self.settings.output_database_server.user,
                password=self.settings.output_database_server.password
            )
            logging.info("DONE")
        finally:
            os.chdir(original_directory)
        

class SimulationResults():

    def __init__(self, results_folder: str, simulation_name: str):
        nexus_e_framework_root_folder = (
            pathlib.Path(__file__).parent.parent.parent.resolve()
        )
        self.__results_folder = results_folder
        self.__simulation_name = simulation_name
        self.__simulation_results_path = os.path.join(
            nexus_e_framework_root_folder,
            self.__results_folder,
            self.__simulation_name
        )
        self.__postprocess_output_folder = os.path.join(
            "Shared",
            "resultPostProcess",
            "Outputs",
            self.__simulation_name
        )

    def get_metadata(self, scenario_description: str) -> pd.DataFrame:
        """
            Re-implementation of Shared/resultPostProcess/webviewMetadat.py

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
        if not os.path.isdir(self.__simulation_results_path):
            return []
        results_folders = [
            element 
            for element in os.listdir(self.__simulation_results_path)
            if os.path.isdir(
                os.path.join(self.__simulation_results_path, element)
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