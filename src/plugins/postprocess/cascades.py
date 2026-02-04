""" Here should be re-implemented the following logic taken from postProcess.m:

    disp('---------------- Cascades -----------------')
        try
            cd(workingDir);
            disp("Executing webviewCascades.m...")
            webviewCascades;
            % Generate line_coordination_YEAR.csv."
            command = 'python Dependencies/grid_map/latexToExcel.py';
            disp(command)
            system(ACTIVATE_ENVIRONMENT + command);
            % Generate grid expansion maps with Latex using Cascades results.
            command = ['python'...
                ' Dependencies/grid_map/generateGridExpansionMap.py'...
                ' --simu-name ' simuName];
            disp(command)
            system(ACTIVATE_ENVIRONMENT + command);
        catch
            disp("FAILED to process Cascades results.")
        end
"""
import logging
import os

from .legacy import latexToExcel as postprocess_cascades_latex_to_excel
from .legacy import generateGridExpansionMap as postprocess_cascades_grid_expansion_map

class CascadesPostprocess():

    def __init__(self, results_simulation_folder: str, postprocess_path: str):
        self.__results_simulation_fodler = results_simulation_folder
        self.__postprocess_path = postprocess_path

    def run(self):
        logging.info("Start of Cascades postprocess")

        # The current directory should be the base nexus-e-framework folder.
        # We should get rid of this directory navigation in the future by
        # making the executed scripts usable with any path.
        current_directory = os.getcwd()

        os.chdir(self.__postprocess_path)

        try:
            logging.info("Executing Cascades latex to Excel...")
            postprocess_cascades_latex_to_excel.main()
            logging.info("DONE")

            logging.info("Executing grid expansion map generation...")
            postprocess_cascades_grid_expansion_map.main(
                simulation=self.__results_simulation_fodler
            )
            logging.info("DONE")

        finally:
            os.chdir(current_directory)