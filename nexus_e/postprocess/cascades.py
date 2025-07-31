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
import sys

sys.path.append(os.path.join("Shared", "resultPostProcess", "Dependencies", "grid_map"))
import latexToExcel as postprocess_cascades_latex_to_excel
import generateGridExpansionMap as postprocess_cascades_grid_expansion_map

import nexus_e.config as config

class CascadesPostprocess():

    def __init__(self, settings: config.Config):
        self.settings = settings

    def run(self):
        logging.info("Start of Cascades postprocess")

        # The current directory should be the base nexus-e-framework folder.
        # We should get rid of this directory navigation in the future by
        # making the executed scripts usable with any path.
        current_directory = os.getcwd()

        postprocess_scripts_directory = os.path.join(
            "Shared", "resultPostProcess"
        )
        os.chdir(postprocess_scripts_directory)

        try:
            logging.info("Executing Cascades latex to Excel...")
            postprocess_cascades_latex_to_excel.main()
            logging.info("DONE")

            logging.info("Executing grid expansion map generation...")
            postprocess_cascades_grid_expansion_map.main(
                simulation=self.settings.results.simulation_folder
            )
            logging.info("DONE")

        finally:
            os.chdir(current_directory)