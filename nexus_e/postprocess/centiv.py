""" Here should be re-implemented the following logic taken from postProcess.m:

    disp('---------------- CentIv -----------------')
        try
             cd(workingDir);
            % Generate output files for CentiV results with Latex using Cascades results.
            disp("Executing Generation")
            command = ['python Generation.py'...
               sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname) ];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Curtailments...")
            command = ['python Curtailments.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Capacity...")
            command = ['python Capacity.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing ElectricityPrice...")
            command = ['python ElectricityPrice.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Storage...")
            command = ['python Storage.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Revenue & Profit calculation")
            command = ['python revenue_profit.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing System Cost script")
            command = ['python system_costs.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Emission calculation")
            command = ['python emissions.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Demand calculation")
            command = ['python demand.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Cross Country Flow script")
            command = ['python cross_country_flow.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

            disp("Executing Power Flow Map script")
            command = ['python power_flow_map.py'...
                sprintf(' --simuname=%s', simuName),   ...
                sprintf(' --DBname=%s', DBname)];
            system(ACTIVATE_ENVIRONMENT + command);

        catch
            disp("FAILED to process CentIv results.")
        end
"""
import logging
import os

import nexus_e.config as config
import Capacity as postprocess_capacity
import cross_country_flow as postprocess_cross_country_flow
import Curtailments as postprocess_curtailments
import demand as postprocess_demand
import ElectricityPrice as postprocess_electricity_price
import emissions as postprocess_emissions
import Generation as postprocess_generation
import power_flow_map as postprocess_power_flow_map
import revenue_profit as postprocess_revenue_profit
import Storage as postprocess_storage
import system_costs as postprocess_system_costs

class CentivPostprocess():

    def __init__(self, settings: config.Config):
        self.settings = settings

    def run(self):
        logging.info("Start of CentIv postprocess")

        # The current directory should be the base nexus-e-framework folder.
        # We should get rid of this directory navigation in the future by
        # making the executed scripts usable with any path.
        current_directory = os.getcwd()
        postprocess_scripts_directory = os.path.join(
            "Shared", "resultPostProcess"
        )
        os.chdir(postprocess_scripts_directory)

        try:
            logging.info("Executing Demand calculation")
            postprocess_demand.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
            
            logging.info("Executing Generation...")
            postprocess_generation.main(
                simulation=self.settings.results.simulation_folder
            )
            logging.info("DONE")
            
            logging.info("Executing Curtailments...")
            postprocess_curtailments.main(
                simulation=self.settings.results.simulation_folder
            )
            logging.info("DONE")
            
            logging.info("Executing Capacity...")
            postprocess_capacity.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
                
            logging.info("Executing ElectricityPrice...")
            postprocess_electricity_price.main(
                simulation=self.settings.results.simulation_folder,
                single_electric_node=self.settings.modules.commons.single_electric_node,
            )
            logging.info("DONE")
            
            logging.info("Executing Storage...")
            postprocess_storage.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
            
            logging.info("Executing Revenue & Profit calculation")
            postprocess_revenue_profit.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
            
            logging.info("Executing System Cost script")
            postprocess_system_costs.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
            
            logging.info("Executing Emission calculation")
            postprocess_emissions.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
            
            logging.info("Executing Cross Country Flow script")
            postprocess_cross_country_flow.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")
            
            logging.info("Executing Power Flow Map script")
            postprocess_power_flow_map.main(
                simulation=self.settings.results.simulation_folder,
                database=self.settings.scenario.original_name,
                host=self.settings.input_database_server.host,
                user=self.settings.input_database_server.user,
                password=self.settings.input_database_server.password
            )
            logging.info("DONE")

            logging.info("End of CentIv postprocess")

        finally:
            os.chdir(current_directory)
        