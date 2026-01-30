from dataclasses import dataclass
import dataclasses
from pathlib import Path
import os, shutil
import pandas as pd
import numpy as np
import argparse
from pandas.errors import EmptyDataError
import pymysql
import json
import scipy.io
import copy
import datetime, glob
import csv

from .system_state import SystemState
from .gen_conventional_CH_invest_binary import ConventionalGeneratorsCHInvestBinary #used for both investment and operation of all non-nuclear conventional generators in CH (binary variable for investment decisions)
from .gen_conventional_CH_invest_continuous import ConventionalGeneratorsCHInvestContinuous #used for both investment and operation of all non-nuclear conventional generators in CH (continuous variable for investment decisions)
from .gen_nuclear_CH_invest_binary import NuclearGeneratorsCHInvestBinary #used for investments in candidate nuclear generators in CH (binary variable for investment decisions)
from .gen_nuclear_CH_invest_continuous import NuclearGeneratorsCHInvestCont #used for investments in candidate nuclear generators in CH (continuous variable for investment decisions)
from .gen_conventional_tight_reserves2 import ConventionalGeneratorsTightReserves  #used for operation of CH nuclear generators - MILP formulation (Unit Commitment)
from .gen_conventional_CH_dispatch_continuous import ConventionalGeneratorsCHDispatchContinuous #used for operation of CH nuclear generators in case LP formulation required
from .gen_conventional_linear import ConventionalGeneratorsLinear #used for all non-CH conventional generators
from .gen_renewable import RenewableGenerators
from .gen_hydro import HydroGenerators
from .gen_battery import BatteryStoragesExisting
from .gen_battery_invest import BatteryStoragesInvest
from .reserves import Reserves
from .dc_pf_trafo_flex import DC_PowerFlow_TrafoFlex
from .dc_pf_trafo_flex_expansion import DC_PowerFlow_TrafoFlex_Expansion
from .gen_invest import InvestGenerators
from .gen_H2DAC_invest import P2XInvest
from .gen_H2DAC_invest_empty import P2XInvest_Empty
from .gen_NET_invest import NETInvest
from .gen_NET_invest_empty import NETInvest_Empty
from .ext2int import ext2int
from .change_timeperiod_resolution import ChangeResolution
from .save_results import SaveResults
from .save_results import saveVarParDualsCsv, saveSelectedStats, saveMappingFiles
import time
import logging


@dataclass
class Config():
    # This is where defaults are mentioned.
    DB_host: str = "localhost"
    """Server on which database is hosted"""

    DB_name: str = "scenario_name"
    """Name of MySQL database"""
    
    DB_user: str = "user"
    """User Name for MySQL database"""
    
    DB_pwd: str = "password"
    """Password for MySQL database"""

    results_folder: str = "results"
    """Folder in which the results are stored"""
    
    idScenario: int = 5
    """ID of simulated scenario from MySQL database
    Represents the simulated year
    """

    ici_multiplier: float = 1.0
    """Multiplier for the investment costs - Input from Gemel"""
    
    vci_multiplier: float = 1.0
    """Multiplier for the variable costs - Input from Gemel"""
    
    foci_multiplier: float = 1.0
    """Multiplier for the fixed operational costs - Input from Gemel"""
    
    demandCH : float = 0
    """Total (yearly) Swiss demand in MWh - Input from Gemel"""
    
    targetRES: float | None = None
    """Total Swiss RES target in TWh"""
    
    targetRESPV: float = 0.0
    """Total Swiss RES target in TWh to be covered by solar rooftop PV installations"""
    
    targetH2: float = 0.0
    """Total Swiss H2 target in tonnes H2"""
    
    targetCH4: float = 0.0
    """Total Swiss CH4 target in GWh_th"""
    
    targetCO2: float = 0.0
    """Total Swiss CO2 target in tonnes CO2"""
    
    distIvRESproduction: float = 0.0
    """Total distribution level RES production in TWh - Input from distIv"""
    
    distIvPVinstalled: float = 0
    """Total installed PV capacity in MW - Input from distIv"""
    
    distivresults_directory: str = os.path.join(os.getcwd(), "output_jo")
    """Directory in which the output from DistIv is located"""
    
    abmresults_directory: str = os.path.join(os.getcwd(), "abm_dist_invest")
    """Directory in which the output from ABM is located"""
    
    cascadesresults_directory: str = os.path.join(os.getcwd(), "cascades_invest")
    """Directory in which the output from Cascades is located"""
    
    timeperiods: int = 8760
    """Number of simulated timeperiods"""
    
    battDIS: float = 0.00054
    """Battery self-discharge
    
    The default value is taken from:
    https://www.sciencedirect.com/science/article/pii/S0306261920307091
    """
    
    solarUP: float = 0.0080
    """Coeff for increasing RR up reserves due to newly installed solar capacities"""
    
    solarDOWN : float = 0.0085
    """Coeff for increasing RR down reserves due to newly installed solar capacities"""
    
    windUP : float = 0.0615
    """Coeff for increasing RR up reserves due to newly installed wind capacities"""
    
    windDOWN : float = 0.0654
    """Coeff for increasing RR down reserves due to newly installed wind capacities"""
    
    loadShedding_cost: float = 10000.0
    """Cost of load shedding in CHF/MWh"""
    
    maxdailyH2withdrawal_p2g2p: float = 0.062
    """Maximum daily withdrawal rate in [%] of H2 storage of P2G2P unit
    
    The default value is taken from:
    https://www.sciencedirect.com/science/article/pii/S0360319914021223?via%3Dihub
    """
    
    maxdailyH2injection_p2g2p: float = 0.037
    """Maximum daily injection rate in [%] of H2 storage of P2G2P unit
    
    The default value is taken from:
    https://www.sciencedirect.com/science/article/pii/S0360319914021223?via%3Dihub
    """
    
    CH4pmin: float = 0.45
    """Percentage of Pmax set for the minimum operating point of the methanation reactor"""
    
    alpha_ex_CentIv: float = 0
    """TSO-DSO Power flow limit is set at alpha_ex_CentIv * maximum load at that node."""
    
    duals_required: bool = True
    """Flag for whether we require re-solve fixing the investment variables to get the duals (1) or not (0)"""
    
    continvest_required: bool = True
    """Flag for whether we require continuous investments in candidate conventional units (1) or not (0)"""

    continvestnuclear_required: bool = True
    """Flag for whether we require continuous investments in candidate nuclear units (1) or not (0)"""

    contnuclear_required: bool = True
    """Flag for whether we require continuous operation of Swiss nuclear units (1) or not (0)"""
    
    equalexportsimports_required: bool = False
    """Flag for whether we require the total annual exports to equal the total annual imports"""
    
    tpResolution: int = 1
    """Resolution of simulated timeperiods"""
    
    disableREStarget: bool = False
    """Disable RES target for the simulation"""
    
    enableTSODSOlimit: bool = False
    """Enable TSO-DSO Power flow limit"""

    single_electric_node: bool = False
    """Flag for whether we want to run the simulation with a single electric node (True) or not (False, that is, with multiple electric nodes based on DC load flow)"""
    # If activated, baseMVA will be set to 1, because not needed!

    threads: int = 8
    """Number of threads to use for solving in gurobi"""

    min_nuclear_gen_lim: bool = True 
    """ if set true, a given minimum of generation is forced on new nuclear plants, if False, the generation may go as low as 0"""

    include_nuclear_in_RES_target : bool = False
    """ if set true, the generation from nuclear plants is included in the RES target accounting (e.g., in the 45 TWh target for 2050)"""

    max_LL_switch: float = 0.3
    """ For calibration purposes, adding X% of max lost load for other countries as back up capacity """


class DataImport(object):
    def __init__(self, timeperiods):
        self.timeperiods = timeperiods

        self.network_info = []
        
        self.generators = []
        self.generators_extra = []
        self.lines = []
        self.lines_Cascades = []
        self.buses = []
        self.transformers = []
        self.dsmshifting = []
        self.targets = []
        self.profiles = []

        self.generator_id = []
        self.line_id = []
        self.bus_id = []
        self.transformer_id = []

        #generation
        self.gens_busnodes = [] #contains time series data of gens (i.e. inflows, pv production, wind production, nuclear refueling schedule)

        #load
        self.loads_busnodes = [] #contains load time series data per bus
        self.emobilityloads_busnodes = [] #contains e-mobility load time series data per bus
        self.emobilityloadsEFlex_busnodes = [] #contains e-mobility daily maximum flexible energy time series data per bus
        self.emobilityloadsPUp_busnodes = [] #contains e-mobility hourly maximum upper power limit time series data per bus
        self.emobilityloadsPDown_busnodes = [] #contains e-mobility hourly minimum lower power limit time series data per bus
        self.heatpumploads_busnodes = [] #contains heat pump load time series data per bus
        self.heatpumploadsPMax_busnodes = [] #contains heatpump maximum power load (i.e., installed capacity)
        self.heatpumploadsECumulMax_busnodes = [] #contains heatpump hourly cummulative energy upper limit time series data per bus
        self.heatpumploadsECumulMin_busnodes = [] #contains heatpump hourly cummulative energy lower limit time series data per bus
        self.HPFlexiblePercentage = 0.33 # 1 = 100% flexibility, 0 = 0% flexibility (all load is fixed) - hard coded but should be an input
        self.H2loads_busnodes = [] #contains H2 load time series data per bus

        self.original_load = {}
        self.adjusted_emobilityload = {}
        self.adjusted_heatpumpload = {}
        self.adjusted_H2electrolyzerload = {}
        self.adjusted_baseload = {}

        #misc
        self.nuclear_availability_timeseries = {} #contains time series with refueling schedule of nuclear reactors (per reactor)
        self.reserves_timeseries = [] #contains time series data for up/down primary, sec, tertiary reserves required in CH
        
        #timeseries from DistIv (if available)
        self.residual_load_DistIv = {} # residual load from DistIv to be covered by CentIv
        self.residual_reserve_DistIv_req = [] #residual reserve requirement from DistIv
        self.generation_DistIv = {} #electricity generated at distribution level (to be subtracted from load in database)
        self.distivinj_busnodes = [] #contains distIv injection time series data per bus
        self.generation_DistIv_EXCEL = {}

    def MySQLConnect(self, host, database, user, password):
        """
        Connect to MySQL server
        """
        self.conn = pymysql.connect(host=host, database=database, user=user, password=password)
    
    def LoadDistIvResults_EXCEL(self, directory):
        """load DistIv results from excel files"""
        #load DistIv residual demand data
        fn = os.path.join(directory, "Hourly_Nodal_Residual_Demand.xlsx")
        if os.path.exists(fn): 
            print('Loading %s...' % (fn)) 
            data =  pd.ExcelFile(fn).parse('Sheet1', header=[0], skiprows=[1,2])
            data.columns = data.columns.astype(int)
            data.astype(float)
            self.residual_load_DistIv = data.to_dict()
            print('Done')
        else: 
            print('{} file not found'.format(fn))
        #load DistIv residual FRR reserves data
        fn = os.path.join(directory, "Residual_FRR_System_Reserves.xlsx")
        if os.path.exists(fn):
            print('Loading %s...' % (fn))                                                                                                                                                             
            data = pd.ExcelFile(fn).parse('Sheet1', header=[0])
            data.astype(float)
            for row in data.iterrows():#, header=[0]) iterrows(): 
                self.residual_reserve_DistIv_req.append(row[1].to_dict())
            print('Done')
        else: 
            print('{} file not found'.format(fn))
        
    def LoadDistIvGeneration_EXCEL(self, directory):
        """load DistIv previous year PV generation from excel files"""
        fn = os.path.join(directory, "GenerationDistIv_hourly_CH.xlsx")
        if os.path.exists(fn): 
            print('Loading %s...' % (fn)) 
            data =  pd.ExcelFile(fn).parse('CH', header=[0], skiprows=[1,2])
            data.columns = data.columns.astype(int)
            data.astype(float)
            self.generation_DistIv_EXCEL = data.to_dict()
            print('Done')
        else: 
            print('{} file not found'.format(fn)) 
       
    def LoadDistIvResults_MAT(self, directory):
        """load DistIv results from .mat file"""
        fn = os.path.join(directory, "DGEPtoCGEP.mat")
        if os.path.exists(fn): 
            print('-----Load DistIv Results-------------------------')
            print('....Loading %s...' % (fn)) 
            data = scipy.io.loadmat(fn)
            contents = data['DGEPtoCGEP'] #in case we want to access the names saved in the struct #names = contents.dtype
            #load DistIv residual demand data
            residual_demand = contents[0,0]['Demand_nodal_res_MW']
            residual_demand_df = pd.DataFrame(residual_demand)
            header = residual_demand_df.iloc[0]
            residual_demand_df = residual_demand_df[1:]
            residual_demand_df.columns = header
            residual_demand_df.reset_index(drop=True, inplace=True)
            residual_demand_df.columns = residual_demand_df.columns.astype(int)
            residual_demand_df.astype(float)
            self.residual_load_DistIv = residual_demand_df.to_dict()
            #load DistIv residual FRR reserves data
            residual_upward_FRR = contents[0,0]['Reserve_res_upward_hourly_MWh']
            residual_downward_FRR = contents[0,0]['Reserve_res_downward_hourly_MWh']
            residual_upward_FRR_df = pd.DataFrame(residual_upward_FRR, columns=['FRRupReq'])
            residual_downward_FRR_df = pd.DataFrame(residual_downward_FRR, columns=['FRRdnReq'])
            residual_FRR_df = pd.concat([residual_upward_FRR_df,residual_downward_FRR_df], axis=1)
            for row in residual_FRR_df.iterrows():
                self.residual_reserve_DistIv_req.append(row[1].to_dict())
            #load DistIv generation data
            distIv_production = contents[0,0]['Injection_nodal_MW']
            distIv_production_df = pd.DataFrame(distIv_production)
            header_distiv_production = distIv_production_df.iloc[0]
            distIv_production_df = distIv_production_df[1:]
            distIv_production_df.columns = header_distiv_production
            distIv_production_df.reset_index(drop=True, inplace=True)
            distIv_production_df.columns = distIv_production_df.columns.astype(int)
            distIv_production_df.astype(float)
            self.generation_DistIv = distIv_production_df.to_dict()
            print('....Done')
            print('-------------------------------------------------')
            print('')
        else: 
            print('-----Load DistIv Results-------------------------')
            print('....{} file not found'.format(fn))
            print('-------------------------------------------------')
            print('')
    
    def LoadABMResults_MAT(self, directory):
        """load ABM results from .mat file"""
        fn = os.path.join(directory, "ABMtoCGEP.mat")
        if os.path.exists(fn): 
            print('-----Load DistAB Results-------------------------')
            print('....Loading %s...' % (fn)) 
            data = scipy.io.loadmat(fn)
            contents = data['ABMtoCGEP'] #in case we want to access the names saved in the struct #names = contents.dtype
            #load ABM residual demand data
            residual_demand = contents[0,0]['Demand_nodal_res_MW']
            residual_demand_df = pd.DataFrame(residual_demand)
            header = residual_demand_df.iloc[0]
            residual_demand_df = residual_demand_df[1:]
            residual_demand_df.columns = header
            residual_demand_df.reset_index(drop=True, inplace=True)
            residual_demand_df.columns = residual_demand_df.columns.astype(int)
            residual_demand_df.astype(float)
            self.residual_load_DistIv = residual_demand_df.to_dict()
            #load ABM residual FRR reserves data
            residual_upward_FRR = contents[0,0]['Reserve_res_upward_hourly_MWh']
            residual_downward_FRR = contents[0,0]['Reserve_res_downward_hourly_MWh']
            residual_upward_FRR_df = pd.DataFrame(residual_upward_FRR, columns=['FRRupReq'])
            residual_downward_FRR_df = pd.DataFrame(residual_downward_FRR, columns=['FRRdnReq'])
            residual_FRR_df = pd.concat([residual_upward_FRR_df,residual_downward_FRR_df], axis=1)
            for row in residual_FRR_df.iterrows():
                self.residual_reserve_DistIv_req.append(row[1].to_dict())
            #load ABM generation data
            distIv_production = contents[0,0]['Injection_nodal_MW']
            distIv_production_df = pd.DataFrame(distIv_production)
            header_distiv_production = distIv_production_df.iloc[0]
            distIv_production_df = distIv_production_df[1:]
            distIv_production_df.columns = header_distiv_production
            distIv_production_df.reset_index(drop=True, inplace=True)
            distIv_production_df.columns = distIv_production_df.columns.astype(int)
            distIv_production_df.astype(float)
            self.generation_DistIv = distIv_production_df.to_dict()
            print('....Done')
            print('-------------------------------------------------')
            print('')
        else: 
            print('-----Load DistAB Results-------------------------')
            print('....{} file not found'.format(fn))
            print('-------------------------------------------------')
            print('')

    def LoadCascadesResults_CSV(self, directory):
        """load the input files from Cascades
        -- priorityList: this table gives the candidate lines for the CentIv transmission expansion planning optimization
        -- expPlanTable: this table gives all lines that have been built in either a previous run of CentIv or in Cascades and need to be considered as built"""
        fn = os.path.join(directory, "priorityList.csv")
        if os.path.exists(fn): 
            print('....Loading %s...' % (fn)) 
            try:
                priorityListTable = pd.read_csv(fn)
                if priorityListTable.empty == True:
                    print("File is empty.")
                else:
                    for id, row in priorityListTable.iterrows():
                        row['Candidate'] = 1
                        self.lines_Cascades.append(row.to_dict())
            except EmptyDataError: 
                print("No columns to parse from file.")
        fn1 = os.path.join(directory, "expPlanTable.csv")
        if os.path.exists(fn1): 
            print('....Loading %s...' % (fn1)) 
            try:
                expPlanTable = pd.read_csv(fn1)
                if expPlanTable.empty == True:
                    print("File is empty.")
                else:
                    for id, row in expPlanTable.iterrows():
                        row['Candidate'] = 0
                        self.lines_Cascades.append(row.to_dict())
            except EmptyDataError: 
                print("No columns to parse from file.")

    def GetScenario(self, config: Config):
        """
        Fetch the required data from MySQL Nexus database
        Input: id of the scenario
        Output: all empty lists from  def __init__  are filled in with the required data for the given scenario
        """
        #0.Fetch basic info from scenarioconfiguration table
        #0.0Scenario Name
        cursor = self.conn.cursor()
        sql_select_query_scenName = """
           SELECT name FROM
           scenarioconfiguration WHERE idScenario = %s
        """
        cursor.execute(sql_select_query_scenName, (config.idScenario, ))
        scenName = cursor.fetchall()
        self.ScenarioName = scenName[0][0]
        cursor.close()
        print('-----Scenario Info-------------------------------')
        print('')
        print('    1) Scenario-Config name is %s' %(self.ScenarioName))
        print('-------------------------------------------------')
        print('')
        #0.1Scenario Year
        cursor = self.conn.cursor()
        sql_select_query_scenYear = """
           SELECT Year FROM
           scenarioconfiguration WHERE idScenario = %s
        """
        cursor.execute(sql_select_query_scenYear, (config.idScenario, ))
        logging.debug("In DataImport, executed cursor.execute(sql_select_query_scenYear, (config.idScenario, ))")
        scenYear = cursor.fetchall()
        self.ScenarioYear = scenYear[0][0]
        cursor.close()

        #0.2Pull target configurations
        cursor = self.conn.cursor()
        cursor.callproc('getSwissAnnualTargets', (config.idScenario, ))
        logging.debug("In DataImport, executed cursor.callproc('getSwissAnnualTargets', (config.idScenario, ))")
        TargetsInfo = list(cursor.fetchall())
        logging.debug("In DataImport, fetched TargetsInfo from cursor.fetchall()")
        self.col_names_targets = [field[0] for field in cursor.description]
        logging.debug("In DataImport, set self.col_names_targets to cursor.description")
        cursor.close()
        logging.debug("In DataImport, closed cursor after fetching targets")

        #0.3.Store the targets in list self.targets
        Targets_DataFrame = pd.DataFrame(data=TargetsInfo, columns=self.col_names_targets)
        for row in Targets_DataFrame.iterrows():
            self.targets.append(row[1].to_dict())
        logging.debug("In DataImport, iterated through Targets_DataFrame to fill self.targets")
        
        for target in self.targets:
            if target["TargetName"] == "H2_Demand_inCH_Target":
                config.targetH2 = target['Value']
                self.H2profileId = target['idProfile']
            elif target["TargetName"] == "Renew_Gen_Target":
                if config.timeperiods != 8760: #if not annual time resolution
                    if config.targetRES is None:
                        config.targetRES = target['Value']
                        print(f"Info: Using RES target from database: {config.targetRES}")
                    else:
                        print(f"Info: Using RES target from config: {config.targetRES} (ignoring database value)") 
                    logging.warning("In DataImport, targetRES is set to 0 because timeperiods != 8760 (timeperiods is e.g. 168 for debugging)")
                else:
                    #if annual time resolution, then set targetRES to the value from the database
                    config.targetRES = target['Value']    
            elif target["TargetName"] == "PV-Roof_Gen_Target":
                if config.timeperiods != 8760: #if not annual time resolution
                    config.targetRESPV = 0
                    logging.warning("In DataImport, targetRESPV is set to 0 because timeperiods != 8760 (timeperiods is e.g. 168 for debugging)")
                else:
                    config.targetRESPV = target['Value']
            elif target["TargetName"] == "Gas-Syn_Demand_inCH_Target":
                config.targetCH4 = target['Value']
                self.CH4profileId = target['idProfile']
            elif target["TargetName"] == "CO2-capt_inCH_Target":
                config.targetCO2 = target['Value']
            elif target["TargetName"] == "NetImport_WinterHalf_Target":
                if config.timeperiods != 8760: #if not annual time resolution
                    config.winterNetImport = 0
                    logging.warning("In DataImport, winterNetImport target is set to 0 because timeperiods != 8760 (timeperiods is e.g. 168 for debugging)")
                else:
                    config.winterNetImport = target["Value"]
            elif target["TargetName"] == "NetImport_Annual_Target":
                config.annualNetImport = target["Value"]
        logging.debug("In DataImport, loaded targets from database")
                #write a warning that there is a target that is not recorded ... 
        print('-----Data for User Defined Targets---------------')
        print('    2.0) RES production target from db is %d' %(config.targetRES)+' TWh')
        print('    2.1) PV Rooftop production target from db is %d' %(config.targetRESPV)+' TWh')
        print('    2.2) SynGas production target from db is %d' %(config.targetCH4)+' GWh_th')
        print('    2.3) H2 production target from db is %d' %(config.targetH2)+' tonne_H2')
        print('    2.4) CO2 capture target from db is %d' %(config.targetCO2)+' tonne_CO2')
        print('    2.5) Annual Net Import target from db is %d' %(config.annualNetImport)+' TWh')
        print('    2.6) Winter Net Import target from db is %d' %(config.annualNetImport)+' TWh')
        #print('....Warning: CO2 capture target is currently not supported.')
        print('....Warning: CH4 (non-domestic) and H2 (domestic) production targets are currently not supported.')
        print('-------------------------------------------------')
        print('')
        logging.debug("In DataImport, printed user defined targets")

        #1.Get the Network Configuration Id for this Scenario
        cursor = self.conn.cursor()
        sql_select_query_idNetwork = """
           SELECT idNetworkConfig FROM
           scenarioconfiguration WHERE idScenario = %s
        """
        cursor.execute(sql_select_query_idNetwork, (config.idScenario, ))
        networkId = cursor.fetchall()
        self.NetworkConfig = networkId[0][0]
        cursor.close()
        logging.debug("In DataImport, executed cursor.execute(sql_select_query_idNetwork, (config.idScenario, ))")
        #2.Get the Network for the corresponding Network Configuration Id
        cursor = self.conn.cursor()
        cursor.callproc('getBranchData', (self.NetworkConfig, ))
        NetworkInfo = list(cursor.fetchall())
        self.col_names_Branch = [field[0] for field in cursor.description]
        cursor.close()
        logging.debug("In DataImport, executed cursor.callproc('getBranchData', (self.NetworkConfig, ))")
        #3.Store the network in list self.lines/Store the network ids in list self.line_id
        Network_DataFrame = pd.DataFrame(data=NetworkInfo, columns=self.col_names_Branch)
        for row in Network_DataFrame.iterrows():
            #self.line_id.append(len(self.lines))
            self.lines.append(row[1].to_dict())

        for id, row in enumerate(self.lines):
            row.update({u'expFlag':0}) #only necessary for interface with Cascades
            row.update({u'Rank':0}) #only necessary for interface with Cascades
        logging.debug("In DataImport, iterated through Network_DataFrame to fill self.lines and set expFlag and Rank to 0")
        #4.Get additional details for the network (i.e. name of network, year, baseMVA)
        cursor = self.conn.cursor()
        sql_select_query_idNetworkInfo = """
            SELECT * FROM
            networkconfiginfo WHERE idNetworkConfig = %s
        """
        cursor.execute(sql_select_query_idNetworkInfo, (self.NetworkConfig, ))
        NetworkAdditionalInfo = list(cursor.fetchall())
        self.col_names_NetworkInfo = [field[0] for field in cursor.description]
        cursor.close()
        #5.Store the details for the network in list self.network_info
        NetworkAdditionalInfo_DataFrame = pd.DataFrame(data=NetworkAdditionalInfo, columns=self.col_names_NetworkInfo)
        for row in NetworkAdditionalInfo_DataFrame.iterrows():
            self.network_info.append(row[1].to_dict())
        #6.Get the Bus configuration for the corresponding Scenario Id
        cursor = self.conn.cursor()
        cursor.callproc('getBusData_v2', (config.idScenario, )) #getBusData
        BusInfo = list(cursor.fetchall())
        self.col_names_Bus = [field[0] for field in cursor.description]
        cursor.close()
        #7.Store the bus configuration in list self.buses/Store the bus ids in list self.bus_id
        BusInfo_DataFrame = pd.DataFrame(data=BusInfo, columns=self.col_names_Bus)
        for row in BusInfo_DataFrame.iterrows():
            self.buses.append(row[1].to_dict())
        for key,row in enumerate(self.buses):
            self.bus_id.append(row['idIntBus'])
        logging.debug("In DataImport, iterated through BusInfo_DataFrame to fill self.buses and self.bus_id")
        #8.Get the Generator Configuration Id for this Scenario
        cursor = self.conn.cursor()
        sql_select_query_idGens = """
           SELECT idGenConfig FROM
           scenarioconfiguration WHERE idScenario = %s
        """
        cursor.execute(sql_select_query_idGens, (config.idScenario, ))
        gensId = cursor.fetchall()
        self.GensConfig = gensId[0][0]
        cursor.close()
        #8.1...Include the Investment Cost, Variable Cost and Operational Cost Multipliers - these are inputs from Gemel
        for _,row in enumerate(self.generators):
            row['InvCost'] = row['InvCost'] * config.ici_multiplier
            row['FOM_Cost'] = row['FOM_Cost'] * config.foci_multiplier
            row['VOM_Cost'] = row['VOM_Cost'] * config.vci_multiplier
        #9.Get the Generators for the corresponding Generators Configuration Id
        cursor = self.conn.cursor()
        cursor.callproc('getGeneratorData', (self.GensConfig, ))
        GeneratorInfo = list(cursor.fetchall())
        self.col_names_Gens = [field[0] for field in cursor.description]
        cursor.close()
        #9.1.Store the generators in list self.generators
        Generators_DataFrame = pd.DataFrame(data=GeneratorInfo, columns=self.col_names_Gens)

        # if a single electric is to be considered, for CandidateUnit (=1), per Dispatchable generation UnitType, keep only one generator (remove the rest)
        # On the RES side (PV and Wind), we keep all generators because their capacity factor may differ
        if config.single_electric_node:
            # Step 1: Create a boolean mask for candidate, dispatchable units
            dispatchable_mask = (
                (Generators_DataFrame["CandidateUnit"] == 1) &
                (Generators_DataFrame["UnitType"] == "Dispatchable")
            )

            # Step 2: Group the filtered generators by Technology
            grouped = Generators_DataFrame[dispatchable_mask].groupby("Technology")

            # Step 3: Prepare to collect indices of duplicate rows (to drop)
            indices_to_drop = []

            # Step 4: Columns whose values should be summed and assigned to the kept row (this way we e.g., make sure total potential of investments is correct)
            cols_to_sum = ["Pmax", "Pmin", "Emax", "Emin"]

            # Step 5: Iterate through each technology group
            for tech, group in grouped:
                first_idx = group.index[0]  # Keep the first generator for each technology

                # Step 5.1: Sum selected columns and assign to the first row
                for col in cols_to_sum:
                    total = group[col].sum()
                    Generators_DataFrame.loc[first_idx, col] = total

                # Step 5.2: Mark the remaining generators in this group for removal
                indices_to_drop.extend(group.index[1:])

            # Step 6: Drop all the extra generator rows
            Generators_DataFrame = Generators_DataFrame.drop(index=indices_to_drop)

        # --- Optional: boost installed capacities in selected countries to avoid lost load (calibration) ---
        # Controlled by a switch value on the Config, e.g. config.max_LL_switch in {0, 0.5, 1}.
        # If present and > 0, we add factor * value (MW) to Pmax for matching GenName keys.
        # Countries with highest lost load are chosen, and some GasSC is added.
        try:
            factor = float(getattr(config, "max_LL_switch", 0.0))
        except Exception:
            factor = 0.0
        if factor and factor != 0.0:
            max_LL = {
                # "DE_Conv_GasSC": 110568,
                "DE_Conv_GasCC-CCS": 13333, # what CH_Conv_GasCC-CCS generated 
                "IT_Conv_GasCC-CCS": 13333,
                "AT_Conv_GasCC-CCS": 13333,
                "FR_Conv_GasCC-CCS": 13333,
                "UK_Conv_GasSC": 33130,
                # "NL_Conv_GasSC": 28917,
                "CZ_Conv_GasSC": 50000,  # 19631
                "PL_Conv_GasSC": 60000, # 19479
                "BE_Conv_GasSC": 33000, # 18947 was original peak lost load (5K peak lost load seen after 30% GasSC increase)
                # "ES_Conv_GasSC": 11627,
                "PT_Conv_GasSC": 9018,
                # "FI_Conv_GasSC": 8939,
                "DK_Conv_GasSC": 20000, # 7899
                "SE_Conv_GasSC": 12000, # 6977
                "HU_Conv_GasSC": 10000, # 4995
                # "AT_Conv_GasSC": 4921,
                "LT_Conv_GasSC": 3436,
                "EE_Conv_GasSC": 10000, # 3419
                "SK_Conv_GasSC": 10000, #2723
                "LV_Conv_GasSC": 10000, # 2411
                "RO_Conv_GasSC": 10000, #2212
                # "LU_Conv_GasSC": 1891,
                "HR_Conv_GasSC": 2000, # 491
                "RS_Conv_GasSC": 2000, #439
                "SI_Conv_GasSC": 1000, #377
                "BA_Conv_GasSC": 300, #70
                # "BG_Conv_GasSC": 98,
                "MT_Conv_GasSC": 500, # 90
            }
            add_map = pd.Series(max_LL)
            add_series = Generators_DataFrame["GenName"].map(add_map).fillna(0.0) * float(factor)
            # Record applied addition for traceability
            Generators_DataFrame["LL_added_MW"] = add_series
            # Apply to installed capacity (Pmax)
            logging.warning(f"In DataImport, boosting Pmax of selected generators by up to {factor*100:.1f}% of max lost load values for calibration.")
            Generators_DataFrame["Pmax"] = Generators_DataFrame["Pmax"].astype(float) + add_series.astype(float)
            logging.warning("In DataImport, investment options are deactivated as part of calibration")
            # remove certain technologies from the candidate list, in the calibration stage

            # Further calibrations
            # For all power generators that are of Technology "Battery-TSO", multiply the current value of Emax by 6
            Generators_DataFrame.loc[
                Generators_DataFrame["Technology"] == "Battery-TSO",
                "Emax",
            ] = Generators_DataFrame.loc[
                Generators_DataFrame["Technology"] == "Battery-TSO",
                "Emax",
            ].astype(float) * 6.0
            print("here")

            # Further calibrations
            # Mannually adjusting severla batteries, for the plants with names like keys in bat_new, apply the new Emax and Pmax values
            stor_new = {
                "PT_Stor_Battery-TSO": [("Emax", 6000)],
                "PL_Stor_Battery-TSO": [("Pmax", 5000), ("Pmin", -5000), ("Emax", 10000)],
                "DK_Stor_Battery-TSO": [("Emax", 6000)],
                "EE_Stor_Battery-TSO": [("Pmax", 1000), ("Pmin", -1000), ("Emax", 6000)],
                "PL_Hydro_Dam": [("Emax", 62000)],
                "CZ_Hydro_Dam": [("Emax", 111000)],
                "SI_Hydro_Dam": [("Emax", 1000)],
            }

            # for the plants with names like keys in stor_new, apply the new Emax and Pmax values
            for name, updates in stor_new.items():
                for col, new_val in updates:
                    Generators_DataFrame.loc[
                        Generators_DataFrame["GenName"] == name,
                        col,
                    ] = new_val

        # -------------------------------------------------------------------------------------------

        for row in Generators_DataFrame.iterrows():
            #self.generator_id.append(len(self.generators))
            self.generators.append(row[1].to_dict())
        self.generators = sorted(self.generators, key=lambda k: k['idGen'])
        #9.2.Get the extra units (P2G2P) for the corresponding Generators Configuration Id
        cursor = self.conn.cursor()
        cursor.callproc('getGeneratorData_Extra', (self.GensConfig, ))
        GeneratorInfoExtra = list(cursor.fetchall())
        self.col_names_GensExtra = [field[0] for field in cursor.description]
        cursor.close()
        #9.3.Store the extra generators in list self.generators_extra
        GeneratorsExtra_DataFrame = pd.DataFrame(data=GeneratorInfoExtra, columns=self.col_names_GensExtra)
        for row in GeneratorsExtra_DataFrame.iterrows():
            self.generators_extra.append(row[1].to_dict())
        #9.4.Append the additional info from self.generators_extra to self.generators
        for _, row in enumerate(self.generators):
            for _,row1 in enumerate(self.generators_extra):
                if row['idGen'] == row1['idGen']:
                    temp = row.copy()
                    row.update(row1)
                    row.update(temp)
        logging.debug("In DataImport, finished step 10")
        #11.Get a table with the production profile for each generator at the generator's internal bus node
        cursor = self.conn.cursor()
        sql_select_query_Gens_MergeWith_ProfileData1 = """
        CREATE TEMPORARY TABLE temp_tbl
        SELECT  (SELECT busdata.internalBusId
                FROM busdata
                where genconfiguration.idBus = busdata.idBus ) as idIntBus,
                gendata.idGen,
                genconfiguration.idProfile,
                gendata.GenName,
                gendata.Technology,
                genconfiguration.Pmax,
                genconfiguration.Pmin,
                genconfiguration.Qmax,
                genconfiguration.Qmin,
                genconfiguration.Emax,
                genconfiguration.Emin,
                genconfiguration.E_ini,
                gendata.UnitType,
                genconfiguration.CandidateUnit
        FROM gendata
        INNER JOIN genconfiguration ON gendata.idGen = genconfiguration.idGen where genconfiguration.idGenConfig = %s;
        """
        sql_select_query_Gens_MergeWith_ProfileData2 = """
        SELECT  temp_tbl.idIntBus,
                temp_tbl.idGen,
                temp_tbl.idProfile,
                temp_tbl.Technology,
                temp_tbl.GenName,
                temp_tbl.CandidateUnit,
                temp_tbl.Pmax,
                temp_tbl.Pmin,
                profiledata.name,
                profiledata.idProfile,
                profiledata.type,
                profiledata.unit,
                profiledata.timeSeries
        FROM temp_tbl LEFT JOIN profiledata
        ON temp_tbl.idProfile = profiledata.idProfile;
        """
        #query is with left join because not all generators have a profile associated with them, but we still need all the bus ids
        cursor.execute(sql_select_query_Gens_MergeWith_ProfileData1, (self.GensConfig, ))
        cursor.execute(sql_select_query_Gens_MergeWith_ProfileData2)
        Gens_MergeWith_ProfileDataInfo = list(cursor.fetchall())
        self.col_names_GensBusesWithRESProfiles = [field[0] for field in cursor.description]
        cursor.close()
        #12.Store the generators,corresponding bus ids and profiles in list self.gens_busnodes/store the generator ids in list self.generator_id
        GenBusConfigWithProfiles_DataFrame = pd.DataFrame(data=Gens_MergeWith_ProfileDataInfo, columns=self.col_names_GensBusesWithRESProfiles)

        # only keep the rows of GenBusConfigWithProfiles_DataFrame,  whose GenName is also mentioned in Generators_DataFrame's GenName column 
        GenBusConfigWithProfiles_DataFrame = GenBusConfigWithProfiles_DataFrame[GenBusConfigWithProfiles_DataFrame['GenName'].isin(Generators_DataFrame['GenName'])]
        for row in GenBusConfigWithProfiles_DataFrame.iterrows():
            self.gens_busnodes.append(row[1].to_dict())
        self.gens_busnodes = sorted(self.gens_busnodes, key=lambda k: k['idGen'])
        for key, row in enumerate(self.gens_busnodes):
            self.generator_id.append(key)
        #13.Get all profiles
        cursor = self.conn.cursor()
        sql_select_query_getProfileData = """
        SELECT * FROM profiledata;
        """
        cursor.execute(sql_select_query_getProfileData)
        ProfileData_Info = list(cursor.fetchall())
        self.col_names_ProfileData = [field[0] for field in cursor.description]
        cursor.close()
        #13.1 Store the profiles in list self.profiles
        Profiles_DataFrame = pd.DataFrame(data=ProfileData_Info, columns=self.col_names_ProfileData)
        for row in Profiles_DataFrame.iterrows():
            self.profiles.append(row[1].to_dict())
        #13.2Get the Load Configuration for the given Scenario Id
        cursor = self.conn.cursor()
        sql_select_query_idLoad = """
           SELECT idLoadConfig FROM
           scenarioconfiguration WHERE idScenario = %s
        """
        cursor.execute(sql_select_query_idLoad, (config.idScenario, ))
        loadId = cursor.fetchall()
        self.LoadConfig = loadId[0][0]
        cursor.close()
        
        logging.debug("In DataImport, finished step 13")
        #14.1Get the location of the loads (in terms of bus ids), the load profiles (time series) at each bus AND load shifting limits
        cursor = self.conn.cursor()

        # This query contains the basic information of the bus ID, name, country, demand share, and dsm bounds (for conventional load).
        sql_select_query_LoadConfig_MergeWith_ProfileData1 = """
        CREATE TEMPORARY TABLE temp_tbl1 SELECT  
		(SELECT busdata.idBus
                FROM busdata
                where loadconfiguration.idBus = busdata.idBus ) as idIntBus,
                
                (SELECT busdata.BusName
                FROM busdata
                where loadconfiguration.idBus = busdata.idBus ) as BusName,
                
                loaddata.idLoad,        
                (SELECT busdata.Country
                    FROM busdata
                    WHERE loadconfiguration.idBus = busdata.idBus ) AS Country, 
                
                loadconfiguration.DemandShare, # to remove, only necessary for nodal vs central check
                
                (SELECT centflexpotential.PowerShift_Hrly
                    FROM busdata
                    INNER JOIN centflexpotential on centflexpotential.Country = busdata.Country
                    WHERE loadconfiguration.idBus = busdata.idBus AND centflexpotential.flex_type = 'DSM_general' AND centflexpotential.Year = loadconfiginfo.year ) AS DSM_PowerShift_Hrly, # to remove, only necessary for nodal vs central check
                
                (SELECT centflexpotential.EnergyShift_Daily
                    FROM busdata
                    INNER JOIN centflexpotential on centflexpotential.Country = busdata.Country
                    WHERE loadconfiguration.idBus = busdata.idBus AND centflexpotential.flex_type = 'DSM_general' AND centflexpotential.Year = loadconfiginfo.year ) AS DSM_EnergyShift_Daily # to remove, only necessary for nodal vs central check
        FROM loaddata
        INNER JOIN loadconfiguration ON loaddata.idLoad = loadconfiguration.idLoad 
        INNER JOIN loadconfiginfo ON loadconfiguration.idLoadConfig = loadconfiginfo.idLoadConfig
        WHERE loadconfiguration.idLoadConfig = %s
        """
        # This query creates a table that includes (for the ocnventional loads) the consumptions profiles        
        sql_select_query_LoadConfig_MergeWith_ProfileData2 = """
        SELECT temp_tbl1.idIntBus,
                temp_tbl1.idLoad,
                temp_tbl1.DemandShare, # to remove, only necessary for nodal vs central check
                temp_tbl1.DSM_PowerShift_Hrly, # to remove, only necessary for nodal vs central check
                temp_tbl1.DSM_EnergyShift_Daily, # to remove, only necessary for nodal vs central check
                load_profiles.LoadType,
                load_profiles.unit,
                load_profiles.timeSeries
        FROM temp_tbl1 INNER JOIN load_profiles
        ON temp_tbl1.BusName = load_profiles.BusName
        WHERE load_profiles.idLoadConfig = %s
        AND load_profiles.LoadType = 'Conventional'; 
        """
        cursor.execute(sql_select_query_LoadConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        cursor.execute(sql_select_query_LoadConfig_MergeWith_ProfileData2,  (self.LoadConfig, ))
        LoadConfigWithProfilesInfo = list(cursor.fetchall())
        self.col_names_LoadProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.1.1Store the loads (in terms of bus ids) and profiles (time series) at each bus in list self.loads_busnodes
        LoadProfile_DataFrame = pd.DataFrame(data=LoadConfigWithProfilesInfo, columns=self.col_names_LoadProfile)
        for row in LoadProfile_DataFrame.iterrows():
            self.loads_busnodes.append(row[1].to_dict())
        #14.2Get the location of the e-mobility loads (in terms of bus ids) and the load profiles (time series) at each bus
        cursor = self.conn.cursor()
        sql_select_query_eMobilityLoadConfig_MergeWith_ProfileData1 = """
        SELECT temp_tbl1.idIntBus,
            temp_tbl1.idLoad,
            load_profiles.LoadType,
            load_profiles.unit,
            load_profiles.timeSeries
        FROM temp_tbl1 INNER JOIN load_profiles
        ON temp_tbl1.BusName = load_profiles.BusName
        WHERE load_profiles.idLoadConfig = %s
        AND load_profiles.LoadType = 'eMobility'; 
        """        
        #some buses might not be demand buses, so we need a query with left join and not an inner join
        cursor.execute(sql_select_query_eMobilityLoadConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        eMobilityLoadConfigWithProfilesInfo = list(cursor.fetchall())
        self.col_names_eMobilityLoadProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.2.1Store the e-mobility loads (in terms of bus ids) and profiles (time series) at each bus in list self.emobilityloads_busnodes
        eMobilityLoadProfile_DataFrame = pd.DataFrame(data=eMobilityLoadConfigWithProfilesInfo, columns=self.col_names_eMobilityLoadProfile)
        for row in eMobilityLoadProfile_DataFrame.iterrows():
            self.emobilityloads_busnodes.append(row[1].to_dict())
        
        #14.2.2Get the information of the e-mobility loads' energy flexibility 
        cursor = self.conn.cursor()
        sql_select_query_eMobilityLoadEFlexConfig_MergeWith_ProfileData1 = """
        SELECT temp_tbl1.idIntBus,
            temp_tbl1.idLoad,
            flex_profiles_ev.Parameter,
            flex_profiles_ev.unit,
            flex_profiles_ev.timeSeries
        FROM temp_tbl1 INNER JOIN flex_profiles_ev
        ON temp_tbl1.BusName = flex_profiles_ev.BusName
        WHERE flex_profiles_ev.idLoadConfig = %s
        AND flex_profiles_ev.Parameter = 'DailyShift_Max'; 
        """
    
        cursor.execute(sql_select_query_eMobilityLoadEFlexConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        eMobilityLoadConfigWithEFlexInfo = list(cursor.fetchall())
        self.col_names_eMobilityEFlexProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.2.2.1Store the e-mobility Energy Flexibility in list self.emobilityloadsEFlex_busnodes
        eMobilityLoadEFlexProfile_DataFrame = pd.DataFrame(data=eMobilityLoadConfigWithEFlexInfo, columns=self.col_names_eMobilityEFlexProfile)
        for row in eMobilityLoadEFlexProfile_DataFrame.iterrows():
            self.emobilityloadsEFlex_busnodes.append(row[1].to_dict())

        #14.2.3Get the information of the e-mobility loads' upwards power flexibility 
        cursor = self.conn.cursor()
        sql_select_query_eMobilityLoadPUpConfig_MergeWith_ProfileData1 = """
        SELECT temp_tbl1.idIntBus,
            temp_tbl1.idLoad,
            flex_profiles_ev.Parameter,
            flex_profiles_ev.unit,
            flex_profiles_ev.timeSeries
        FROM temp_tbl1 INNER JOIN flex_profiles_ev
        ON temp_tbl1.BusName = flex_profiles_ev.BusName
        WHERE flex_profiles_ev.idLoadConfig = %s
        AND flex_profiles_ev.Parameter = 'Demand_Max'; 
        """
        cursor.execute(sql_select_query_eMobilityLoadPUpConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        eMobilityLoadConfigWithPUpInfo = list(cursor.fetchall())
        self.col_names_eMobilityPUpProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.2.3.1Store the e-mobility upwards power flexibility in list self.emobilityloadsPUp_busnodes
        eMobilityLoadPUpProfile_DataFrame = pd.DataFrame(data=eMobilityLoadConfigWithPUpInfo, columns=self.col_names_eMobilityPUpProfile)
        for row in eMobilityLoadPUpProfile_DataFrame.iterrows():
            self.emobilityloadsPUp_busnodes.append(row[1].to_dict())

        #14.2.4Get the information of the e-mobility loads' downwards power flexibility 
        cursor = self.conn.cursor()
        sql_select_query_eMobilityLoadPDownConfig_MergeWith_ProfileData1 = """
        SELECT temp_tbl1.idIntBus,
            temp_tbl1.idLoad,
            flex_profiles_ev.Parameter,
            flex_profiles_ev.unit,
            flex_profiles_ev.timeSeries
        FROM temp_tbl1 INNER JOIN flex_profiles_ev
        ON temp_tbl1.BusName = flex_profiles_ev.BusName
        WHERE flex_profiles_ev.idLoadConfig = %s
        AND flex_profiles_ev.Parameter = 'Demand_Min'; 
        """
        cursor.execute(sql_select_query_eMobilityLoadPDownConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        eMobilityLoadConfigWithPDownInfo = list(cursor.fetchall())
        self.col_names_eMobilityPDownProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.2.4.1Store the e-mobility downwards power flexibility in list self.emobilityloadsPDown_busnodes
        eMobilityLoadPDownProfile_DataFrame = pd.DataFrame(data=eMobilityLoadConfigWithPDownInfo, columns=self.col_names_eMobilityPDownProfile)
        for row in eMobilityLoadPDownProfile_DataFrame.iterrows():
            self.emobilityloadsPDown_busnodes.append(row[1].to_dict())
        
        #14.3Get the location of the heat pump loads (in terms of bus ids) and the load profiles (time series) at each bus
        cursor = self.conn.cursor()
        sql_select_query_heatPumpLoadConfig_MergeWith_ProfileData1 = """
        SELECT temp_tbl1.idIntBus,
                temp_tbl1.idLoad,
                load_profiles.LoadType,
                load_profiles.unit,
                load_profiles.timeSeries
        FROM temp_tbl1 INNER JOIN load_profiles
        ON temp_tbl1.BusName = load_profiles.BusName
        WHERE load_profiles.idLoadConfig = %s
        AND load_profiles.LoadType = 'HeatPump';
        """
        #some buses might not be demand buses, so we need a query with left join and not an inner join
        cursor.execute(sql_select_query_heatPumpLoadConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        heatPumpLoadConfigWithProfilesInfo = list(cursor.fetchall())
        self.col_names_heatPumpLoadProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.3.1Store the heat pump loads (in terms of bus ids) and profiles (time series) at each bus in list self.heatpumploads_busnodes
        heatPumpLoadProfile_DataFrame = pd.DataFrame(data=heatPumpLoadConfigWithProfilesInfo, columns=self.col_names_heatPumpLoadProfile)
        for row in heatPumpLoadProfile_DataFrame.iterrows():
            self.heatpumploads_busnodes.append(row[1].to_dict())
        
        #14.3.2 Get the information of the heatpump loads' max power flexibility
        cursor = self.conn.cursor()
        sql_select_query_HeatPumpLoadPMaxConfig_MergeWith_ProfileData1 = """                 
        SELECT temp_tbl1.idIntBus,
                    temp_tbl1.idLoad,
                    flex_params_hp.Parameter,
                    flex_params_hp.unit,
                    flex_params_hp.value
            FROM temp_tbl1 INNER JOIN flex_params_hp
            ON temp_tbl1.BusName = flex_params_hp.BusName
            WHERE flex_params_hp.idLoadConfig = %s
            AND flex_params_hp.Parameter = 'PowerCapacity_Max'; 
        """
        cursor.execute(sql_select_query_HeatPumpLoadPMaxConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        HeatPumpLoadConfigWithPMaxinfo = list(cursor.fetchall())
        self.col_names_HeatPumpPMaxProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.3.2.1Store the heatpump max power flexibility in list self.heatpumploadsPMax_busnodes
        HeatPumpLoadPMaxProfile_DataFrame = pd.DataFrame(data=HeatPumpLoadConfigWithPMaxinfo, columns=self.col_names_HeatPumpPMaxProfile)
        for row in HeatPumpLoadPMaxProfile_DataFrame.iterrows():
            self.heatpumploadsPMax_busnodes.append(row[1].to_dict())

        #14.3.3 Get the information of the heatpump loads' max cumulative energy bound
        cursor = self.conn.cursor()
        sql_select_query_HeatPumpLoadECumulMaxConfig_MergeWith_ProfileData1 = """                 
        SELECT temp_tbl1.idIntBus,
            temp_tbl1.idLoad,
            flex_profiles_hp.Parameter,
            flex_profiles_hp.unit,
            flex_profiles_hp.timeSeries
            FROM temp_tbl1 INNER JOIN flex_profiles_hp
            ON temp_tbl1.BusName = flex_profiles_hp.BusName
            WHERE flex_profiles_hp.idLoadConfig = %s
            AND flex_profiles_hp.Parameter = 'EnergyCumulPerDay_Max';  
        """
        cursor.execute(sql_select_query_HeatPumpLoadECumulMaxConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        HeatPumpLoadConfigWithECumulMaxinfo = list(cursor.fetchall())
        self.col_names_HeatPumpECumulMaxProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.3.3.1Store the heatpump max cummulative energy in list self.heatpumploadsECumulMax_busnodes
        HeatPumpLoadECumulMaxProfile_DataFrame = pd.DataFrame(data=HeatPumpLoadConfigWithECumulMaxinfo, columns=self.col_names_HeatPumpECumulMaxProfile)
        for row in HeatPumpLoadECumulMaxProfile_DataFrame.iterrows():
            self.heatpumploadsECumulMax_busnodes.append(row[1].to_dict())

        #14.3.4 Get the information of the heatpump loads' min cumulative energy bound
        cursor = self.conn.cursor()
        sql_select_query_HeatPumpLoadECumulMinConfig_MergeWith_ProfileData1 = """                 
        SELECT temp_tbl1.idIntBus,
            temp_tbl1.idLoad,
            flex_profiles_hp.Parameter,
            flex_profiles_hp.unit,
            flex_profiles_hp.timeSeries
            FROM temp_tbl1 INNER JOIN flex_profiles_hp
            ON temp_tbl1.BusName = flex_profiles_hp.BusName
            WHERE flex_profiles_hp.idLoadConfig = %s
            AND flex_profiles_hp.Parameter = 'EnergyCumulPerDay_Min';  
        """
        cursor.execute(sql_select_query_HeatPumpLoadECumulMinConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        HeatPumpLoadConfigWithECumulMininfo = list(cursor.fetchall())
        self.col_names_HeatPumpECumulMinProfile = [field[0] for field in cursor.description]
        cursor.close()
        #14.3.4.1Store the heatpump min cummulative energy in list self.heatpumploadsECumulMin_busnodes
        HeatPumpLoadECumulMinProfile_DataFrame = pd.DataFrame(data=HeatPumpLoadConfigWithECumulMininfo, columns=self.col_names_HeatPumpECumulMinProfile)
        for row in HeatPumpLoadECumulMinProfile_DataFrame.iterrows():
            self.heatpumploadsECumulMin_busnodes.append(row[1].to_dict())

        #14.4Get the location of the H2 loads (in terms of bus ids) and the load profiles (time series) at each bus
        cursor = self.conn.cursor()
        sql_select_query_H2LoadConfig_MergeWith_ProfileData1 = """
        SELECT temp_tbl1.idIntBus,
                temp_tbl1.idLoad,
                load_profiles.LoadType,
                load_profiles.unit,
                load_profiles.timeSeries
        FROM temp_tbl1 INNER JOIN load_profiles
        ON temp_tbl1.BusName = load_profiles.BusName
        WHERE load_profiles.idLoadConfig = %s
        AND load_profiles.LoadType = 'Electrolysis'; 
        """
        #some buses might not be demand buses, so we need a query with left join and not an inner join
        cursor.execute(sql_select_query_H2LoadConfig_MergeWith_ProfileData1, (self.LoadConfig, ))
        H2LoadConfigWithProfilesInfo = list(cursor.fetchall())
        self.col_names_H2LoadProfile = [field[0] for field in cursor.description]
        cursor.close()
        
        #14.4.1Store the hydrogen loads (in terms of bus ids) and profiles (time series) at each bus in list self.H2loads_busnodes
        H2LoadProfile_DataFrame = pd.DataFrame(data=H2LoadConfigWithProfilesInfo, columns=self.col_names_H2LoadProfile)
        for row in H2LoadProfile_DataFrame.iterrows():
            self.H2loads_busnodes.append(row[1].to_dict())

        logging.debug("In DataImport, finished step 14")

        #15.Get the Transformer Data for the given Network Configuration Id
        cursor = self.conn.cursor()
        cursor.callproc('getTransformerData', (self.NetworkConfig, ))
        TransformerInfo = list(cursor.fetchall())
        self.col_names_TransformerInfo = [field[0] for field in cursor.description]
        cursor.close()
        #16.Store the transformer info in list self.transformers
        Transformer_DataFrame = pd.DataFrame(data=TransformerInfo, columns=self.col_names_TransformerInfo)
        for row in Transformer_DataFrame.iterrows():
            self.transformer_id.append(len(self.transformers))
            self.transformers.append(row[1].to_dict())
        for id, row in enumerate(self.transformers):
            row.update({u'LineName':row['TrafoName']})
            row.update({u'expFlag':0}) #only necessary for interface with Cascades
            row.update({u'Rank':0}) #only necessary for interface with Cascades
        #16.1.Append to trafo list any candidate trafos from Cascades
        if any(len(x) != 0 for x in self.lines_Cascades):
            print('....Incorporating input (transformer candidates) from Cascades...1/2')
            candidate_trafos = []
            for id, row in enumerate(self.transformers):
                for _, row1 in enumerate(self.lines_Cascades):
                    if row['TrafoName'] == row1['LineName']:
                        row_update = row.copy()
                        row_update.update({u'LineName':row['TrafoName']})
                        if row1['Candidate'] == 1: #we are in the priorityList table of Cascades
                            row_update['Candidate'] = 1                                                                                                                                                                                          
                            row_update['CandCost'] = (1 - row1['Rank'] * 0.1) * row_update['CandCost'] #adjust the candidate cost according to the priority ranking from Cascades (the rank contributes towards max 10% inv cost reduction)
                            row_update.update({u'expFlag': 0})
                            row_update.update({u'Rank': row1['Rank']})
                        elif row1['Candidate'] == 0: #we are in the expPlan table of Cascades
                            row_update['Candidate'] = 1
                            row_update['CandCost'] = (1 - row1['Rank'] * 0.1) * row_update['CandCost'] #adjust the candidate cost according to the priority ranking from Cascades (the rank contributes towards max 10% inv cost reduction)
                            row_update.update({u'expFlag': 1})
                            row_update.update({u'FixInv': row1['NewInvestment']})
                            row_update.update({u'Rank': row1['Rank']})
                        candidate_trafos.append(row_update)
            self.transformers = self.transformers + candidate_trafos #candidate trafos at the end of the trafo list
        
        #17.Get hourly system reserve timeseries  
        cursor = self.conn.cursor()   
        sql_select_query_HourlySystemReserves = """
        SELECT name, timeSeries FROM 
            profiledata WHERE type = 'ReserveReq'
        """
        cursor.execute(sql_select_query_HourlySystemReserves, )
        ReservesInfo = list(cursor.fetchall())
        self.col_names_ReservesInfo = [field[0] for field in cursor.description]
        cursor.close()
        #18.Store hourly system reserve in list self.reserves_timeseries
        Reserves_DataFrame = pd.DataFrame(data=ReservesInfo, columns=self.col_names_ReservesInfo)
        for row in Reserves_DataFrame.iterrows():
            self.reserves_timeseries.append(row[1].to_dict())
        #19.Adjustments of line parameters
        #we don't model transformer active power losses
        for _, row in enumerate(self.transformers):
            row['r'] = 0
        #all lines have a tap ratio of 1
        for _, row in enumerate(self.lines):
            row.update({u'tapRatio':1})
        #19.1.Append to line list any candidate lines from Cascades
        if any(len(x) != 0 for x in self.lines_Cascades):
            print('....Incorporating input (transmission lines candidates) from Cascades...2/2')
            candidate_lines = []
            for id, row in enumerate(self.lines):
                for _, row1 in enumerate(self.lines_Cascades):
                    if row['LineName'] == row1['LineName']:
                        row_update = row.copy()
                        if row1['Candidate'] == 1: #we are in the priorityList table of Cascades
                            row_update['Candidate'] = 1 
                            row_update['CandCost'] = (1 - row1['Rank'] * 0.1) * row_update['CandCost'] #adjust the candidate cost according to the priority ranking from Cascades (the rank contributes towards max 10% inv cost reduction)
                            row_update.update({u'expFlag': 0})
                            row_update.update({u'Rank': row1['Rank']})
                        elif row1['Candidate'] == 0: #we are in the expPlan table of Cascades
                            row_update['Candidate'] = 1
                            row_update['CandCost'] = (1 - row1['Rank'] * 0.1) * row_update['CandCost'] 
                            row_update.update({u'expFlag': 1})
                            row_update.update({u'FixInv': row1['NewInvestment']})
                            row_update.update({u'Rank': row1['Rank']})
                        candidate_lines.append(row_update)
            self.lines = self.lines + candidate_lines #candidate lines at the end of the line list
        self.lines = self.lines + self.transformers #all lines and transformers in one list of dicts
        for i in range(len(self.lines)):
            self.line_id.append(i)

        logging.debug("In DataImport, finished step 19")

        #20.Get the power injections from DistIv    
        cursor = self.conn.cursor()
        sql_select_query_DistivInj1 = """
        CREATE TEMPORARY TABLE temp_tbl5
        SELECT  busdata.internalBusId,
                busdata.BusName,
                (SELECT busconfiguration.idDistProfile
                FROM busconfiguration
                where busdata.idBus = busconfiguration.idBus AND busconfiguration.idNetworkConfig = %s) as idDistProfile
        FROM busdata
        INNER JOIN busconfiguration ON busdata.idBus = busconfiguration.idBus
        INNER JOIN scenarioconfiguration ON busconfiguration.idNetworkConfig = scenarioconfiguration.idNetworkConfig WHERE scenarioconfiguration.idScenario = %s;
        """
        sql_select_query_DistivInj2 = """
        SELECT  temp_tbl5.internalBusId,
                temp_tbl5.BusName,
                temp_tbl5.idDistProfile,
                distprofiles.idDistProfile,
                distprofiles.timeSeries
        FROM temp_tbl5 LEFT JOIN distprofiles
        ON temp_tbl5.idDistProfile = distprofiles.idDistProfile;
        """
        cursor.execute(sql_select_query_DistivInj1, (self.NetworkConfig, config.idScenario))
        cursor.execute(sql_select_query_DistivInj2)
        DistivGenerationProfilesInfo = list(cursor.fetchall())
        self.col_names_DistivGenerationProfiles = [field[0] for field in cursor.description]
        cursor.close()
        #21.Store the distiv injections (in terms of bus ids) and profiles (time series) at each bus in list self.distivinj_busnodes
        DistIvProfile_DataFrame = pd.DataFrame(data=DistivGenerationProfilesInfo, columns=self.col_names_DistivGenerationProfiles)
        for row in DistIvProfile_DataFrame.iterrows():
            self.distivinj_busnodes.append(row[1].to_dict())
        #22.Convert possibly non-consecutive external bus numbers to consecutive internal bus numbers
        bus_id_remap = ext2int()
        def remap_bus_id(k, v):
            if k in ['internalBusId', 'idIntBus', 'idFromBus', 'idToBus']:
                return bus_id_remap.remap_id(v)
            return v
        self.bus_id = [bus_id_remap.remap_id(b) for b in self.bus_id]
        self.buses = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.buses]
        self.generators = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.generators]
        self.lines = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.lines]
        self.transformers = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.transformers]
        self.gens_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.gens_busnodes]
        self.distivinj_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.distivinj_busnodes]
        self.loads_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.loads_busnodes]
        self.emobilityloads_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.emobilityloads_busnodes]
        self.emobilityloadsEFlex_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.emobilityloadsEFlex_busnodes]
        self.emobilityloadsPUp_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.emobilityloadsPUp_busnodes]
        self.emobilityloadsPDown_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.emobilityloadsPDown_busnodes]     
        self.heatpumploads_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.heatpumploads_busnodes]
        self.heatpumploadsPMax_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.heatpumploadsPMax_busnodes]
        self.heatpumploadsECumulMax_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.heatpumploadsECumulMax_busnodes]
        self.heatpumploadsECumulMin_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.heatpumploadsECumulMin_busnodes]
        self.H2loads_busnodes = [{k:remap_bus_id(k,v) for k,v in d.items()} for d in self.H2loads_busnodes]

        # Create mapping files for post-processing analysis (after all remapping is complete)
        print('-----Creating Mapping Files----------------------')
        mapping_data = {
            'generators': self.generators,
            'buses': self.buses,
            'lines': self.lines,
            'transformers': self.transformers,
            'gens_busnodes': self.gens_busnodes,
            'distivinj_busnodes': self.distivinj_busnodes,
            'loads_busnodes': self.loads_busnodes,
            'emobilityloads_busnodes': self.emobilityloads_busnodes,
            'heatpumploads_busnodes': self.heatpumploads_busnodes,
            'H2loads_busnodes': self.H2loads_busnodes
        }
        
        # Construct results_folder path using the same logic as in GetOptimization
        try:
            idScenario_to_year = {1: 2018, 2: 2020, 3: 2030, 4: 2040, 5: 2050}

            if __name__ == "__main__": 
                # matlab workflow
                results_folder = ""
            else: 
                # python workflow
                idScenario_to_year = {
                    1: 2018,
                    2: 2020,
                    3: 2030,
                    4: 2040,
                    5: 2050,
                }
                results_folder = str(
                    Path(config.results_folder) 
                    / f"CentIv_{idScenario_to_year[config.idScenario]}"
                )
            # Create mappings subfolder
            saveMappingFiles(mapping_data, results_folder)

        except Exception as e:
            print(f"Warning: Could not create mapping files: {e}")
        print('-------------------------------------------------')
        print('')

        print('-----Number of Transmission Lines----------------')
        print(len(self.lines))
        print('-------------------------------------------------')
        logging.debug("In DataImport, finished all steps")
    
    def GetOptimization(self, config: Config):
        """
        Steps:
        0. Get unit status - candidate or existing  
        1.Look up lists to be used for sorting the generators  
        2.Empty lists (to be filled in with the key of the generators belonging to the given technology)  
        3.Sort generators depending on whether they are candidate or not and append their key to the corresponding list  
        4.Filter generators depending on their technology type  
        4.1. Sort out candidate lines  
        5.Fill in a dictionary with time series for every generator in the system  
        5.1. Fill in renewable timeseries aka windspeeds, solar irradiation, water inflows, etc.  
        5.2. Fill in a dictionary with refueling schedule of nuclear reactors  
        5.3. Fill in a dictionary with production cost time series / sell price time series  
        6.Fill in a dictionary with load timeseries for each bus in the system AND collect power/energy shifting limits for each bus  
        7.Fill in a dictionary with distIv power injection timeseries for each bus in the system  


        8.Get the Generator Configuration Id for this Scenario  
        9.Trim time series data using resolution args.tpResolution  
        

        10.Overwrite demand and reserves time series data using inputs from DistIv  
        11.Slack Bus and BaseMVA values
        12.Set up the cost optimization (here we have all constraints related to operation of the different generator types)
            12.1...Set up conventional generator constraints
            12.2...Set up RES constraints for existing RES units
            12.3...Set up dispatchable hydro (dams and pumps) constraints
            12.4...Set up RES target constraint + investment constraints for RES units, i.e. candidates_nondisp
            12.5...Set reserves per generator type
            12.6...Set up total system reserve constraints
            
        13.Set up the network for DC Power Flow (here we have all constraints related to the grid)
        14.Set up the objective function
        15.Solve
        16.Post Processing (of mostly the investment run)
            16.0 Initiating and basic info extraction
            16.1 output: store newly built and existing generators
            16.2 output: store all parameters of newly built lines
            16.3 output: new investments (to update MySQL database) 
            16.4 output: input arguments for additional reserve calculation (for eMark and to update MySQL database)
            16.5 output: nodal demand in CH with bus/canton info (tpResolution)
            16.6 output: nodal demand in CH with bus/canton info (hourly)
            16.7 output: demand in neighbouring countries (hourly)
            16.8 output: original demand for EM with nodal information and bus names (hourly)
            16.9 output: secondary reserve requirement (tpResolution)
            16.10 output: original RES target 
            16.11 output: demand scalar from CGE for CH
        17.Solve LP Problem
            17.1.Fixing Investment and Commitment Variables for Dual Calculation and LP Re-solve
            17.2 Setting up the objective function for the LP re-solve
            17.3 Saving the results of the LP re-solve (reserve prices, electricity prices, RES premium price, winter net import dual, generation per generator ALL (hourly) and also per country (hourly), generation per generator ALL (hourly) and also per country (hourly), operation costs per generator (hourly), batteries state of charge in CH (hourly), cumulative storage level in CH (hourly and monthly)  , generation/consumption per technology type in DE,AT,FR,IT (total and monthly), generators and costs in CH for eMark, LCOE information - new investments only, RES curtailment of nodal distIv injections (hourly), nodal load shedding for all nodes including neighbours (hourly), nodal load shifting for all nodes including neighbours (hourly), final load for all nodes including neighbors (load - load shedding + upshifting - downshifting), dam production - CH and AT, DE, FR, IT (hourly), pump production/consumption - CH and AT, DE, FR, IT (hourly), RES production (pv, wind and biomass) in CH (total), exports&imports CH to neighbours / neighbours to neighbours (total), cross-border flows CH (hourly), active power per branch ALL (hourly), reserve contribution per generator (hourly), all P2G2P time series, state of charge of H2 storages in CH (hourly), H2/CH4 imports and costs + Revenue from sold H2/CH4 + Revenue from sequestered CO2, H2 variables (non-import) all together)
        """

        if __name__ == "__main__": 
            # matlab workflow
            results_folder = ""
        else: 
            # python workflow
            idScenario_to_year = {
                1: 2018,
                2: 2020,
                3: 2030,
                4: 2040,
                5: 2050,
            }
            results_folder = str(
                Path(config.results_folder) 
                / f"CentIv_{idScenario_to_year[config.idScenario]}"
            )
            os.makedirs(results_folder, exist_ok=True)

        duration_log_dict = {}


        logging.debug('Startng separate timing for ----- GetOptimization ---------------------')
        time_restarted = time.time()
        #0.Get unit status - candidate or existing
        unitsTechnology = {k:self.generators[k]['Technology'] for k in self.generator_id}

        #1.Look up lists to be used for sorting the generators
        conventional_technologies = ['Nuclear', 'Nuclear-FastRamp', 'Coal', 'Coal-IGCC', 'Gas', 'GasIC', 'GasBlr',
                                     'Lignite', 'GasCHP', 'GasTurbine', 'CCGT', 'OCGT', 'Oil',
                                     'Geothermal', 'GasCC', 'GasCC-Syn', 'GasCC-CCS', 'GasSC', 'Biogas', 'Biomass',
                                     'Waste', 'Oil-DNS', 'Geothermal-Advanced', 'GasST', 'Other-NonRES', 'FuelCell', 'GasCC-Syn-CCS', 'GasCC-H2']
        solar_technologies = ['Solar-Thermal', 'pv', 'PV', 'PV-roof', 'PV-open', 'PV-alpine', 'PV-agri', 'PV-hwy', 'PV-facade', 'CSP', 'Other-RES']
        solar_rooftop_technologies = ['PV', 'PV-roof']
        wind_technologies = ['Wind', 'WindTurbine', 'WindOn', 'WindOff']
        battery_technologies = ['BatteryStorage', 'GenericStorage', 'Battery', 'battery', 'BattDSO', 'BattTSO', 'Battery-DSO', 'Battery-TSO']
        p2X_technologies = ['P2G2P']
        NET_technologies = ['DAC']
        hydro_technologies = ['RoR', 'Dam', 'Pump', 'Pump-Open', 'Pump-Closed']
        nuclear_technologies = ['Nuclear', 'nuclear', 'Nuclear-FastRamp']
        DSO_gen_technologies = ['Solar-Thermal', 'PV', 'PV-roof', 'PV-facade', 'BattDSO', 'Battery-DSO']
        hydro_storage_technologies = ['Dam', 'Pump', 'Pump-Open', 'Pump-Closed']

        #2.Empty lists (to be filled in with the key of the generators belonging to the given technology)
        candidates = [] #all conventional candidate units (i.e. gas, biomass)
        candidates_nondisp = [] #all non-dispatchable candidate units (i.e. PV, wind)
        candidates_batteries = [] #all BESS candidate units
        candidates_P2X = [] #all p2X candidate units
        candidates_P2X_notconnected = []
        candidates_NET = [] #all NET candidate units
        list_pv_cand = [] #only pv candidates
        list_wind_cand = [] #only wind candidates

        #dsm = [] #all dsm units
        hydro_Pumped_CH_daily = [] #all pumped hydro units with daily cycles regardless if candidate or existing

        # Select the algorithm used to define the constraint 
        # for rooftop PV target
        base_rooftop_PV_target_on_potential_generation = True
        
        #3.Sort generators depending on whether they are candidate or not and append their key to the corresponding list
        for key, row in enumerate(self.generators):
            if row['CandidateUnit'] == 1 and row['UnitType'] == 'NonDispatchable':
                candidates_nondisp.append(key)
                if row['Technology'] in wind_technologies:
                    list_wind_cand.append(key)
                if row['Technology'] in solar_technologies:
                    list_pv_cand.append(key)
            elif row['CandidateUnit'] == 1 and row['UnitType'] == 'Dispatchable':
                if row['GenType'] == 'Conv' or row['GenType'] == 'RES':
                    candidates.append(key)
                if row['GenType'] == 'Stor' and (row['Technology'] in battery_technologies):
                    candidates_batteries.append(key)
                if row['GenType'] == 'Stor' and row['Technology'] in p2X_technologies:
                    candidates_P2X.append(key)
                if row['GenType'] == 'NET' and row['Technology'] in NET_technologies:
                    candidates_NET.append(key)    
        print('-----Candidate P2X technologies------------------')
        print(candidates_P2X)
        print({k:self.generators[k]['InvCost_Charge'] for k in candidates_P2X})
        print({k:-self.generators[k]['Pmin'] for k in candidates_P2X})
        print('-------------------------------------------------')
        print('')
        print('-----Candidate NET technologies------------------')
        print(candidates_NET)
        print('-------------------------------------------------')
        print('')
        #4. Filter generators depending on their technology type
        def get_units_with_technologies(units: dict, technologies: list) -> list:
            return  [id for id, technology in units.items() if technology in technologies]
        
        conv = get_units_with_technologies(unitsTechnology, conventional_technologies)
        pv = get_units_with_technologies(unitsTechnology, solar_technologies)
        rooftop_pv = get_units_with_technologies(unitsTechnology, solar_rooftop_technologies)
        wind = get_units_with_technologies(unitsTechnology, wind_technologies)
        batt = get_units_with_technologies(unitsTechnology, battery_technologies)
        hydro_RoR = get_units_with_technologies(unitsTechnology, ["RoR"])
        hydro_Dam = get_units_with_technologies(unitsTechnology, ["Dam"])
        hydro_Pumped = get_units_with_technologies(unitsTechnology, ["Pump", "Pump-Open", "Pump-Closed"])
        nuclear = get_units_with_technologies(unitsTechnology, nuclear_technologies)
        conv_biomass = get_units_with_technologies(unitsTechnology, ["Biogas", "Biomass", "Waste"])
        conv_geothermal = get_units_with_technologies(unitsTechnology, ["Geothermal", "Geothermal-Advanced"])
        p2x = get_units_with_technologies(unitsTechnology, p2X_technologies)
        net = get_units_with_technologies(unitsTechnology, NET_technologies)
        #4.1.Sort out candidate lines
        candidate_lines = [] #all candidate transmission lines
        existing_lines = [] #all existing transmission lines
        fixed_lines_values = {} #these are candidate lines which are built by either CentIv or Cascades but need to be included in the objective f-n (for Cascades interface)
        for key, row in enumerate(self.lines):
            if row['Candidate'] == 1:
                if config.single_electric_node == False: # if we were in single node mode, we would not have candidate transmission lines.
                    candidate_lines.append(key)
                    if row['expFlag'] == 1:
                        fixed_lines_values.update({key:row['FixInv']})
            else:
                existing_lines.append(key)
        #print('Fixed Lines Values')
        #print(fixed_lines_values)
        #print('Candidate Lines')
        #print(candidate_lines)
        #print('Existing Lines')
        #print(existing_lines)

        #5.Fill in a dictionary with time series for every generator in the system
        #...If a given generator does not have time series data associated with it, an array with zeroes is automatically created
        #...All time series have length the number of simulated time periods
        #...5.1 Fill in renewable timeseries aka windspeeds, solar irradiation, water inflows, etc. 
        renewables_timeseries = {}
        for Id, row in enumerate(self.gens_busnodes):
            if row['type'] != 'Refueling': #we treat nuclear reactors differently
                if row['timeSeries'] and row['type'] == 'WindGen':
                    ts = json.loads(row['timeSeries'])
                    renewables_timeseries.update({Id:{k:round(ts[k],2) for k in range(self.timeperiods)}})
                elif row['timeSeries'] and row['type'] == 'SolarGen':
                    ts = json.loads(row['timeSeries'])
                    renewables_timeseries.update({Id:{k:round(ts[k],2) for k in range(self.timeperiods)}})
                elif row['timeSeries'] and row['type'] == 'Water':
                    ts = json.loads(row['timeSeries'])
                    renewables_timeseries.update({Id:{k:ts[k] for k in range(self.timeperiods)}})
                elif row['timeSeries'] and row['type'] == 'Gen':
                    ts = json.loads(row['timeSeries'])
                    renewables_timeseries.update({Id:{k:round(ts[k],2) for k in range(self.timeperiods)}})
                else:
                    renewables_timeseries.update({Id:{k:0 for k in range(self.timeperiods)}})  #list with zeroes with the same length
            else:
                renewables_timeseries.update({Id:{k:0 for k in range(self.timeperiods)}})  #list with zeroes with the same length
        #dam_inflows = {k:renewables_timeseries[k] for k in hydro_Dam} #timeseries of dam inflows
        non_swiss_gens = []
        for Id, row in enumerate(self.generators):
            if row['Country'] != 'CH':
                non_swiss_gens.append(Id)

        hydro_Dam_CH = [item for item in hydro_Dam if item not in non_swiss_gens]
        hydro_Pumped_CH = [item for item in hydro_Pumped if item not in non_swiss_gens]


        hourly_inflows_pumps = {k:renewables_timeseries[k] for k in hydro_Pumped_CH} 
        print('-----Pump Annual Inflows-------------------------')
        print({gens:sum([inflow for inflow in inflows.values()]) for gens,inflows in hourly_inflows_pumps.items()})
        print('-------------------------------------------------')
        print('')
        #Total_Dam_Inflows = {gens:sum([inflow for inflow in inflows.values()]) for gens,inflows in dam_inflows.items()}  #all dam inflows summed up
        #...5.2 Fill in a dictionary with refueling schedule of nuclear reactors
        for Id, row in enumerate(self.gens_busnodes):
            if row['type'] == 'Refueling':
                ts = json.loads(row['timeSeries'])
                self.nuclear_availability_timeseries.update({Id:{k:ts[k] for k in range(len(ts))}})
                
        for key,row in self.nuclear_availability_timeseries.items():
            hourly_status = np.repeat(list(row.values()), 168, axis=0) #augment weekly timeseries to hourly timeseries
            self.nuclear_availability_timeseries.update({key:{k:hourly_status[k] for k in range(self.timeperiods)}}) #trim to self.timeperiods
        #...5.3 Fill in a dictionary with production cost time series / sell price time series 
        fuelprice_timeseries = {}
        fuelpriceSELL_timeseries = {} #for p2g2p
        co2price_timeseries = {}
        h2priceSELL_timeseries = {} #for p2g2p
        h2importprice_timeseries = {} #for p2g2p
        ch4importprice_timeseries = {} #for p2g2p
        H2demand_timeseries = {} #for p2g2p
        CH4demand_timeseries = {} #for p2g2p
        for Id, row in enumerate(self.generators):
            if row['FuelPrice_mult_idProfile'] is None or np.isnan(row['FuelPrice_mult_idProfile']):
                fuelprice_timeseries.update({Id:{k:row['FuelPrice'] for k in range(self.timeperiods)}})
            else:
                for _, row2 in enumerate(self.profiles):
                    if row['FuelPrice_mult_idProfile'] == row2['idProfile']:
                        ts = json.loads(row2['timeSeries'])
                        fuelprice_timeseries.update({Id:{k:ts[k]*row['FuelPrice'] for k in range(self.timeperiods)}})
        for Id, row in enumerate(self.generators):
            if row['CO2Price_mult_idProfile'] is None:
                co2price_timeseries.update({Id:{k:row['CO2Price'] for k in range(self.timeperiods)}})
            else:
                for _, row2 in enumerate(self.profiles):
                    if row['CO2Price_mult_idProfile'] == row2['idProfile']:
                        ts = json.loads(row2['timeSeries'])
                        co2price_timeseries.update({Id:{k:ts[k]*row['CO2Price'] for k in range(self.timeperiods)}})
        #P2G2P-related time series
        for Id, row in enumerate(self.generators):
            if row['Technology'] != 'P2G2P':
                h2priceSELL_timeseries.update({Id:{k:0 for k in range(self.timeperiods)}}) #for non-p2g2p units populate with 0
            else:
                if row['H2Price_sell_mult_idProfile'] is None or np.isnan(row['H2Price_sell_mult_idProfile']):
                    h2priceSELL_timeseries.update({Id:{k:row['H2Price_sell'] for k in range(self.timeperiods)}})
                else:
                    for _, row2 in enumerate(self.profiles):
                       if row['H2Price_sell_mult_idProfile'] == row2['idProfile']:
                        ts = json.loads(row2['timeSeries'])
                        h2priceSELL_timeseries.update({Id:{k:ts[k]*row['H2Price_sell'] for k in range(self.timeperiods)}}) 
        for Id, row in enumerate(self.generators):
            if row['Technology'] != 'P2G2P':
                fuelpriceSELL_timeseries.update({Id:{k:0 for k in range(self.timeperiods)}}) #for non-p2g2p units populate with 0
            else:
                if row['FuelPrice_sell_mult_idProfile'] is None or np.isnan(row['FuelPrice_sell_mult_idProfile']):
                    fuelpriceSELL_timeseries.update({Id:{k:row['FuelPrice_sell'] for k in range(self.timeperiods)}})
                else:
                    for _, row2 in enumerate(self.profiles):
                       if row['FuelPrice_sell_mult_idProfile'] == row2['idProfile']:
                        ts = json.loads(row2['timeSeries'])
                        fuelpriceSELL_timeseries.update({Id:{k:ts[k]*row['FuelPrice_sell'] for k in range(self.timeperiods)}}) 
        for Id, row in enumerate(self.generators):
            if row['Technology'] != 'P2G2P':
                h2importprice_timeseries.update({Id:{k:0 for k in range(self.timeperiods)}}) #for non-p2g2p units populate with 0
            else:
                if row['H2Price_import_mult_idProfile'] is None or np.isnan(row['H2Price_import_mult_idProfile']):
                    h2importprice_timeseries.update({Id:{k:row['H2Price_import'] for k in range(self.timeperiods)}})
                else:
                    for _, row2 in enumerate(self.profiles):
                       if row['H2Price_import_mult_idProfile'] == row2['idProfile']:
                        ts = json.loads(row2['timeSeries'])
                        h2importprice_timeseries.update({Id:{k:ts[k]*row['H2Price_import'] for k in range(self.timeperiods)}}) 
        for Id, row in enumerate(self.generators):
            if row['Technology'] != 'P2G2P':
                ch4importprice_timeseries.update({Id:{k:0 for k in range(self.timeperiods)}}) #for non-p2g2p units populate with 0
            else:
                if row['CH4Price_import_mult_idProfile'] is None or np.isnan(row['CH4Price_import_mult_idProfile']):
                    ch4importprice_timeseries.update({Id:{k:row['CH4Price_import'] for k in range(self.timeperiods)}})
                else:
                    for _, row2 in enumerate(self.profiles):
                       if row['CH4Price_import_mult_idProfile'] == row2['idProfile']:
                        ts = json.loads(row2['timeSeries'])
                        ch4importprice_timeseries.update({Id:{k:ts[k]*row['CH4Price_import'] for k in range(self.timeperiods)}}) 
        if self.H2profileId is None or np.isnan(self.H2profileId):
            H2demand_timeseries.update({k:{'H2Demand':config.targetH2/8760} for k in range(self.timeperiods)})
        else:
            for Id, row in enumerate(self.profiles):
                if row['idProfile'] == self.H2profileId:
                    ts = json.loads(row['timeSeries'])
                    H2demand_timeseries.update({k:{'H2Demand':ts[k]*config.targetH2} for k in range(self.timeperiods)})
        if self.CH4profileId is None or np.isnan(self.CH4profileId):
            CH4demand_timeseries.update({k:{'CH4Demand':config.targetCH4/8760} for k in range(self.timeperiods)})
        else:
            for Id, row in enumerate(self.profiles):
                if row['idProfile'] == self.CH4profileId:
                    ts = json.loads(row['timeSeries'])
                    CH4demand_timeseries.update({k:{'CH4Demand':ts[k]*config.targetCH4} for k in range(self.timeperiods)})
        
        
        #6.Fill in a dictionary with load timeseries for each bus in the system AND collect power/energy shifting limits for each bus
        #...If a given bus does not have load time series data associated with it, an array with zeroes is automatically created
        #...All time series have length the number of simulated time periods
        eload_timeseries = {}
        dsm_pshift_hourly = {}
        dsm_eshift_daily = {}
        for Id, row in enumerate(self.loads_busnodes):
            dsm_pshift_hourly.update({row['idIntBus']:row['DemandShare'] * row['DSM_PowerShift_Hrly'] * 1000}) # the value in the DB is in GW
            dsm_eshift_daily.update({row['idIntBus']:row['DemandShare'] * row['DSM_EnergyShift_Daily'] * 1000 * 2}) # the value in the DB is in GWh
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                eload_timeseries.update({(row['idIntBus']):{k:ts[k] for k in range(self.timeperiods)}}) # the multiplier "* row['DemandShare']" was removed. timeSeries already contains the final value of the demand.
            else:
                eload_timeseries.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})  #list with zeroes with the same length ....
        emobilityload_timeseries = {}
        for Id, row in enumerate(self.emobilityloads_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                emobilityload_timeseries.update({(row['idIntBus']):{k:ts[k] for k in range(self.timeperiods)}}) # the multiplier "* row['DemandShare']" was removed. timeSeries already contains the final value of the demand.
            else:
                emobilityload_timeseries.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})  #list with zeroes with the same length .... 
        # for the new eMobility dictionaries
        emob_eshift_daily = {}
        emob_pupshift_hourly = {}
        emob_pdownshift_hourly = {}
        for Id, row in enumerate(self.emobilityloadsEFlex_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                emob_eshift_daily.update({(row['idIntBus']):{k:ts[k] for k in range(int(self.timeperiods/24))}})
                
            else: 
                emob_eshift_daily.update({(row['idIntBus']):{k:0 for k in range(int(self.timeperiods/24))}})
        for Id, row in enumerate(self.emobilityloadsPUp_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                emob_pupshift_hourly.update({(row['idIntBus']):{k:ts[k] for k in range(self.timeperiods)}})
            else:
                emob_pupshift_hourly.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})
        for Id, row in enumerate(self.emobilityloadsPDown_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                emob_pdownshift_hourly.update({(row['idIntBus']):{k:ts[k] for k in range(self.timeperiods)}})
            else: 
                emob_pdownshift_hourly.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})

        # define the remaining inflexible HP load 
        heatpumpload_timeseries = {}
        uncontrolled_heatpumpload_timeseries = {}
        heatpumpload_timeseries_flexibleportion = {}
        for Id, row in enumerate(self.heatpumploads_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                heatpumpload_timeseries.update({(row['idIntBus']):{k:ts[k]*(1-self.HPFlexiblePercentage) for k in range(self.timeperiods)}}) # the multiplier "*(1-self.HPFlexiblePercentage)" indicates how much inflexible load should be kept in the HP load timeseries
                uncontrolled_heatpumpload_timeseries.update({(row['idIntBus']):{k:ts[k] for k in range(self.timeperiods)}}) # we keep a version of the provided uncontrolled demand, JG: this should be the full original HP load before any shifting
                heatpumpload_timeseries_flexibleportion.update({(row['idIntBus']):{k:ts[k]*(self.HPFlexiblePercentage) for k in range(self.timeperiods)}}) # JG: here the multiplier "*(self.HPFlexiblePercentage)" indicates how much flexible load should be kept in the HP load timeseries
            else:
                heatpumpload_timeseries.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})  #list with zeroes with the same length .... 
        # added for the new Heatpump flexibility dictionaries
        heatpump_pmax_hourly = {}
        heatpump_ecumulmax_hourly = {}
        heatpump_ecumulmin_hourly = {}
        for Id, row in enumerate(self.heatpumploadsPMax_busnodes):
            heatpump_pmax_hourly.update({row['idIntBus']:row['value']}) # the value in the DB is in MW
        for Id, row in enumerate(self.heatpumploadsECumulMax_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                heatpump_ecumulmax_hourly.update({(row['idIntBus']):{k:ts[k]*(self.HPFlexiblePercentage) for k in range(self.timeperiods)}})
            else:
                heatpump_ecumulmax_hourly.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})
        for Id, row in enumerate(self.heatpumploadsECumulMin_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                heatpump_ecumulmin_hourly.update({(row['idIntBus']):{k:ts[k]*(self.HPFlexiblePercentage) for k in range(self.timeperiods)}})
            else: 
                heatpump_ecumulmin_hourly.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})
        
        H2load_timeseries = {}
        for Id, row in enumerate(self.H2loads_busnodes):
            if row['timeSeries'] :
                ts = json.loads(row['timeSeries'])
                H2load_timeseries.update({(row['idIntBus']):{k:ts[k] for k in range(self.timeperiods)}}) # the multiplier "* row['DemandShare']" was removed. timeSeries already contains the final value of the demand.
            else:
                H2load_timeseries.update({(row['idIntBus']):{k:0 for k in range(self.timeperiods)}})  #list with zeroes with the same length .... 
        #check for None values within the load and flexibility data
        for Id, row in eload_timeseries.items():
            for k, _ in row.items():
                if row[k] is None:
                    #print "None value in load time series for time period", k, "for bus", Id
                    row[k] = row[k-1]

        for Id, row in emobilityload_timeseries.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        
        for Id, row in emob_eshift_daily.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        
        for Id, row in emob_pupshift_hourly.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        
        for Id, row in emob_pdownshift_hourly.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]

        for Id, row in heatpumpload_timeseries.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        
        for Id, row in uncontrolled_heatpumpload_timeseries.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        
        for Id, row in heatpumpload_timeseries_flexibleportion.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]

        for Id, row in heatpump_ecumulmax_hourly.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        for Id, row in heatpump_ecumulmin_hourly.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]

        for Id, row in H2load_timeseries.items():
            for k, _ in row.items():
                if row[k] is None:
                    row[k] = row[k-1]
        # check for negative values in the downwards flexibility (created by downsampling of data, values very close to zero)
        for Id, row in emob_pdownshift_hourly.items():
            for k, _ in row.items():
                if row[k] < 0:
                    row[k] = 0
        
        #add all load timeseries 
        H2_plus_heatpumpload_timeseries = {}
        # Include all buses from both H2 and heatpump loads
        all_buses_h2_hp = set(H2load_timeseries.keys()) | set(heatpumpload_timeseries.keys())
        for k in all_buses_h2_hp:
            h2_load = H2load_timeseries.get(k, {t: 0 for t in range(self.timeperiods)})
            hp_load = heatpumpload_timeseries.get(k, {t: 0 for t in range(self.timeperiods)})
            dict_add = {key: h2_load.get(key, 0) + hp_load.get(key, 0) for key in range(self.timeperiods)}
            H2_plus_heatpumpload_timeseries[k] = dict_add

        eload_plus_emobilityload_timeseries = {}
        # Include all buses from both conventional and e-mobility loads
        all_buses_conv_emob = set(eload_timeseries.keys()) | set(emobilityload_timeseries.keys())
        for k in all_buses_conv_emob:
            conv_load = eload_timeseries.get(k, {t: 0 for t in range(self.timeperiods)})
            emob_load = emobilityload_timeseries.get(k, {t: 0 for t in range(self.timeperiods)})
            dict_add = {key: conv_load.get(key, 0) + emob_load.get(key, 0) for key in range(self.timeperiods)}
            eload_plus_emobilityload_timeseries[k] = dict_add

        load_timeseries = {} #this is the sum of all loads
        # Include all buses from both combined dictionaries
        all_buses_final = set(H2_plus_heatpumpload_timeseries.keys()) | set(eload_plus_emobilityload_timeseries.keys())
        for k in all_buses_final:
            h2_hp_load = H2_plus_heatpumpload_timeseries.get(k, {t: 0 for t in range(self.timeperiods)})
            conv_emob_load = eload_plus_emobilityload_timeseries.get(k, {t: 0 for t in range(self.timeperiods)})
            dict_add = {key: h2_hp_load.get(key, 0) + conv_emob_load.get(key, 0) for key in range(self.timeperiods)}
            load_timeseries[k] = dict_add
            
        self.original_load = copy.deepcopy(load_timeseries) #this is the total original load before any adjustments (i.e. Gemel load scale factor, subtraction of DistIv injections, etc.)
        
        #swiss busses
        buses_CH = []
        #other buses
        buses_Neighbors = []
        buses_AT = []
        buses_DE = []
        buses_FR = []
        buses_IT = []
        bus_neighbor_NTC_rep = [] #buses that are neighbors are considered in neighbouring countries used for the rep. NTC connection 
        bus_neighbor_no_NTC_rep = [] # buses that are neighbors but not used for the rep. NTC connection. They will always require nodal balance equation.
        #all buses
        buses_all = []
        
        for Id, row in enumerate(self.buses):
            buses_all.append(Id)
            if row['Country'] == 'CH':
                row.update({u'PayInjection': 0}) #should be a parameter coming from DistIv
                buses_CH.append(Id)
            else:
                row.update({u'PayInjection': 0})
                buses_Neighbors.append(Id)
                if row['Country'] == 'AT':
                    buses_AT.append(Id)
                if row['Country'] == 'DE':
                    buses_DE.append(Id) 
                if row['Country'] == 'FR':
                    buses_FR.append(Id)
                if row['Country'] == 'IT':
                    buses_IT.append(Id)  
                # if row['canton'] ends in _X, save it as bus_neighbor_NTC_rep
                if '_X_' in row['BusName']: # NOTE this is not ideal! there should be a better of indicating such busses in getBusData_v2 procedure in sql!
                    bus_neighbor_NTC_rep.append(Id)
                else:
                    bus_neighbor_no_NTC_rep.append(Id)
                    
                 
        Total_Load_Per_Bus_db = {bus:sum([demand for demand in demands.values()]) for bus, demands in load_timeseries.items()}

        swiss_load_db = []
        for key,value in Total_Load_Per_Bus_db.items():
            if key in buses_CH: 
                swiss_load_db.append(value)
        swiss_load_total_db = sum(swiss_load_db)
        print('-----Load Information----------------------------')
        print('    3) Total initial CH load from db is %f' %(swiss_load_total_db) + ' MWh')
        
        if config.demandCH == 0:
            Adjusted_Load_CH = 1.0 #if we don't get an input from Gemel keep the load pulled from the database       
        else:
            Adjusted_Load_CH = config.demandCH/float(swiss_load_total_db)
        print('    4) CH load scale factor is %f' %(Adjusted_Load_CH))
        
        for Id, row in load_timeseries.items():
            if Id in buses_CH:
                row.update((k, v*Adjusted_Load_CH) for k, v in row.items()) #adjust the Swiss load using the total Swiss demand from Gemel
        
        for Id, row in emobilityload_timeseries.items():
            if Id in buses_CH:
                row.update((k, v*Adjusted_Load_CH) for k, v in row.items()) #adjust the Swiss e-mobility load using the total Swiss demand from Gemel
        
        for Id, row in heatpumpload_timeseries.items():
            if Id in buses_CH:
                row.update((k, v*Adjusted_Load_CH) for k, v in row.items()) #adjust the Swiss heatpump load using the total Swiss demand from Gemel

        for Id, row in uncontrolled_heatpumpload_timeseries.items():
            if Id in buses_CH:
                row.update((k, v*Adjusted_Load_CH) for k, v in row.items()) #adjust the Swiss (uncontrolled) heatpump load using the total Swiss demand from Gemel

        for Id, row in heatpumpload_timeseries_flexibleportion.items():
            if Id in buses_CH:
                row.update((k, v*Adjusted_Load_CH) for k, v in row.items()) #adjust the Swiss (flexible portion) heatpump load using the total Swiss demand from Gemel
        
        self.adjusted_emobilityload = copy.deepcopy(emobilityload_timeseries)
        self.adjusted_heatpumpload = copy.deepcopy(heatpumpload_timeseries)
        self.adjusted_uncontrolledheatpumpload = copy.deepcopy(uncontrolled_heatpumpload_timeseries)
        self.adjusted_heatpumpload_flexibleportion = copy.deepcopy(heatpumpload_timeseries_flexibleportion)
        self.adjusted_H2electrolyzerload = copy.deepcopy(H2load_timeseries)
        self.adjusted_baseload = copy.deepcopy(eload_timeseries)
        
        #copy the load before accounting for any distIv injections - this needs to be sent to distIv
        load_timeseries_distIv = copy.deepcopy(load_timeseries)
        Total_Load_Per_Bus_adjusted = {bus:sum([demand for demand in demands.values()]) for bus,demands in load_timeseries.items()} 
        swiss_load_adjusted = []
        for key,value in Total_Load_Per_Bus_adjusted.items():
            if key in buses_CH: 
                swiss_load_adjusted.append(value)
        swiss_load_total_adjusted = sum(swiss_load_adjusted)        
        print('    5) Total adjusted CH load is %f' %(swiss_load_total_adjusted) + ' MWh')
        
        #7.Fill in a dictionary with distIv power injection timeseries for each bus in the system
        #...If a given bus does not have power injection series data associated with it, an array with zeroes is automatically created
        #...All time series have length the number of simulated time periods
        distiv_inj_timeseries = {}
        for Id, row in enumerate(self.distivinj_busnodes):
            if row['timeSeries']:
                ts = json.loads(row['timeSeries'])
                distiv_inj_timeseries.update({(row['internalBusId']):{k:ts[k] for k in range(self.timeperiods)}}) #{k:ts[k] for k in range(self.timeperiods)}}
            else:
                distiv_inj_timeseries.update({(row['internalBusId']):{k:0 for k in range(self.timeperiods)}})
        #check for None values within the data 
        for Id, row in distiv_inj_timeseries.items():
            for k, _ in row.items():
                if row[k] is None:
                    #print "None value in load time series for time period", k, "for bus", Id
                    row[k] = row[k-1]
        #subtract distiv_inj_timeseries from load_timeseries
        for k,v in distiv_inj_timeseries.items():
            if k in load_timeseries:
                dict_subtract = {key: load_timeseries[k][key] - v.get(key, 0) for key in load_timeseries[k]} # this is load_timeseries - distiv_inj_timeseries
                load_timeseries[k] = dict_subtract
                    
        Total_Load_Per_Bus_distIvInj = {bus:sum([demand for demand in demands.values()]) for bus,demands in load_timeseries.items()} 
        swiss_load_distIvInj = []
        for key,value in Total_Load_Per_Bus_distIvInj.items():
            if key in buses_CH: 
                swiss_load_distIvInj.append(value)
        swiss_load_total_distIvInj = sum(swiss_load_distIvInj)
        #distIv_inj_total = sum(distiv_inj_timeseries)   
        #print('    6.1) DistIv PV inj. from previous year is %f' %(distIv_inj_total) + ' MWh')
        print('    6) Total CH load after distIv PV inj. from previous year is %f' %(swiss_load_total_distIvInj) + ' MWh')
        #8.Fill in a dictionary with system reserves timeseries
        #...timeseries from database have length 8760
        SCR_UP = {}
        SCR_DN = {}
        TCR_UP = {}
        TCR_DN = {}
        for _, row in enumerate(self.reserves_timeseries):
            ts = json.loads(row['timeSeries'])
            if row['name'] == 'CH_Reserve_Secondary_UP':
                SCR_UP.update({i:{'FRRupReq':ts[i]} for i in range(len(ts))})
            if row['name'] == 'CH_Reserve_Secondary_DN':
                SCR_DN.update({i:{'FRRdnReq':ts[i]} for i in range(len(ts))})
            if row['name'] == 'CH_Reserve_Tertiary_UP':
                TCR_UP.update({i:{'RRupReq':ts[i]} for i in range(len(ts))})
            if row['name'] == 'CH_Reserve_Tertiary_DN':
                TCR_DN.update({i:{'RRdnReq':ts[i]} for i in range(len(ts))})

        def merge_dict(d1, d2):
            r = {}
            for k, v in d1.items():
                for k1, v1 in v.items():
                    r.setdefault(k,{})[k1] = v1
            for k, v in d2.items():
                for k1, v1 in v.items():
                    r.setdefault(k,{})[k1] = v1
            return r
        
        def dict_to_list(d):
            r = []
            for k, v in d.items():
                while k >= len(r):
                    r.append({})
                r[k] = v
            return r

        SCR = merge_dict(SCR_UP, SCR_DN)
        TCR = merge_dict(TCR_UP, TCR_DN)
        reserves_timeseries = merge_dict(SCR, TCR)

        #9.Trim time series data using resolution config.tpResolution
        res_change = ChangeResolution(config.timeperiods, config.tpResolution)
        res_change.remap_hours_dict_in_dict(renewables_timeseries)
        res_change.remap_hours_dict_in_dict(fuelprice_timeseries)
        res_change.remap_hours_dict_in_dict(fuelpriceSELL_timeseries)
        res_change.remap_hours_dict_in_dict(co2price_timeseries)
        res_change.remap_hours_dict_in_dict(h2priceSELL_timeseries)
        res_change.remap_hours_dict_in_dict(h2importprice_timeseries)
        res_change.remap_hours_dict_in_dict(ch4importprice_timeseries)
        res_change.remap_hours_dict_in_dict(load_timeseries)
        res_change.remap_hours_dict_in_dict(load_timeseries_distIv)
        res_change.remap_hours_dict_in_dict(emobilityload_timeseries) #we trim because we have constraints that depend on hourly emobility demand profile
        res_change.remap_days_dict_in_dict(emob_eshift_daily) # function to handle 1 daily value time series
        res_change.remap_hours_dict_in_dict(emob_pupshift_hourly)
        res_change.remap_hours_dict_in_dict(emob_pdownshift_hourly)
        res_change.remap_hours_dict_in_dict(self.adjusted_emobilityload) 
        res_change.remap_hours_dict_in_dict(self.adjusted_heatpumpload) 
        res_change.remap_hours_dict_in_dict(self.adjusted_uncontrolledheatpumpload) 
        res_change.remap_hours_dict_in_dict(self.adjusted_heatpumpload_flexibleportion)
        res_change.remap_hours_dict_in_dict(heatpumpload_timeseries) #we trim because we have constraints that depend on hourly heatpump demand profile
        res_change.remap_hours_dict_in_dict(uncontrolled_heatpumpload_timeseries)
        res_change.remap_hours_dict_in_dict(heatpumpload_timeseries_flexibleportion)
        res_change.remap_hours_dict_in_dict(heatpump_ecumulmax_hourly)
        res_change.remap_hours_dict_in_dict(heatpump_ecumulmin_hourly) 
        res_change.remap_hours_dict_in_dict(self.adjusted_H2electrolyzerload) #we want to save this data input
        res_change.remap_hours_dict_in_dict(self.adjusted_baseload) #we want to save this data input 
        res_change.remap_hours_dict_in_dict(self.original_load)
        res_change.remap_hours_dict_in_dict(self.nuclear_availability_timeseries)
        res_change.remap_hours_dict(reserves_timeseries)
        res_change.remap_hours_dict(H2demand_timeseries)
        res_change.remap_hours_dict(CH4demand_timeseries)
        self.timeperiods = res_change.new_timeperiods()  #this is the trimmed timeperiods length
        self.days = res_change.new_days() #the number of simulated days
        residual_reserves_timeseries_DistIv = {}
        for Id, d in enumerate(self.residual_reserve_DistIv_req):
            residual_reserves_timeseries_DistIv.update({Id:d})

        #10.Overwrite demand and reserves time series data using inputs from DistIv
        #demand
        if len(self.residual_load_DistIv) == 0:
            print('.... No input from DistIv for this same year....Taking load from 6)')
            print('-------------------------------------------------')
            print('')
            pass #the list is empty
        else:
            print('.... Detected input from DistIv for this same year....Taking residual load from DistIv instead of 6)')
            for k,v in self.residual_load_DistIv.items():
                if k in load_timeseries:
                    v1 = load_timeseries[k]
                    for h in v1.keys():
                        if h in v:
                            v1[h] = v[h]
        #reserves                    
        if len(self.residual_reserve_DistIv_req) == 0:
            pass #the list is empty
        else:
            for k,v in residual_reserves_timeseries_DistIv.items():
                if k in reserves_timeseries:
                    v1 = reserves_timeseries[k]
                    v1['FRRupReq'] = v['FRRupReq']
                    v1['FRRdnReq'] = v['FRRdnReq']
        
        Total_Load_Per_Bus_residual = {bus:sum(res_change.expand_array([demand for demand in demands.values()])) for bus,demands in load_timeseries.items()} 
        swiss_load_residual = []
        for key,value in Total_Load_Per_Bus_residual.items():
            if key in buses_CH: 
                swiss_load_residual.append(value)
        swiss_load_total_residual = sum(swiss_load_residual)
        if len(self.residual_load_DistIv) == 0:
            pass #the list is empty
        else:      
            print('    7) Total residual CH load is %f' %(swiss_load_total_residual) + ' MWh')
            print('-------------------------------------------------')
            print('')
        
        #11.Slack Bus and BaseMVA values
        #...Find the id of the slack bus and the value for baseMVA
        BusTypes = {k:self.buses[k]['BusType'] for k in range(len(self.bus_id))}
        SlackBusId = [key for key, bustype in BusTypes.items() if bustype == "SL"]
        if not config.single_electric_node:
            baseMVA = list({k:self.network_info[k]['baseMVA'] for k in range(len(self.network_info))}.values())[0]
        elif config.single_electric_node:
            baseMVA = 1.0

        #12.Set up the cost optimization (here we have all constraints related to operation of the different generator types)
        opt = SystemState(num_generators = len(self.generators), num_snaphots = self.timeperiods)
        print('-----Number of Generators------------------------')
        print(len(self.generators))
        print('-------------------------------------------------')
        print('')
        non_swiss_gens = []
        for Id, row in enumerate(self.generators):
            if row['Country'] != 'CH':
                non_swiss_gens.append(Id)
        non_dispatchable_gens = sorted(pv + wind + hydro_RoR)

        hydro_Dam_CH = [item for item in hydro_Dam if item not in non_swiss_gens]
        hydro_Pumped_CH = [item for item in hydro_Pumped if item not in non_swiss_gens]
        for key, row in enumerate(self.generators):
            if key in hydro_Pumped_CH:
                if row['Emax']/row['Pmax'] < 48: #these are "daily pumps"
                    hydro_Pumped_CH_daily.append(key)
        hydro_Pumped_CH_notdaily = [item for item in hydro_Pumped_CH if item not in hydro_Pumped_CH_daily]
        hydro_Pumped_notdaily = [item for item in hydro_Pumped if item not in hydro_Pumped_CH_daily] #all pumped hydro excluding pumps with daily cycles in CH     
        nuclear_CH = [item for item in nuclear if item not in non_swiss_gens]
        nuclear_CH_exist = [item for item in nuclear if item not in non_swiss_gens and item not in candidates]
        nuclear_CH_candidate = [item for item in nuclear if item not in non_swiss_gens and item in candidates]
        candidates_nonuclear = [item for item in candidates if item not in nuclear_CH_candidate]
        conv_biomass_CH = [item for item in conv_biomass if item not in non_swiss_gens]
        conv_geothermal_CH = [item for item in conv_geothermal if item not in non_swiss_gens]
        conv_biomass_and_geothermal = sorted(conv_biomass_CH + conv_geothermal_CH)
        pv_CH = [item for item in pv if item not in non_swiss_gens]
        wind_CH = [item for item in wind if item not in non_swiss_gens]
        RoR_CH = [item for item in hydro_RoR if item not in non_swiss_gens]
        conv_CH = [item for item in conv if item not in non_swiss_gens and item not in conv_biomass_CH
                   and item not in conv_geothermal_CH and item not in nuclear_CH] #everything conventional in CH except Swiss biomass, geothermal and Swiss nuclear
        conv_not_CH = [item for item in conv if item in non_swiss_gens] #everything conventional abroad
        conv_CH_and_biomassCH = sorted(conv_CH + conv_biomass_CH) #everything conventional in CH including biomass (without Swiss nuclear)
        conv_CH_and_biomassCH_and_geothermalCH = sorted(conv_CH_and_biomassCH + conv_geothermal_CH) #everything conventional in CH including biomass and geothermal (without Swiss nuclear)
        conv_CH_ALL = sorted(conv_CH_and_biomassCH_and_geothermalCH + nuclear_CH_candidate) #everything conventional in CH including biomass, geothermal + Swiss nuclear candidates
        conv_CH_ALL2 = sorted(conv_CH_and_biomassCH_and_geothermalCH + nuclear_CH) #everything conventional in CH including biomass, geothermal + Swiss nuclear units
        conv_CH_UT2 = []
        conv_CH_UT1 = []
        for Id, row in enumerate(self.generators):
            if row['Technology'] != 'Biomass':
                if row['UT'] >= 2 and row['UT'] <= 100: 
                    conv_CH_UT2.append(Id) 
                elif row['UT'] == 1:
                    conv_CH_UT1.append(Id)

        #12.1...Set up conventional generator constraints
        existing_conv_CH = [item for item in conv_CH_and_biomassCH_and_geothermalCH if item not in candidates]
        print('-----Some Swiss Generator Quantities-------------')
        print('     Number of existing conventional generators in CH (without nuclear):', len(existing_conv_CH))
        print('     Number of existing nuclear generators in CH:', len(nuclear_CH_exist))
        print('     Number of candidate conventional generators in CH (without nuclear):', len(candidates_nonuclear))
        print('     Number of candidate nuclear generators in CH:', len(nuclear_CH_candidate))
        print('     Number of all conventional generators in CH candidate or existing (without nuclear):', len(conv_CH_and_biomassCH_and_geothermalCH))
        print('     Number of all conventional generators in CH candidate or existing (with nuclear):', len(conv_CH_ALL2))
        print('     Number of hydro dam generators in CH:', len(hydro_Dam_CH))
        print('     Number of hydro pump (daily) generators in CH:', len(hydro_Pumped_CH_daily))
        print('     Number of hydro pump (non-daily) generators in CH:', len(hydro_Pumped_CH_notdaily))
        print('-------------------------------------------------')
        print('')
        
        print('-----Info if Unit Commitment or Continuous-------')
        if not config.contnuclear_required:
            print('.... Unit Commitment formulation - Swiss nuclear gens')
            cg = ConventionalGeneratorsTightReserves(opt, nuclear_CH_exist)
            #unit committment of CH nuclear reactors
            cg.set_initial_status(nuclear_CH_exist,
                {k:int(self.generators[k]['Tini']) for k in nuclear_CH_exist})
            cg.set_genlimits_min_power_tight(nuclear_CH_exist,
                {k:self.generators[k]['Pmin']/baseMVA for k in nuclear_CH_exist}) 
            cg.set_genlimits_max_power_tight_UT2(nuclear_CH_exist, 
                {k:self.generators[k]['Pmax']/baseMVA for k in nuclear_CH_exist}, 
                {k:self.generators[k]['RU_start']/baseMVA for k in nuclear_CH_exist}, 
                {k:self.generators[k]['RD_shutd']/baseMVA for k in nuclear_CH_exist})
            cg.set_pgen_t0(nuclear_CH_exist, 
                {k:self.generators[k]['Pini']/baseMVA for k in nuclear_CH_exist},
                {k:self.generators[k]['Pmin']/baseMVA for k in nuclear_CH_exist})
            cg.set_logical_order(nuclear_CH_exist)
            cg.set_rampup_tight(nuclear_CH_exist, 
                {k:self.generators[k]['RU_start']/baseMVA for k in nuclear_CH_exist}, 
                {k:self.generators[k]['RU']/baseMVA for k in nuclear_CH_exist})
            cg.set_rampdown_tight(nuclear_CH_exist, 
                {k:self.generators[k]['RD_shutd']/baseMVA for k in nuclear_CH_exist}, 
                {k:self.generators[k]['RD']/baseMVA for k in nuclear_CH_exist})    
            cg.set_minimum_uptime_tight(nuclear_CH_exist, 
                {k:int(self.generators[k]['UT']) for k in nuclear_CH_exist})
            cg.set_minimum_downtime_tight(nuclear_CH_exist, 
                {k:int(self.generators[k]['DT']) for k in nuclear_CH_exist})
            #set nuclear refueling
            for Id, row in self.nuclear_availability_timeseries.items():
                for t in range(self.timeperiods):
                    cg.model.UnitOn[Id,t].fix(row[t])
        else: 
            print('.... Continuous dispatch - Swiss nuclear gens')
            cg = ConventionalGeneratorsCHDispatchContinuous(opt, nuclear_CH_exist)
            #continuous operation of CH nuclear reactors
            cg.set_pgen_t0_dispatchcont(nuclear_CH_exist,
                {k:self.generators[k]['Pini']/baseMVA for k in nuclear_CH_exist})
            cg.set_max_power_CH_dispatchcont(nuclear_CH_exist,
                {k:self.generators[k]['Pmax']/baseMVA for k in nuclear_CH_exist})
            cg.set_ramp_linear_CH_dispatchcont_reserves(nuclear_CH_exist,
                {k:self.generators[k]['RU']/baseMVA for k in nuclear_CH_exist},
                {k:self.generators[k]['RD']/baseMVA for k in nuclear_CH_exist})
            cg.set_min_power_CH_Nuke_dispatchcont(nuclear_CH_exist,
                                                  {k: self.generators[k]['Pmin'] / baseMVA for k in nuclear_CH_exist},
                                                  {k: self.nuclear_availability_timeseries[k] for k in nuclear_CH_exist})
            #set nuclear refueling
            for Id, row in self.nuclear_availability_timeseries.items(): #nuclear availability only for existing nuclear pp
                if Id in nuclear_CH_exist:
                    for t in range(self.timeperiods):
                        if row[t] == 0.0:
                            opt.model.PowerGenerated[Id,t].fix(row[t])

        if config.continvest_required: 
            print('.... Continuous investment in conventional candidate units')
            cg_CH_noUC = ConventionalGeneratorsCHInvestContinuous(opt, conv_CH_and_biomassCH_and_geothermalCH,
                                                                  candidates_nonuclear, existing_conv_CH)
            cg_CH_noUC.set_pgen_t0_linear(conv_CH_and_biomassCH_and_geothermalCH,
                {k:self.generators[k]['Pini']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH})
            cg_CH_noUC.set_min_power_CH_linear(conv_CH_and_biomassCH_and_geothermalCH,
                {k:0 for k in conv_CH_and_biomassCH_and_geothermalCH})
            cg_CH_noUC.set_max_power_CH_linear(conv_CH_and_biomassCH_and_geothermalCH,
                {k:self.generators[k]['Pmax']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH})
            cg_CH_noUC.set_ramp_linear_reserves(conv_CH_and_biomassCH_and_geothermalCH,
                {k:self.generators[k]['RU']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH},
                {k:self.generators[k]['RD']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH})
        else: 
            print('.... Discrete (binary) investment in conventional candidate units')
            cg_CH_noUC = ConventionalGeneratorsCHInvestBinary(opt, conv_CH_and_biomassCH_and_geothermalCH, candidates_nonuclear, existing_conv_CH)
            cg_CH_noUC.set_pgen_t0_linear(conv_CH_and_biomassCH_and_geothermalCH,
                {k:self.generators[k]['Pini']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH})
            cg_CH_noUC.set_min_power_CH_linear(conv_CH_and_biomassCH_and_geothermalCH,
                {k:0 for k in conv_CH_and_biomassCH_and_geothermalCH})
            cg_CH_noUC.set_max_power_CH_linear(conv_CH_and_biomassCH_and_geothermalCH,
                {k:self.generators[k]['Pmax']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH})
            cg_CH_noUC.set_ramp_linear_reserves(conv_CH_and_biomassCH_and_geothermalCH,
                {k:self.generators[k]['RU']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH},
                {k:self.generators[k]['RD']/baseMVA for k in conv_CH_and_biomassCH_and_geothermalCH})
        
        if config.continvestnuclear_required: 
            print('.... Continuous investment in nuclear candidate units')
            CH_nuclear_invest = NuclearGeneratorsCHInvestCont(opt, nuclear_CH_candidate)
            CH_nuclear_invest.set_pgen_t0_nuclear(nuclear_CH_candidate,
                {k:self.generators[k]['Pini']/baseMVA for k in nuclear_CH_candidate})
            if config.min_nuclear_gen_lim:
                CH_nuclear_invest.set_min_power_CH_nuclear(nuclear_CH_candidate, 
                    {k: self.generators[k]['Pmin'] / baseMVA for k in nuclear_CH_candidate},
                    {k: self.nuclear_availability_timeseries[k] for k in nuclear_CH_candidate})
            else:    
                CH_nuclear_invest.set_min_power_CH_nuclear(nuclear_CH_candidate, 
                    {k:0 for k in nuclear_CH_candidate})
            CH_nuclear_invest.set_max_power_CH_nuclear(nuclear_CH_candidate,
                {k:self.generators[k]['Pmax']/baseMVA for k in nuclear_CH_candidate})
            CH_nuclear_invest.set_ramp_nuclear_reserves(nuclear_CH_candidate,
                {k:self.generators[k]['RU']/baseMVA for k in nuclear_CH_candidate},
                {k:self.generators[k]['RD']/baseMVA for k in nuclear_CH_candidate})
            #set nuclear refueling
            for Id, row in self.nuclear_availability_timeseries.items():
                if Id in nuclear_CH_candidate:
                    for t in range(self.timeperiods):
                        if row[t] == 0.0:
                            opt.model.PowerGenerated[Id,t].fix(row[t])
        else:
            print('.... Discrete (binary) investment in nuclear candidate units')
            CH_nuclear_invest = NuclearGeneratorsCHInvestBinary(opt, nuclear_CH_candidate)
            CH_nuclear_invest.set_pgen_t0_nuclear(nuclear_CH_candidate,
                {k:self.generators[k]['Pini']/baseMVA for k in nuclear_CH_candidate})
            CH_nuclear_invest.set_min_power_CH_nuclear(nuclear_CH_candidate, 
                {k:0 for k in nuclear_CH_candidate})
            CH_nuclear_invest.set_max_power_CH_nuclear(nuclear_CH_candidate,
                {k:self.generators[k]['Pmax']/baseMVA for k in nuclear_CH_candidate})
            CH_nuclear_invest.set_ramp_nuclear_reserves(nuclear_CH_candidate,
                {k:self.generators[k]['RU']/baseMVA for k in nuclear_CH_candidate},
                {k:self.generators[k]['RD']/baseMVA for k in nuclear_CH_candidate})
            #set nuclear refueling
            for Id, row in self.nuclear_availability_timeseries.items():
                if Id in nuclear_CH_candidate:
                    for t in range(self.timeperiods):
                        if row[t] == 0.0:
                            opt.model.PowerGenerated[Id,t].fix(row[t])

        print('-------------------------------------------------')
        print('')

        cg_DEATFRIT = ConventionalGeneratorsLinear(opt, conv_not_CH)
        conv_not_CH_biomass = [item for item in conv_biomass if item not in conv_biomass_CH]
        conv_not_CH_not_biomass = [item for item in conv_not_CH if item not in conv_not_CH_biomass]
        #min/max neighbouring countries
        cg_DEATFRIT.set_min_power(conv_not_CH,
            {k:self.generators[k]['Pmin']/baseMVA for k in conv_not_CH})
        cg_DEATFRIT.set_max_power(conv_not_CH,
            {k:self.generators[k]['Pmax']/baseMVA for k in conv_not_CH})

        logging.debug('Within GetOptimization, time steps 10 to 12.1: %f minutes' %((time.time() - time_restarted)/60))
        time_restarted = time.time()

        #12.2...Set up RES constraints for existing RES units
        pv_existing = [item for item in pv if item not in candidates_nondisp]
        wind_existing = [item for item in wind if item not in candidates_nondisp]
        all_RES_list = sorted(hydro_RoR + pv_existing + wind_existing)
        RES = RenewableGenerators(opt, all_RES_list)

        #12.2.1...Set up PV constraints for existing PV units
        RES.set_pv_power_production(pv_existing,
            {k:renewables_timeseries[k] for k in pv_existing},
            baseMVA)

        #12.2.2...Set up wind constraints for existing wind units
        RES.set_wind_power_production(wind_existing,
            {k:renewables_timeseries[k] for k in wind_existing},
            baseMVA)

        #12.2.3...Set up hydro run-of-river(RoR) constraints
        RES.set_hydro_power_RoR(hydro_RoR,
            {k:renewables_timeseries[k] for k in hydro_RoR},
            {k:self.generators[k]['Pmax']/baseMVA for k in hydro_RoR})

        #12.3...Set up dispatchable hydro (dams and pumps) constraints
        all_hydro_list = sorted(hydro_Dam + hydro_Pumped)
        hydro = HydroGenerators(opt, all_hydro_list)
        
        #12.3.1...Set up hydro dam constraints
        hydro.set_hydro_power_dam(hydro_Dam,
            {k:0 for k in hydro_Dam}, #Emin is 0
            {k:self.generators[k]['Emax']/baseMVA for k in hydro_Dam},
            {k:self.generators[k]['Pmax']/baseMVA for k in hydro_Dam},
            {k:self.generators[k]['Pmin']/baseMVA for k in hydro_Dam},
            {k:self.generators[k]['E_ini'] for k in hydro_Dam},
            {k:renewables_timeseries[k] for k in hydro_Dam},
            {k:self.generators[k]['eta_dis'] for k in hydro_Dam},
            config.tpResolution)

        #12.3.2...Set up hydro pumped constraints (for all pumped storages in CH and abroad except daily storages)
        hydro.set_hydro_power_Pumped(hydro_Pumped_notdaily,
            {k:0 for k in hydro_Pumped_notdaily}, #Emin is 0 in database
            {k:self.generators[k]['Emax']/baseMVA for k in hydro_Pumped_notdaily},
            {k:self.generators[k]['Pmax']/baseMVA for k in hydro_Pumped_notdaily},
            {k:-self.generators[k]['Pmin']/baseMVA for k in hydro_Pumped_notdaily},#we need the (-) because in the database Pmin is negative and the notation for maximum pump power HERE is positive
            {k:self.generators[k]['E_ini'] for k in hydro_Pumped_notdaily}, # E_ini [-] so no need to divide by baseMVA
            {k:renewables_timeseries[k] for k in hydro_Pumped_notdaily},
            {k:self.generators[k]['eta_ch'] for k in hydro_Pumped_notdaily},
            {k:self.generators[k]['eta_dis'] for k in hydro_Pumped_notdaily},
            config.tpResolution)
        
        #12.3.3...Set up hydro pumped constraints for daily storages
        hydro.set_hydro_power_Pumped_daily(hydro_Pumped_CH_daily,
            {k:0 for k in hydro_Pumped_CH_daily}, #Emin is 0 in database
            {k:self.generators[k]['Emax']/baseMVA for k in hydro_Pumped_CH_daily},
            {k:self.generators[k]['Pmax']/baseMVA for k in hydro_Pumped_CH_daily},
            {k:-self.generators[k]['Pmin']/baseMVA for k in hydro_Pumped_CH_daily},#we need the (-) because in the database Pmin is negative and the notation for maximum pump power HERE is positive
            {k:self.generators[k]['E_ini'] for k in hydro_Pumped_CH_daily}, #E_ini [-] so no need to divide by baseMVA
            {k:renewables_timeseries[k] for k in hydro_Pumped_CH_daily},
            {k:self.generators[k]['eta_ch'] for k in hydro_Pumped_CH_daily},
            {k:self.generators[k]['eta_dis'] for k in hydro_Pumped_CH_daily},
            config.tpResolution)
        #hydro.set_min_pumping(hydro_Pumped_CH, config.tpResolution)

        #12.3.4...Set up battery constraints
        existing_batteries = [item for item in batt if item not in candidates_batteries]
        batteries1 = BatteryStoragesExisting(opt, existing_batteries)
        batteries1.set_battery_daily(existing_batteries,
            {k:self.generators[k]['Emin']/baseMVA for k in existing_batteries},
            {k:self.generators[k]['Emax']/baseMVA for k in existing_batteries},
            {k:self.generators[k]['Pmax']/baseMVA for k in existing_batteries},
            {k:-self.generators[k]['Pmin']/baseMVA for k in existing_batteries},#we need the (-) because in the database Pmin is negative and the notation for maximum charge power HERE is positive
            {k:self.generators[k]['E_ini'] for k in existing_batteries}, #E_ini [-] so no need to divide by baseMVA
            {k:self.generators[k]['eta_ch'] for k in existing_batteries},
            {k:self.generators[k]['eta_dis'] for k in existing_batteries},
            config.battDIS,
            1) #config.tpResolution
        batteries2 = BatteryStoragesInvest(opt, candidates_batteries)
        batteries2.set_battery_daily_invest({k:self.generators[k]['Emin']/baseMVA for k in candidates_batteries},
            {k:self.generators[k]['Emax']/baseMVA for k in candidates_batteries},
            {k:self.generators[k]['Pmax']/baseMVA for k in candidates_batteries},
            {k:-self.generators[k]['Pmin']/baseMVA for k in candidates_batteries},#we need the (-) because in the database Pmin is negative and the notation for maximum pump power HERE is positive
            {k:0 for k in candidates_batteries}, #E_ini [-] so no need to divide by baseMVA
            {k:self.generators[k]['eta_ch'] for k in candidates_batteries},
            {k:self.generators[k]['eta_dis'] for k in candidates_batteries},
            config.battDIS,
            1) #config.tpResolution

        #12.3.5...Set up p2x constraints for candidate p2x units
        existing_p2x = [item for item in p2x if item not in candidates_P2X] #existing p2x should be later incorporated in the P2XInvest class to simplify the handling of the CH4 and H2 requirements
        candidates_P2X_notH2connected = []
        for key, row in enumerate(self.generators):
            if key in candidates_P2X:
                if row['Ind_h2_MarketConnect'] != 1: #these are p2x units not connected to the EU H2 backbone
                    candidates_P2X_notH2connected.append(key)
        if candidates_P2X:
            P2X1 = P2XInvest(opt, candidates_P2X, candidates_P2X_notH2connected)
            P2X1.set_P2X_invest({k:self.generators[k]['Pmax']/baseMVA for k in candidates_P2X},
                {k:-self.generators[k]['Pmin']/baseMVA for k in candidates_P2X},
                {k:self.generators[k]['Pmax_methdac']/baseMVA for k in candidates_P2X},
                {k:self.generators[k]['Emax_h2stor']/baseMVA for k in candidates_P2X})
            P2X1.set_H2_balance(config.tpResolution, 
                config.maxdailyH2withdrawal_p2g2p,
                config.maxdailyH2injection_p2g2p)
            P2X1.set_H2_constraints({k:self.generators[k]['Conv_elzr'] for k in candidates_P2X},
                {k:self.generators[k]['Conv_fc'] for k in candidates_P2X})
            P2X1.set_CH4_constraints({k:self.generators[k]['Conv_methdac_h2'] for k in candidates_P2X},
                {k:self.generators[k]['Conv_methdac_el'] for k in candidates_P2X}, 
                config.CH4pmin)
            P2X1.set_H2_importlimit_inequality(candidates_P2X)
            P2X1.set_CH4_importlimit_inequality(candidates_P2X)
            if self.H2profileId is None or np.isnan(self.H2profileId):
                P2X1.set_H2_annually_demand_inequality(candidates_P2X,
                    config.targetH2/baseMVA,
                    config.tpResolution)
            else:
                P2X1.set_H2_hourly_demand_inequality(candidates_P2X,
                    {k:H2demand_timeseries[k]['H2Demand']/baseMVA for k in range(self.timeperiods)})
            if self.CH4profileId is None or np.isnan(self.CH4profileId):
                # The synthetic gas target is in GWh so x 1000
                P2X1.set_CH4_annually_demand_inequality(candidates_P2X,
                    config.targetCH4*1000/baseMVA,
                    config.tpResolution)
            else:
                P2X1.set_CH4_hourly_demand_inequality(candidates_P2X,
                    {k:CH4demand_timeseries[k]['CH4Demand']*1000/baseMVA for k in range(self.timeperiods)})
        else:
            P2X1 = P2XInvest_Empty(opt, candidates_P2X, candidates_P2X_notH2connected) 

        #set end conditions for storage level during each month (for validation purposes)
        #if self.ScenarioYear == 2015 and config.tpResolution == 1:
        #    hydro_CH = hydro_Dam_CH + hydro_Pumped_CH_notdaily
        #    monthly_end_conditions = [24 * numOfDay - 1 for numOfDay in [31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]] #we count from 0 and not 1
        #    monthly_level = [3995000, 2217000, 1303000, 1034000, 2309000, 4360000, 6027000, 7381000, 7781000, 6763000, 5342000, 4204000] #in mwh from bfe for 2015
        #    hydro.set_SoC_validation(hydro_CH, 
        #        {k:monthly_end_conditions[k] for k in range(len(monthly_end_conditions))}, #these are the final hours of the month
        #        {k:monthly_level[k]/baseMVA for k in range(len(monthly_level))})

        #12.3.6...Set up NET constraints for candidate NET units
        existing_net = [item for item in net if item not in candidates_NET]
        if candidates_NET:
            NET1 = NETInvest(opt, candidates_NET)
            NET1.set_NET_invest({k:-self.generators[k]['Pmin']/baseMVA for k in candidates_NET}) #we need the (-) because in the database Pmin is negative and the notation for maximum NET technology power HERE is positive  
        else:
            NET1 = NETInvest_Empty(opt, candidates_NET)        
        
        duration_log_dict['0_to_12-3'] = (time.time() - time_restarted)/60
        logging.debug('Within GetOptimization, time between 0 to 12.3: %f minutes' %duration_log_dict['0_to_12-3'])
        time_restarted = time.time()

        #12.4...Set up RES target constraint + investment constraints for RES units, i.e. candidates_nondisp
        investment = InvestGenerators(opt, candidates=[], candidates_nondisp=candidates_nondisp)

        print("candidates_nondisp:", candidates_nondisp)
        
        investment.set_operation_nondisp_simple({k:self.generators[k]['Pmax']/baseMVA for k in candidates_nondisp},
            {k:renewables_timeseries[k] for k in candidates_nondisp},
            baseMVA)
        
        original_target = copy.deepcopy(config.targetRES) #copy of the original RES target
        if config.targetRES - config.distIvRESproduction > 0: 
            config.targetRES = (config.targetRES - round(config.distIvRESproduction, 6))*1e6 
        else:
            config.targetRES = 0
        print('-----RES Target after DistIv---------------------')
        print('     Total RES target is %f' %(original_target) + ' TWh')
        print('     DistIv RES gen is %f' %(config.distIvRESproduction) + ' TWh')
        print('     DistIv contributed RES target is %f' %(round(config.distIvRESproduction, 6)*1e6) + ' MWh')
        print('     Remaining RES target is %f' %(config.targetRES) + ' MWh')
        print('-------------------------------------------------')
        print('')

        candidates_biomass = [item for item in candidates if item in conv_biomass_CH]
        candidates_geothermal = [item for item in candidates if item in conv_geothermal_CH]
        candidatesRES_CH = sorted(candidates_nondisp + candidates_biomass + candidates_geothermal)
        
        existing_biomass_CH = [item for item in conv_biomass_CH if item not in candidates]
        existing_geothermal_CH = [item for item in conv_geothermal_CH if item not in candidates]
        existing_pv_CH = [item for item in pv_CH if item not in candidates_nondisp]
        existing_wind_CH = [item for item in wind_CH if item not in candidates_nondisp]
        existingRES_CH = sorted(existing_biomass_CH + existing_geothermal_CH + existing_pv_CH + existing_wind_CH)
        
        allRES_CH = sorted(candidatesRES_CH + existingRES_CH) #these are all non-hydro RES generators in Switzerland (both existing and candidate) 
        
        #12.5...Set reserves per generator type
        all_gens = sorted(conv + hydro_Dam + hydro_Pumped + hydro_RoR + pv + wind + batt + p2x + net)
        #print(len(all_gens))
        reserves = Reserves(opt, all_gens)

        #we have no reserves for these generators (they are either non-swiss or non-dispatchable)
        no_reserves_generators = list(set(non_swiss_gens + non_dispatchable_gens + candidates_batteries + p2x + net))
        reserves.set_no_reserves(no_reserves_generators)

        hydro.set_hydro_power_dam_reserves(reserves, hydro_Dam_CH, config.tpResolution)
        hydro.set_hydro_power_Pumped_reserves(reserves, hydro_Pumped_CH_notdaily, config.tpResolution)
        hydro.set_hydro_power_Pumped_daily_reserves(reserves, hydro_Pumped_CH_daily, config.tpResolution)
        cg.set_FRR_RR(reserves, GENS=nuclear_CH_exist)
        cg_CH_noUC.set_FRR_RR_Linear(reserves, GENS=conv_CH_and_biomassCH_and_geothermalCH)
        CH_nuclear_invest.set_FRR_RR_Nuclear(reserves, GENS=nuclear_CH_candidate)
        existing_batteries_CH = [item for item in existing_batteries if item not in non_swiss_gens]
        batteries1.set_battery_daily_reserves(reserves, existing_batteries_CH, config.tpResolution)
        batteries2.set_battery_daily_reserves_invest(reserves, candidates_batteries, config.tpResolution)

        #12.6...Set up total system reserve constraints
        reserves.set_system_reserve_constraints(
            system_up_requirements_FRR={k:reserves_timeseries[k]['FRRupReq']/baseMVA for k in range(self.timeperiods)},
            system_down_requirements_FRR={k:reserves_timeseries[k]['FRRdnReq']/baseMVA for k in range(self.timeperiods)},
            system_up_requirements_RR={k:reserves_timeseries[k]['RRupReq']/baseMVA for k in range(self.timeperiods)},
            system_down_requirements_RR={k:reserves_timeseries[k]['RRdnReq']/baseMVA for k in range(self.timeperiods)},
            system_up_additional_RR=investment.additional_up_RR(list_wind_cand, 
                                                                config.windUP, 
                                                                list_pv_cand,
                                                                config.distIvPVinstalled/baseMVA, 
                                                                config.solarUP),
            system_down_additional_RR=investment.additional_down_RR(list_wind_cand, 
                                                                config.windDOWN,
                                                                list_pv_cand,
                                                                config.distIvPVinstalled/baseMVA,
                                                                config.solarDOWN))        
        duration_log_dict['12-4_to_12-6'] = (time.time() - time_restarted)/60
        logging.debug('Within GetOptimization, time for doing steps 12.4-12.6: %f minutes' %duration_log_dict['12-4_to_12-6'])
        time_restarted = time.time()

        #13.Set up the network for DC Power Flow (here we have all constraints related to the grid)
        line_idFromBus = [int(self.lines[k]['idFromBus']) for k in self.line_id]
        line_idToBus = [int(self.lines[k]['idToBus']) for k in self.line_id]
        line_type = [str(self.lines[k]['line_type']) if "line_type" in self.lines[k] else "AC" for k in self.line_id]
        line_loss_factor = [int(self.lines[k]['loss_factor']) if "loss_factor" in self.lines[k] else 0.0 for k in self.line_id]
        gen_idBus = [int(self.gens_busnodes[k]["idIntBus"]) for k in self.generator_id]

        #13.1...Set DC power flow constraints and objective function
        combined_list = sorted(conv_not_CH + pv + wind + batt + conv_CH_ALL + hydro_RoR + hydro_Dam + hydro_Pumped) #for all of these generator ids we use get_operational_costs_disagg
        
        swiss_gens = []
        swiss_NET_gens = []
        ALL_swiss_gens = []
        for Id, row in enumerate(self.generators):
            if row['Country'] == 'CH':
                if row['GenType'] == 'NET':
                    swiss_NET_gens.append(Id)
                else:
                    swiss_gens.append(Id)
        ALL_swiss_gens = swiss_gens + swiss_NET_gens
        
        nodemand_buses = []
        for _, row in load_timeseries.items():
            result = dict((key, value) for key, value in row.items() if value < 0) 
            nodemand_buses.append(result)

        #find cross-border lines --> necessary if config.equalexportsimports_required == 1
        cross_border_lines_CH = []
        for Id, row in enumerate(self.lines):
            if row['tapRatio'] == 1: #this is a line (transformers have tapRatios != 1)
                if row['Ind_CrossBord'] == 0:
                    if row['FromCountry'] == 'AT' and row['ToCountry'] == 'AT':
                        cross_border_lines_CH.append(Id)
                    if row['FromCountry'] == 'DE' and row['ToCountry'] == 'DE':
                        cross_border_lines_CH.append(Id)
                    if row['FromCountry'] == 'FR' and row['ToCountry'] == 'FR':
                        cross_border_lines_CH.append(Id)
                    if row['FromCountry'] == 'IT' and row['ToCountry'] == 'IT':
                        cross_border_lines_CH.append(Id)
        
        lines_NTC = []
        for Id, row in enumerate(self.lines):
            if row['line_type'] == "NTC":
                print(row)
                print()
                lines_NTC.append(Id)


        
        all_trafo_ids = []
        all_line_ids = []
        for Id, row in enumerate(self.lines):
            if 'TrafoName' in row:
                all_trafo_ids.append(Id)
            else:
                all_line_ids.append(Id)
        OHL_candidates = [item for item in all_line_ids if item in candidate_lines]
        trafo_candidates = [item for item in all_trafo_ids if item in candidate_lines]


        no_loadshift_buses_test = [k for k in range(len(self.bus_id))]
        print('-----Transmission Expansion----------------------')
        if candidate_lines:
            print('.... Detected candidate lines in the network. Turning on transmission expansion planning...')
            DCPF = DC_PowerFlow_TrafoFlex_Expansion(opt, num_nodes = len(self.bus_id), num_lines=len(self.line_id), no_load_shift_buses=[], candidate_lines=candidate_lines, existing_lines=existing_lines, fixed_lines_values=fixed_lines_values)
            # DCPF = DC_PowerFlow_Trafo_Expansion(opt, num_nodes = len(self.bus_id), num_lines=len(self.line_id), candidate_lines=candidate_lines, existing_lines=existing_lines, fixed_lines_values=fixed_lines_values)
        else: 
            print('.... No candidate lines detected...')
            if config.single_electric_node: #lines is equal to union of lines_NTC and cross_border_lines_CH
                lines = list(set(lines_NTC + cross_border_lines_CH)) 
            else:
                lines = self.line_id

            DCPF = DC_PowerFlow_TrafoFlex(opt, num_nodes = len(self.bus_id), lines=lines, no_load_shift_buses=[]) # no_loadshift_buses_test
            #DCPF = DC_PowerFlow_Trafo(opt, num_nodes = len(self.bus_id), num_lines=len(self.line_id))

        print('-------------------------------------------------')
        print('')

        print('-----Other Info----------------------------------')

        # 14.Set up the objective function
        if any(len(x) != 0 for x in nodemand_buses):
            print('.... There are buses with negative demand')
            #if negative values in load_timeseries, use set_dc_bus_distIv_injection in combination with connect_buses_distIv_injection
            #set the bus demand, slack bus
            DCPF.set_dc_bus_distIv_injection(bus_demand = {k:load_timeseries[k] for k in self.bus_id}, #no need to divide the load by baseMVA
                        slack_bus = SlackBusId[0], # we take the first bus in the slack bus list
                        baseMVA = baseMVA)
            #set the line parameters (reactances and line limits)
            # set line parameters for different line types
            DCPF.set_line(reactance={k:self.lines[k]['x'] for k in self.line_id},
                        tap_ratio={k:self.lines[k]['tapRatio'] for k in self.line_id},
                        line_power_limit_forward_direction={k:self.lines[k]['rateA']/baseMVA for k in self.line_id},
                        line_power_limit_backward_direction={k:self.lines[k]['rateA2']/baseMVA if "rateA2" in self.lines[k] else self.lines[k]['rateA']/baseMVA for k in self.line_id},
                        type={k:self.lines[k]['line_type'] if "line_type" in self.lines[k] else "AC" for k in self.line_id})
            #set flexibility constraints
            DCPF.set_flexibility(
                max_shift_hourly={k: dsm_pshift_hourly.get(k, 0) / baseMVA for k in self.bus_id},
                max_shift_daily={k: dsm_eshift_daily.get(k, 0) / baseMVA for k in self.bus_id},
                max_up_shift_hourly_emob={k: {t: emob_pupshift_hourly.get(k, [0.0]*int(self.timeperiods))[t] / baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                max_down_shift_hourly_emob={k: {t: emob_pdownshift_hourly.get(k, [0.0]*int(self.timeperiods))[t] / baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                max_shift_daily_emob={k: {t: emob_eshift_daily.get(k, [0.0]*int(self.timeperiods//24))[t] / baseMVA for t in range(int(self.timeperiods//24))} for k in self.bus_id},
                emob_demand={k: self.adjusted_emobilityload.get(k, [0.0]*int(self.timeperiods)) for k in self.bus_id},  # <-- series fallback
                max_up_shift_hourly_heatpump={k: {t: heatpump_ecumulmax_hourly.get(k, [0.0]*int(self.timeperiods))[t] / baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                max_down_shift_hourly_heatpump={k: {t: heatpump_ecumulmin_hourly.get(k, [0.0]*int(self.timeperiods))[t] / baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                max_p_hourly_heatpump={k: heatpump_pmax_hourly.get(k, 0) / baseMVA for k in self.bus_id},
                heatpump_demand={k: self.adjusted_heatpumpload.get(k, [0.0]*int(self.timeperiods)) for k in self.bus_id},  # <-- series fallback
                baseMVA=baseMVA,
                num_days=self.days,
                tpRes=config.tpResolution
            )
            
            #nodal balance at each bus
            DCPF.connect_buses_distIv_injection(line_start=line_idFromBus, line_end=line_idToBus, type=line_type, gen_nodes=gen_idBus, loss_factor = line_loss_factor)
            
            # TSO-DSO trafo power limit
            if config.alpha_ex_CentIv > 0 and config.enableTSODSOlimit:
                print(".....TSO-DSO flows are limited with alpha: " + str(config.alpha_ex_CentIv))
                DCPF.limit_tso_dso_flows(GENS=dso, gen_nodes=gen_idBus, trafo_limit_mf=config.alpha_ex_CentIv)
            else:
                print(".....TSO-DSO flows are NOT limited")
            #set total annual exports to equal total annual imports
            if config.equalexportsimports_required: 
                DCPF.set_equal_annual_exportimport(cross_border_lines_CH, config.tpResolution)
            if config.winterNetImport != 0:
                DCPF.set_net_winter_import_limit(
                    cross_border_lines_CH, 
                    config.tpResolution, 
                    config.winterNetImport)

            print("     Current RES Target is {}"
                .format("disabled" if config.disableREStarget else "enabled"))            
            if not config.disableREStarget:
                DCPF.set_RES_target_DistIv(
                    conv_biomass_and_geothermal,
                    candidates_nondisp,
                    existing_pv_CH, existing_wind_CH,
                    config.targetRES,
                    config.tpResolution,
                    baseMVA)
            print("PV generators: ", pv_CH)
            print("Existing PV generators: ", pv_existing)
            print("Existing CH PV generators: ", existing_pv_CH)

            if config.disableREStarget and (config.targetRESPV != 0):
                print("     WARNING: RES target is disabled but rooftop PV RES target is not 0")
            config.targetRESPV = config.targetRESPV * 1e6
            print('     Rooftop PV target is %f' %(config.targetRESPV) + ' MWh')
            #print('Rooftop PV target is:')
            #print(config.targetRESPV)
            #Set PF rooftop constraint
            rooftop_pv_CH = [item for item in rooftop_pv if item not in non_swiss_gens]
            rooftop_pv_CH_cand = [item for item in rooftop_pv_CH if item in candidates_nondisp]
            rooftop_pv_CH_exist = [item for item in rooftop_pv_CH if item not in candidates_nondisp]
            if base_rooftop_PV_target_on_potential_generation:
                DCPF.set_rooftop_PV_target_potential(
                    rooftop_pv_CH_exist,
                    rooftop_pv_CH_cand,
                    config.targetRESPV,
                    config.tpResolution,
                    baseMVA,
                    oversize_factor=1.009
                )
            else:
                # this sets the target based on the actual generation
                DCPF.set_rooftop_PV_target(
                    rooftop_pv_CH,
                    config.targetRESPV,
                    config.tpResolution,
                    baseMVA,
                    oversize_factor=1.009
                )
            #Set CO2 limits
            opt.set_co2limit(swiss_gens, swiss_NET_gens, {k:self.generators[k]['CO2Rate'] for k in ALL_swiss_gens}, config.targetCO2, config.tpResolution, baseMVA)
            #14.XSet up the objective function
            #The objective function is nested within the DC power flow formulation because depending on whether we have
            #negative demand, i.e. distiv injections, we need to assign a cost to the injections the TSO uses from the DSO
            opt.set_objective_function(
                opt.get_operational_costs_disagg(combined_list,
                    {k:fuelprice_timeseries[k] for k in combined_list},
                    {k:self.generators[k]['GenEffic'] for k in combined_list},
                    {k:co2price_timeseries[k] for k in combined_list},
                    {k:self.generators[k]['CO2Rate'] for k in combined_list}, 
                    {k:self.generators[k]['VOM_Cost'] for k in combined_list},
                    config.tpResolution, 
                    baseMVA)                                                         +
                P2X1.get_operational_costs_P2G2P_disagg(candidates_P2X,
                    {k:fuelprice_timeseries[k] for k in candidates_P2X},
                    {k:self.generators[k]['GenEffic'] for k in candidates_P2X},
                    {k:co2price_timeseries[k] for k in candidates_P2X},
                    {k:self.generators[k]['CO2Rate'] for k in candidates_P2X},
                    {k:self.generators[k]['VOM_Cost'] for k in candidates_P2X},
                    {k:self.generators[k]['VOM_methdac'] for k in candidates_P2X},
                    config.tpResolution, 
                    baseMVA)                                                                   +
                NET1.get_operational_costs_NET_disagg(candidates_NET,
                    {k:fuelprice_timeseries[k] for k in candidates_NET},
                    {k:self.generators[k]['GenEffic'] for k in candidates_NET},
                    {k:co2price_timeseries[k] for k in candidates_NET},
                    {k:self.generators[k]['CO2Rate'] for k in candidates_NET}, 
                    {k:self.generators[k]['VOM_Cost'] for k in candidates_NET},
                    config.tpResolution, 
                    baseMVA)                                                         +                    
                P2X1.get_import_costs_P2G2P_disagg(candidates_P2X,  
                    {k:h2importprice_timeseries[k] for k in candidates_P2X},
                    {k:ch4importprice_timeseries[k] for k in candidates_P2X},
                    config.tpResolution, 
                    baseMVA)                                                                   + 
                DCPF.get_lossload_cost(config.loadShedding_cost*baseMVA, 
                    config.tpResolution)                                                         +
                cg.get_operational_costs_conv_disagg(nuclear_CH_exist,
                    {k:self.generators[k]['StartCost'] for k in nuclear_CH_exist},
                    {k:fuelprice_timeseries[k] for k in nuclear_CH_exist},
                    {k:self.generators[k]['GenEffic'] for k in nuclear_CH_exist},
                    {k:co2price_timeseries[k] for k in nuclear_CH_exist},
                    {k:self.generators[k]['CO2Rate'] for k in nuclear_CH_exist},
                    {k:self.generators[k]['VOM_Cost'] for k in nuclear_CH_exist},
                    config.tpResolution, 
                    baseMVA)                                                                   +
                cg_CH_noUC.get_investment_cost_convCHlinear({k:self.generators[k]['Pmax']/baseMVA for k in candidates_nonuclear},
                    {k:self.generators[k]['InvCost']*baseMVA for k in candidates_nonuclear},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_nonuclear})            +
                CH_nuclear_invest.get_investment_cost_convCHNuclear({k:self.generators[k]['Pmax']/baseMVA for k in nuclear_CH_candidate},
                    {k:self.generators[k]['InvCost']*baseMVA for k in nuclear_CH_candidate},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in nuclear_CH_candidate})            +
                investment.get_invcost_nondisp_simple({k:self.generators[k]['InvCost']*baseMVA for k in candidates_nondisp}, 
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_nondisp})              +
                NET1.get_investment_cost_NET({k:-self.generators[k]['Pmin']/baseMVA for k in candidates_NET},
                    {k:self.generators[k]['InvCost']*baseMVA for k in candidates_NET},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_NET})            +                     
                DCPF.get_investment_cost_trafoline({k:self.lines[k]['CandCost'] for k in trafo_candidates}, 
                    {k:self.lines[k]['CandCost'] for k in OHL_candidates},
                    {k:self.lines[k]['length'] for k in OHL_candidates},
                    {k:self.lines[k]['rateA'] for k in trafo_candidates},
                    trafo_candidates,
                    OHL_candidates)                        +
                DCPF.get_cost_distIv_injection({k:self.buses[k]['PayInjection']*baseMVA for k in self.bus_id}, 
                    config.tpResolution)                                                         +
                batteries2.get_investment_cost_batt({k:self.generators[k]['InvCost']*baseMVA for k in candidates_batteries},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_batteries})  +
                P2X1.get_investment_cost_P2X({k:self.generators[k]['InvCost']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['InvCost_Charge']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['InvCost_h2stor']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['InvCost_methdac']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['FOM_elzr']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['FOM_h2stor']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['FOM_methdac']*baseMVA for k in candidates_P2X}, 
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_P2X})        - 
                P2X1.get_CH4_revenue(candidates_P2X, 
                    {k:fuelpriceSELL_timeseries[k] for k in candidates_P2X}, 
                    config.tpResolution,
                    baseMVA)                                                         - 
                P2X1.get_H2_revenue(candidates_P2X, 
                    {k:h2priceSELL_timeseries[k] for k in candidates_P2X}, 
                    config.tpResolution, 
                    baseMVA)                                                         - 
                P2X1.get_CO2_revenue(candidates_P2X, 
                    {k:co2price_timeseries[k] for k in candidates_P2X}, 
                    {k:self.generators[k]['Conv_methdac_co2'] for k in candidates_P2X},
                    config.tpResolution, 
                    baseMVA))
        else:
            print('.... No buses with negative demand')
            #if no negative values in load_timeseries, use set_dc_bus in combination with connect_buses
            #set the bus demand, slack bus
            DCPF.set_dc_bus(bus_demand = {k:load_timeseries[k] for k in self.bus_id}, #no need to divide the load by baseMVA
                        slack_bus = SlackBusId[0], # we take the first bus in the slack bus list
                        baseMVA = baseMVA,
                        single_electric_node = config.single_electric_node, # if True, we assume that there is only one electric node in the network
            )
            #set the line parameters (reactances and line limits)
            DCPF.set_line(reactance={k:self.lines[k]['x'] for k in self.line_id},
                        tap_ratio={k:self.lines[k]['tapRatio'] for k in self.line_id},
                        line_power_limit_forward_direction={k:self.lines[k]['rateA']/baseMVA for k in self.line_id},
                        line_power_limit_backward_direction={k:self.lines[k]['rateA2']/baseMVA if "rateA2" in self.lines[k] else self.lines[k]['rateA']/baseMVA for k in self.line_id},
                        type={k:self.lines[k]['line_type'] if "line_type" in self.lines[k] else "AC" for k in self.line_id})
            #set flexibility constraints
            # added the differentiation between up and down flexibility
            DCPF.set_flexibility(max_shift_hourly={k:dsm_pshift_hourly[k]/baseMVA for k in self.bus_id},
                        max_shift_daily={k:dsm_eshift_daily.get(k, 0)/baseMVA for k in self.bus_id},
                        max_up_shift_hourly_emob={k:{t:emob_pupshift_hourly.get(k, [0.0]*int(self.timeperiods))[t]/baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                        max_down_shift_hourly_emob={k:{t:emob_pdownshift_hourly.get(k, [0.0]*int(self.timeperiods))[t]/baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                        max_shift_daily_emob={k:{t:emob_eshift_daily.get(k, [0.0]*int(self.timeperiods//24))[t]/baseMVA for t in range(int(self.timeperiods//24))} for k in self.bus_id},
                        emob_demand={k:self.adjusted_emobilityload.get(k, [0.0]*int(self.timeperiods)) for k in self.bus_id},
                        max_up_shift_hourly_heatpump={k:{t:heatpump_ecumulmax_hourly.get(k, [0.0]*int(self.timeperiods))[t]/baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                        max_down_shift_hourly_heatpump={k:{t:heatpump_ecumulmin_hourly.get(k, [0.0]*int(self.timeperiods))[t]/baseMVA for t in range(int(self.timeperiods))} for k in self.bus_id},
                        max_p_hourly_heatpump = {k:heatpump_pmax_hourly.get(k, 0)/baseMVA for k in self.bus_id},
                        heatpump_demand={k:self.adjusted_heatpumpload.get(k, [0.0]*int(self.timeperiods)) for k in self.bus_id},
                        baseMVA=baseMVA,
                        num_days=self.days, 
                        tpRes=config.tpResolution)
            #nodal balance at each bus
            DCPF.connect_buses(line_start=line_idFromBus, line_end=line_idToBus, type = line_type, gen_nodes=gen_idBus, loss_factor = line_loss_factor, single_electric_node=config.single_electric_node, buses_CH = buses_CH, bus_neighbor_NTC_rep = bus_neighbor_NTC_rep, bus_neighbor_no_NTC_rep = bus_neighbor_no_NTC_rep)
            # TSO-DSO trafo power limit
            if config.alpha_ex_CentIv > 0 and config.enableTSODSOlimit:
                print(".....TSO-DSO flows are limited with alpha: " + str(config.alpha_ex_CentIv))
                DCPF.limit_tso_dso_flows(GENS=dso, gen_nodes=gen_idBus, trafo_limit_mf=config.alpha_ex_CentIv)
            else:
                print(".....TSO-DSO flows are NOT limited")
            #set total annual exports to equal total annual imports
            if config.equalexportsimports_required:
                DCPF.set_equal_annual_exportimport(cross_border_lines_CH, config.tpResolution)
            if config.winterNetImport != 0:
                DCPF.set_net_winter_import_limit(
                    cross_border_lines_CH, 
                    config.tpResolution, 
                    config.winterNetImport)

            print("     Current RES Target is {}"
                .format("disabled" if config.disableREStarget else "enabled"))
            if not config.disableREStarget:
                if not config.include_nuclear_in_RES_target:
                    DCPF.set_RES_target(
                        conv_biomass_and_geothermal,
                        candidates_nondisp,
                        existing_pv_CH, existing_wind_CH,
                        config.targetRES,
                        config.tpResolution,
                        baseMVA)
                else:
                    logging.warning('Including nuclear in RES target calculation!')
                    DCPF.set_PROD_target(
                        conv_biomass_and_geothermal,
                        candidates_nondisp,
                        nuclear_CH_candidate,
                        existing_pv_CH, existing_wind_CH,
                        config.targetRES,
                        config.tpResolution,
                        baseMVA)
                        
            print("PV generators: ", pv_CH)
            print("Existing PV generators: ", pv_existing)
            print("Existing CH PV generators: ", existing_pv_CH)

            if config.disableREStarget and (config.targetRESPV != 0):
                print("     WARNING: RES target is disabled but rooftop PV RES target is not 0")
            config.targetRESPV = config.targetRESPV * 1e6
            print('     Rooftop PV target is %f' %(config.targetRESPV) + ' MWh')
            #print('Rooftop PV target is:')
            #print(config.targetRESPV)
            #Set PF rooftop constraint
            rooftop_pv_CH = [item for item in rooftop_pv if item not in non_swiss_gens]
            rooftop_pv_CH_cand = [item for item in rooftop_pv_CH if item in candidates_nondisp]
            rooftop_pv_CH_exist = [item for item in rooftop_pv_CH if item not in candidates_nondisp]
            if base_rooftop_PV_target_on_potential_generation:
                DCPF.set_rooftop_PV_target_potential(
                    rooftop_pv_CH_exist,
                    rooftop_pv_CH_cand,
                    config.targetRESPV,
                    config.tpResolution,
                    baseMVA,
                    oversize_factor=1.009
                )
            else:
                # this sets the target based on the actual generation
                DCPF.set_rooftop_PV_target(
                    rooftop_pv_CH,
                    config.targetRESPV,
                    config.tpResolution,
                    baseMVA,
                    oversize_factor=1.009
                )
            #Set CO2 limits
            opt.set_co2limit(swiss_gens, swiss_NET_gens, {k:self.generators[k]['CO2Rate'] for k in ALL_swiss_gens}, config.targetCO2, config.tpResolution, baseMVA)
            #14.XSet up the objective function
            #The objective function is nested within the DC power flow formulation because depending on whether we have
            #negative demand, i.e. distiv injections, we need to assign a cost to the injections the TSO uses from the DSO
            opt.set_objective_function(
                opt.get_operational_costs_disagg(combined_list,
                    {k:fuelprice_timeseries[k] for k in combined_list},
                    {k:self.generators[k]['GenEffic'] for k in combined_list},
                    {k:co2price_timeseries[k] for k in combined_list},
                    {k:self.generators[k]['CO2Rate'] for k in combined_list}, 
                    {k:self.generators[k]['VOM_Cost'] for k in combined_list},
                    config.tpResolution, 
                    baseMVA)                                                                   +
                P2X1.get_operational_costs_P2G2P_disagg(candidates_P2X,
                    {k:fuelprice_timeseries[k] for k in candidates_P2X},
                    {k:self.generators[k]['GenEffic'] for k in candidates_P2X},
                    {k:co2price_timeseries[k] for k in candidates_P2X},
                    {k:self.generators[k]['CO2Rate'] for k in candidates_P2X},
                    {k:self.generators[k]['VOM_Cost'] for k in candidates_P2X},
                    {k:self.generators[k]['VOM_methdac'] for k in candidates_P2X},
                    config.tpResolution, 
                    baseMVA)                                                                   +
                NET1.get_operational_costs_NET_disagg(candidates_NET,
                    {k:fuelprice_timeseries[k] for k in candidates_NET},
                    {k:self.generators[k]['GenEffic'] for k in candidates_NET},
                    {k:co2price_timeseries[k] for k in candidates_NET},
                    {k:self.generators[k]['CO2Rate'] for k in candidates_NET}, 
                    {k:self.generators[k]['VOM_Cost'] for k in candidates_NET},
                    config.tpResolution, 
                    baseMVA)                                                                   +                    
                P2X1.get_import_costs_P2G2P_disagg(candidates_P2X,  
                    {k:h2importprice_timeseries[k] for k in candidates_P2X},
                    {k:ch4importprice_timeseries[k] for k in candidates_P2X},
                    config.tpResolution, 
                    baseMVA)                                                                   +   
                DCPF.get_lossload_cost(config.loadShedding_cost*baseMVA, 
                    config.tpResolution)                                                         +
                cg.get_operational_costs_conv_disagg(nuclear_CH_exist,
                    {k:self.generators[k]['StartCost'] for k in nuclear_CH_exist},
                    {k:fuelprice_timeseries[k] for k in nuclear_CH_exist},
                    {k:self.generators[k]['GenEffic'] for k in nuclear_CH_exist},
                    {k:co2price_timeseries[k] for k in nuclear_CH_exist},
                    {k:self.generators[k]['CO2Rate'] for k in nuclear_CH_exist},
                    {k:self.generators[k]['VOM_Cost'] for k in nuclear_CH_exist},
                    config.tpResolution, 
                    baseMVA)                                                                   +
                cg_CH_noUC.get_investment_cost_convCHlinear({k:self.generators[k]['Pmax']/baseMVA for k in candidates_nonuclear},
                    {k:self.generators[k]['InvCost']*baseMVA for k in candidates_nonuclear},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_nonuclear})            +
                CH_nuclear_invest.get_investment_cost_convCHNuclear({k:self.generators[k]['Pmax']/baseMVA for k in nuclear_CH_candidate},
                    {k:self.generators[k]['InvCost']*baseMVA for k in nuclear_CH_candidate},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in nuclear_CH_candidate})            +
                investment.get_invcost_nondisp_simple({k:self.generators[k]['InvCost']*baseMVA for k in candidates_nondisp}, 
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_nondisp})              +
                NET1.get_investment_cost_NET({k:-self.generators[k]['Pmin']/baseMVA for k in candidates_NET},
                    {k:self.generators[k]['InvCost']*baseMVA for k in candidates_NET},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_NET})            +                    
                DCPF.get_investment_cost_trafoline({k:self.lines[k]['CandCost'] for k in candidate_lines},
                    {k:self.lines[k]['CandCost'] for k in OHL_candidates},
                    {k:self.lines[k]['length'] for k in OHL_candidates},
                    {k:self.lines[k]['rateA'] for k in trafo_candidates},
                    trafo_candidates,
                    OHL_candidates)                         +
                batteries2.get_investment_cost_batt({k:self.generators[k]['InvCost']*baseMVA for k in candidates_batteries},
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_batteries})  + 
                P2X1.get_investment_cost_P2X({k:self.generators[k]['InvCost']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['InvCost_Charge']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['InvCost_h2stor']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['InvCost_methdac']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['FOM_elzr']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['FOM_h2stor']*baseMVA for k in candidates_P2X},
                    {k:self.generators[k]['FOM_methdac']*baseMVA for k in candidates_P2X}, 
                    {k:self.generators[k]['FOM_Cost']*baseMVA for k in candidates_P2X})     - 
                P2X1.get_CH4_revenue(candidates_P2X, 
                    {k:fuelpriceSELL_timeseries[k] for k in candidates_P2X}, 
                    config.tpResolution,
                    baseMVA)                                                         - 
                P2X1.get_H2_revenue(candidates_P2X, 
                    {k:h2priceSELL_timeseries[k] for k in candidates_P2X}, 
                    config.tpResolution, 
                    baseMVA)                                                         - 
                P2X1.get_CO2_revenue(candidates_P2X, 
                    {k:co2price_timeseries[k] for k in candidates_P2X}, 
                    {k:self.generators[k]['Conv_methdac_co2'] for k in candidates_P2X},
                    config.tpResolution, 
                    baseMVA))

        duration_log_dict['13_14'] = (time.time() - time_restarted)/60
        logging.debug('Within GetOptimization, time for doing steps 13 and 14: %f minutes' %duration_log_dict['13_14'])
        time_restarted = time.time()

        print('-------------------------------------------------')
        print('')
        
        #15.Solve
        print('.... Calling gurobi')
        print('')
        """
        if config.tpResolution == 8 and config.idScenario == 5 and config.continvest_required == 0: #scenario 5 refers to year 2050 
            opt.solve_2050_tpRes8() #spends some time looking for a heuristic solution if the problem is a MILP
        elif config.continvest_required == 1 and config.contnuclear_required == 1:
            opt.solve_linear() #lower tolerance for barrier convergence (compared to solve_with_gap) + crossover is off
        elif config.continvest_required == 1 and config.idScenario == 4: 
            opt.solve_linear() #lower tolerance for barrier convergence (compared to solve_with_gap) + crossover is off
        elif config.continvest_required == 1 and config.idScenario == 5:
            opt.solve_linear() #lower tolerance for barrier convergence (compared to solve_with_gap) + crossover is off
        if candidate_lines:
            opt.solve_with_gap()
        else:
        """
        #opt.solve_with_gap()
        opt.solve_linear(threads=config.threads)
        #opt.solve_quadratic()

        print('')
        print('-------------------------------------------------')
        
        duration_log_dict['15'] = (time.time() - time_restarted)/60
        logging.debug('Within GetOptimization, time for solving the optimization problem, 15: %f minutes' %duration_log_dict['15'])
        time_restarted = time.time()
        saveVarParDualsCsv(opt.model, results_folder.replace("CentIv", "InvestmentRun"))        

        #16.Post Processing
        #16.0 Initiating and basic info extraction
        generators_result_list = copy.deepcopy(self.generators) #copy of the initital generators list
        lines_result_list = self.lines.copy() #copy of the updated list (#19.1) with all lines (branches)
        new_units_built = cg_CH_noUC.get_gens_built() #keys are candidate gens ids and values are between 0 and 1 (depending on whether we have binary investment decisions or continuous investment decisions)
        new_nuclearunits_built = CH_nuclear_invest.get_gens_built_nuclear()
        new_capacities_built = investment.get_capacity_built({k:self.generators[k]['Pmax']/baseMVA for k in candidates_nondisp}) #keys are candidate gens ids and values are capacities built (p.u baseMVA=100)
        new_batteries_built = batteries2.get_batteries_built() #keys are batt ids and values are between 0 and 1
        if candidates_P2X:
            new_P2G2P_H2electrolyzers_built = P2X1.get_H2_EL_inv() 
            new_P2G2P_H2storages_built = P2X1.get_H2_storage_inv()
            new_P2G2P_H2reconversion_built = P2X1.get_H2_gen_inv()
            new_P2G2P_CH4DAC_built = P2X1.get_CH4DAC_inv()
        if candidates_NET:
            new_NET_built = NET1.get_DAC_inv() #keys are DAC ids and values are between 0 and 1           
        if candidate_lines:
            new_lines_built = DCPF.get_lines_built()
            print('New transmission lines built:')
            print(new_lines_built)
        print('New non-dispatchable gens built:')  
        print(new_capacities_built)
        print('New dispatchable gens built:')  
        print(new_units_built)
        print('New nuclear gens built:')  
        print(new_nuclearunits_built)
        print('New utility-scale batteries built:')
        print(new_batteries_built)
        if candidates_P2X:
            print('New H2 electrolyzers:')
            print(new_P2G2P_H2electrolyzers_built)
            print('New H2 storages')
            print(new_P2G2P_H2storages_built)
            print('New H2 reconversion built:')
            print(new_P2G2P_H2reconversion_built)
            print('New CH4 reactor with DAC built:')
            print(new_P2G2P_CH4DAC_built)
        if candidates_NET:
            print('New NET (DAC) units built:')
            print(new_NET_built)            

        existing_units = []
        for Id, row in enumerate(generators_result_list):
            if row['CandidateUnit'] == 0: 
                existing_units.append(Id)
        existing_lines = list(set(self.line_id) - set(candidate_lines))

        res = SaveResults(results_folder=results_folder)

        numOfDays = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
        numOfHours = [24 * numOfDay for numOfDay in numOfDays]
        monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        #monthNames = [str(i) for i in range(12)]
        investment_cost_EUR = {} 
        fixed_operational_cost_EUR = {}
        tot_operational_cost_EUR = {}
        installed_Pmax = {}
        installed_Pmin = {}
        #for P2G2P
        installed_Pmin_el = {}
        installed_Pmax_recon = {}
        installed_Emax = {}
        installed_Pmax_methdac = {}
        investment_cost_electrolyzer_EUR = {}
        investment_cost_reconversion_EUR = {}
        investment_cost_storage_EUR = {}
        investment_cost_methdac_EUR = {}
        fom_electrolyzer_EUR = {}
        fom_reconversion_EUR = {}
        fom_storage_EUR = {}
        fom_methdac_EUR = {}
        
        """
        16.1 output: store newly built and existing generators
        """
        for Id, row in enumerate(generators_result_list):
            for i in existing_units:
                if Id == i:
                    investment_cost_EUR[Id] = 0
                    fixed_operational_cost_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*row['Pmax']
                    installed_Pmax[Id] = row['Pmax']
                    installed_Pmin[Id] = row['Pmin']
                    row.update({u'NewInvestment':0})
            for i,inv in new_units_built.items(): 
                if Id == i: 
                    investment_cost_EUR[Id] = row['InvCost']*inv*row['Pmax']
                    fixed_operational_cost_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*inv*row['Pmax']
                    installed_Pmax[Id] = row['Pmax']*inv
                    installed_Pmin[Id] = 0.0
                    if inv <= 0.001:
                        row.update({u'NewInvestment':0})
                    else: 
                        row.update({u'NewInvestment':1})
            for i,inv in new_nuclearunits_built.items(): 
                if Id == i: 
                    investment_cost_EUR[Id] = row['InvCost']*inv*row['Pmax']
                    fixed_operational_cost_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*inv*row['Pmax']
                    installed_Pmax[Id] = row['Pmax']*inv
                    installed_Pmin[Id] = 0.0
                    if inv <= 0.001:
                        row.update({u'NewInvestment':0})
                    else: 
                        row.update({u'NewInvestment':1})
            for i,inv in new_capacities_built.items():
                if Id == i:
                    investment_cost_EUR[Id] = row['InvCost']*round(inv,3)*baseMVA
                    fixed_operational_cost_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*round(inv,3)*baseMVA
                    installed_Pmax[Id] = round(inv,4)*baseMVA
                    installed_Pmin[Id] = row['Pmin']
                    if round(inv,3) <= 0.001:
                        row.update({u'NewInvestment':0})
                    else: 
                        row.update({u'NewInvestment':1})
            for i,inv in new_batteries_built.items():
                if Id == i:
                    investment_cost_EUR[Id] = row['InvCost']*inv*baseMVA
                    fixed_operational_cost_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*inv*baseMVA
                    installed_Pmax[Id] = inv*baseMVA
                    installed_Pmin[Id] = -inv*baseMVA
                    if inv <= 0.001:
                        row.update({u'NewInvestment':0})
                    else: 
                        row.update({u'NewInvestment':1})
            if candidates_P2X:
                for i,inv in new_P2G2P_H2electrolyzers_built.items():
                    if Id == i:
                        if inv <= 0.001:
                            investment_cost_electrolyzer_EUR[Id] = 0
                            fom_electrolyzer_EUR[Id] = 0
                            installed_Pmin_el[Id] = 0.0
                            row.update({u'InvCost_electrolyzer_built':investment_cost_electrolyzer_EUR[Id]})
                            row.update({u'FOMCost_electrolyzer_built':fom_electrolyzer_EUR[Id]})
                            row.update({u'Pmin_electrolyzer_built':installed_Pmin_el[Id]})
                        else: 
                            investment_cost_electrolyzer_EUR[Id] = row['InvCost_Charge']*inv*row['Pmin']*-1 #Pmin is negative
                            fom_electrolyzer_EUR[Id] = 0 if row['FOM_elzr'] is None else row['FOM_elzr']*inv*row['Pmin']*-1 #Pmin is negative
                            installed_Pmin[Id] = inv*row['Pmin']
                            row.update({u'InvCost_electrolyzer_built':investment_cost_electrolyzer_EUR[Id]})
                            row.update({u'FOMCost_electrolyzer_built':fom_electrolyzer_EUR[Id]})
                            row.update({u'Pmin_electrolyzer_built':installed_Pmin[Id]})
                for i,inv in new_P2G2P_H2storages_built.items():
                    if Id == i:
                        if inv <= 0.001:
                            investment_cost_storage_EUR[Id] = 0
                            fom_storage_EUR[Id] = 0
                            installed_Emax[Id] = 0.0
                            row.update({u'InvCost_H2storage_built':investment_cost_storage_EUR[Id]})
                            row.update({u'FOMCost_H2storage_built':fom_storage_EUR[Id]})
                            row.update({u'Emax_H2storage_built':installed_Emax[Id]})
                        else: 
                            investment_cost_storage_EUR[Id] = row['InvCost_h2stor']*inv*baseMVA
                            fom_storage_EUR[Id] = 0 if row['FOM_h2stor'] is None else row['FOM_h2stor']*inv*baseMVA
                            installed_Emax[Id] = inv*baseMVA
                            row.update({u'InvCost_H2storage_built':investment_cost_storage_EUR[Id]})
                            row.update({u'FOMCost_H2storage_built':fom_storage_EUR[Id]})
                            row.update({u'Emax_H2storage_built':installed_Emax[Id]})
                for i,inv in new_P2G2P_CH4DAC_built.items():
                    if Id == i:
                        if inv <= 0.001:
                            investment_cost_methdac_EUR[Id] = 0
                            fom_methdac_EUR[Id] = 0
                            installed_Pmax_methdac[Id] = 0.0
                            row.update({u'InvCost_methdac_built':investment_cost_methdac_EUR[Id]})
                            row.update({u'FOMCost_methdac_built':fom_methdac_EUR[Id]})
                            row.update({u'Pmax_DACMeth_built':installed_Pmax_methdac[Id]})
                        else: 
                            investment_cost_methdac_EUR[Id] = row['InvCost_methdac']*inv*row['Pmax_methdac']
                            fom_methdac_EUR[Id] = 0 if row['FOM_methdac'] is None else row['FOM_methdac']*inv*row['Pmax_methdac']
                            installed_Pmax_methdac[Id] = inv*row['Pmax_methdac']
                            row.update({u'InvCost_methdac_built':investment_cost_methdac_EUR[Id]})
                            row.update({u'FOMCost_methdac_built':fom_methdac_EUR[Id]})
                            row.update({u'Pmax_DACMeth_built':installed_Pmax_methdac[Id]})
                for i,inv in new_P2G2P_H2reconversion_built.items():
                    if Id == i:
                        if inv <= 0.001:
                            investment_cost_reconversion_EUR[Id] = 0
                            fom_reconversion_EUR[Id] = 0
                            installed_Pmax_recon[Id] = 0.0
                            row.update({u'InvCost_recon_built':investment_cost_reconversion_EUR[Id]})
                            row.update({u'FOMCost_recon_built':fom_reconversion_EUR[Id]})
                            row.update({u'Pmax_recon_built':installed_Pmax_recon[Id]})
                            investment_cost_EUR[Id] = 0 #we don't need these for P2G2P
                            fixed_operational_cost_EUR[Id] = 0 #we don't need these for P2G2P
                            installed_Pmax[Id] = 0.0 #we don't need these for P2G2P
                            installed_Pmin[Id] = 0.0 #we don't need these for P2G2P
                            row.update({u'NewInvestment':2}) #these units are separate
                        else: 
                            investment_cost_reconversion_EUR[Id] = row['InvCost']*inv*row['Pmax']
                            fom_reconversion_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*inv*row['Pmax']
                            installed_Pmax_recon[Id] = inv*row['Pmax']
                            row.update({u'InvCost_recon_built':investment_cost_reconversion_EUR[Id]})
                            row.update({u'FOMCost_recon_built':fom_reconversion_EUR[Id]})
                            row.update({u'Pmax_recon_built':installed_Pmax_recon[Id]})
                            investment_cost_EUR[Id] = 0 #we don't need these for P2G2P
                            fixed_operational_cost_EUR[Id] = 0 #we don't need these for P2G2P
                            installed_Pmax[Id] = 0.0 #we don't need these for P2G2P
                            installed_Pmin[Id] = 0.0 #we don't need these for P2G2P
                            row.update({u'NewInvestment':2}) #these units are separate
            if candidates_NET:
                for i,inv in new_NET_built.items(): 
                    if Id == i: 
                        investment_cost_EUR[Id] = row['InvCost']*inv*(-row['Pmin'])
                        fixed_operational_cost_EUR[Id] = 0 if row['FOM_Cost'] is None else row['FOM_Cost']*inv*(-row['Pmin'])
                        installed_Pmax[Id] = 0.0
                        installed_Pmin[Id] = row['Pmin']*inv
                        if inv <= 0.001:
                            row.update({u'NewInvestment':0})
                        else: 
                            row.update({u'NewInvestment':1})                
                           
            row.update({u'Tot_InvCost_CHF':investment_cost_EUR[Id]})#the investment costs of a generator  
            row.update({u'Tot_FOpCost_CHF':fixed_operational_cost_EUR[Id]})#the fixed operational costs of a generator  
            row.update({u'generatorPmax_MW':installed_Pmax[Id]})#the pmax of a generator
            row.update({u'generatorPmin_MW':installed_Pmin[Id]})#the pmin of a generator

        df = pd.DataFrame(generators_result_list)
        df_CH = df[(df.Country == 'CH')]#only generators at Swiss nodes

        """
        16.2 output: store all parameters of newly built lines
        """
        if candidate_lines:
            for Id, row in enumerate(lines_result_list):
                for i in existing_lines:
                    if Id == i:
                        row.update({u'NewInvestment':999})
                        row.update({u'NewInvestmentExp':999})
                for i,inv in new_lines_built.items():
                    if Id == i: 
                        if inv <= 0.49999: #less than 1/2 a line built is not considered as built
                            row.update({u'NewInvestmentExp':0})
                            row.update({u'NewInvestment':inv})
                        else: 
                            row.update({u'NewInvestmentExp':1})
                            row.update({u'NewInvestment':inv})
            df_lines = pd.DataFrame(lines_result_list)
            df_newInv_lines = df_lines[['idFromBus','idToBus','LineName','Rank','NewInvestment']]
            df_newInv_lines2 = df_lines[['idFromBus','idToBus','LineName','NewInvestmentExp']]
            df_newInv_lines = df_newInv_lines[(df_newInv_lines.NewInvestment <= 1)]
            df_newInv_lines2 = df_newInv_lines2[(df_newInv_lines2.NewInvestmentExp == 1)] #only newly built lines
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewLines.xlsx'
                ),
                engine='xlsxwriter'
            ) #contains the investment status of all candidate lines
            writer2 = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewLinesOnlyOneStatus.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_newInv_lines.to_excel(writer, sheet_name='CH')
            df_newInv_lines2.to_excel(writer2, sheet_name='CH')
            writer.close()
            writer2.close()
        else:
            column_names = ['idFromBus','idToBus','LineName','Rank','NewInvestment']
            df_lines_empty = pd.DataFrame(columns = column_names)
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewLines.xlsx'
                ),
                engine='xlsxwriter'
            )
            writer2 = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewLinesOnlyOneStatus.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_lines_empty.to_excel(writer, sheet_name='CH')
            df_lines_empty.to_excel(writer2, sheet_name='CH')
            writer.close()
            writer2.close()
        
        """
        16.3 output: new investments (to update MySQL database) 
        """
        #standard units (no P2G2P)
        df_newInv = df_CH[['GenName','generatorPmax_MW','generatorPmin_MW','Technology','NewInvestment']]
        df_newInv_withNode = df_CH[['GenName','BusName','generatorPmax_MW','generatorPmin_MW','Technology','NewInvestment']]
        df_newInv = df_newInv.rename(columns={'generatorPmax_MW':'Pmax'})
        df_newInv_withNode = df_newInv_withNode.rename(columns={'generatorPmax_MW':'Pmax'})
        df_newInv = df_newInv.rename(columns={'generatorPmin_MW':'Pmin'})
        df_newInv_withNode = df_newInv_withNode.rename(columns={'generatorPmin_MW':'Pmin'})
        df_newInv = df_newInv[(df_newInv.NewInvestment == 1)]#only newly built generators
        df_newInv_withNode = df_newInv_withNode[(df_newInv_withNode.NewInvestment == 1)]
        writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewUnits.xlsx'
                ),
                engine='xlsxwriter'
            )
        df_newInv.to_excel(writer, sheet_name='CH')
        writer.close()
        writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewUnitsWithNodeInfo.xlsx'
                ),
                engine='xlsxwriter'
            )
        df_newInv_withNode.to_excel(writer, sheet_name='CH')
        writer.close()
        #P2G2P units
        if candidates_P2X:
            df_newInv_P2G2P = df_CH[['GenName','Pmax_recon_built','Pmin_electrolyzer_built','Emax_H2storage_built','Pmax_DACMeth_built','Technology','NewInvestment']]
            df_newInv_P2G2P = df_newInv_P2G2P.rename(columns={'Pmax_recon_built':'Pmax'})
            df_newInv_P2G2P = df_newInv_P2G2P.rename(columns={'Pmin_electrolyzer_built':'Pmin'})
            df_newInv_P2G2P = df_newInv_P2G2P.rename(columns={'Emax_H2storage_built':'Emax_h2stor'})
            df_newInv_P2G2P = df_newInv_P2G2P.rename(columns={'Pmax_DACMeth_built':'Pmax_methdac'})
            df_newInv_P2G2P = df_newInv_P2G2P[(df_newInv_P2G2P.NewInvestment == 2)]#only P2G2P units
            df_newInv_P2G2P = df_newInv_P2G2P[(df_newInv_P2G2P['Pmax']!=0)|(df_newInv_P2G2P['Pmin']!=0)|(df_newInv_P2G2P['Emax_h2stor']!=0)|(df_newInv_P2G2P['Pmax_methdac']!=0)]
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewUnits_P2G2P.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_newInv_P2G2P.to_excel(writer, sheet_name='CH')
            writer.close()
        else:
            column_names_p2g2p = ['GenName','Pmax','Pmin','Emax_h2stor','Pmax_methdac','Technology','NewInvestment']
            df_p2g2p_empty = pd.DataFrame(columns = column_names_p2g2p)
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'NewUnits_P2G2P.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_p2g2p_empty.to_excel(writer, sheet_name='CH')
            writer.close()
        
        """
        output: RES premium price - premium for RES suppliers to build RES capacity to provide 1 more MWh
        """
        res.savePriceTimeseries(opt.get_REStarget_dual(baseMVA), 'RESpremiumPrice_CH.xlsx', 'RESpremium_price_CHF_per_MWh', 'CH')
     
        """
        16.4 output: input arguments for additional reserve calculation (for eMark and to update MySQL database)
        """  
        #should also save newly built PV units in CentIv
        df_TCR_windUP = pd.DataFrame(np.column_stack([config.windUP]), columns = ['Multiplier_WindUP'])
        df_TCR_windDOWN = pd.DataFrame(np.column_stack([config.windDOWN]), columns = ['Multiplier_WindDOWN'])
        df_TCR_solarUP = pd.DataFrame(np.column_stack([config.solarUP]), columns = ['Multiplier_SolarUP'])
        df_TCR_solarDOWN = pd.DataFrame(np.column_stack([config.solarDOWN]), columns = ['Multiplier_SolarDOWN'])
        df_distIv_Solar = pd.DataFrame(np.column_stack([config.distIvPVinstalled]), columns = ['DistIv_Solar_MW'])
        df_centIv_Wind_0 = df_newInv[df_newInv['Technology'].isin(wind_technologies)]
        centIv_Wind_total = df_centIv_Wind_0['Pmax'].sum()
        df_centIv_Wind = pd.DataFrame(np.column_stack([centIv_Wind_total]), columns = ['CentIv_Wind_MW']) #this includes the total newly installed power of all wind types in the list solar_technologies
        df_centIv_Solar_0 = df_newInv[df_newInv['Technology'].isin(solar_technologies)]
        centIv_Solar_total = df_centIv_Solar_0['Pmax'].sum()
        df_centIv_Solar = pd.DataFrame(np.column_stack([centIv_Solar_total]), columns = ['CentIv_Solar_MW']) #this includes the total newly installed power of all PV types in the list solar_technologies
        all_args = pd.concat([df_TCR_windUP,df_TCR_windDOWN,df_TCR_solarUP,df_TCR_solarDOWN,df_distIv_Solar,df_centIv_Solar,df_centIv_Wind], axis=1)      
        writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'AddReserve_Args.xlsx'
                ),
                engine='xlsxwriter'
            )
        all_args.to_excel(writer)
        writer.close()

        """
        16.5 output: nodal demand in CH with bus/canton info (tpResolution)
        """           
        bus_cantons = [{k:self.buses[k]['SubRegion'] for k in buses_CH}] #CH only
        bus_names = [{k:self.buses[k]['BusName'] for k in buses_CH}] #CH only
        regions = [{k:self.buses[k]['SubRegion'] for k in range(len(self.bus_id))}] #all
        names = [{k:self.buses[k]['BusName'] for k in range(len(self.bus_id))}] #all
        swiss_hourly_demand = {bus:[demand for demand in demands.values()] for bus,demands in load_timeseries_distIv.items() if bus in buses_CH}
        swiss_hourly_emobility_demand = {bus:[demand for demand in demands.values()] for bus,demands in self.adjusted_emobilityload.items() if bus in buses_CH}
        emobility_demand_CH_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_emobilityload.items() if bus in buses_CH}
        heatpump_uncontrolled_demand_CH_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_uncontrolledheatpumpload.items() if bus in buses_CH} # filtering uncontrolled demand of Switzerland for HPs
        heatpump_demand_CH_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_heatpumpload.items() if bus in buses_CH}
        base_demand_CH_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_baseload.items() if bus in buses_CH}
        H2electrolyzer_demand_CH_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_H2electrolyzerload.items() if bus in buses_CH}
        res.saveLoadGenerationTimeseries_Excel(swiss_hourly_demand, 'Demand_tpRes_CH.xlsx', 'CH')
        res.saveLoadGenerationTimeseriesNames_CSV(swiss_hourly_demand, 'Demand_tpRes_CH.csv', bus_cantons, bus_names)
        res.saveLoadGenerationTimeseriesNames_CSV(swiss_hourly_emobility_demand, 'eMobilityDemand_tpRes_CH.csv', bus_cantons, bus_names)
        # unpacked version of each demand type - nodal - CH
        res.saveLoadGenerationTimeseries_Excel(emobility_demand_CH_hourly, 'eMobilityDemand_hourly_CH.xlsx', 'CH')
        res.saveLoadGenerationTimeseries_Excel(heatpump_demand_CH_hourly, 'unflex_heatpumpDemand_hourly_CH.xlsx', 'CH')
        res.saveLoadGenerationTimeseries_Excel(heatpump_uncontrolled_demand_CH_hourly, 'uncontrolled_heatpumpDemand_hourly_CH.xlsx', 'CH') # this saves the reference "uncontrolled" demand from heat pumps
        res.saveLoadGenerationTimeseries_Excel(base_demand_CH_hourly, 'baseDemand_hourly_CH.xlsx', 'CH')
        res.saveLoadGenerationTimeseries_Excel(H2electrolyzer_demand_CH_hourly, 'H2electrDemand_hourly_CH.xlsx', 'CH') 

        # create output file of emobility demand
        emobility_demand_All_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_emobilityload.items()}
        res.saveLoadGenerationTimeseriesNames_CSV(emobility_demand_All_hourly, 'LoadEmob_BeforeShift_hourly_ALL_LP.csv', regions, names)
        # create output file of heat pump demand
        heatpump_demand_All_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_uncontrolledheatpumpload.items()}
        res.saveLoadGenerationTimeseriesNames_CSV(heatpump_demand_All_hourly, 'LoadHeatPump_BeforeShift_hourly_ALL_LP.csv', regions, names)
        heatpump_demand_nonflex_portion_All_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_heatpumpload.items()}
        res.saveLoadGenerationTimeseriesNames_CSV(heatpump_demand_nonflex_portion_All_hourly, 'LoadHeatPump_BeforeShift_NonFlexiblePortion_hourly_ALL_LP.csv', regions, names)
        heatpump_demand_flex_portion_All_hourly = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.adjusted_heatpumpload_flexibleportion.items()}
        res.saveLoadGenerationTimeseriesNames_CSV(heatpump_demand_flex_portion_All_hourly, 'LoadHeatPump_BeforeShift_FlexiblePortion_hourly_ALL_LP.csv', regions, names)

        """
        16.6 output: nodal demand in CH with bus/canton info (hourly)
        """
        swiss_hourly_demand_EM = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in load_timeseries.items() if bus in buses_CH}
        res.saveLoadGenerationTimeseries_Excel(swiss_hourly_demand_EM, 'Demand_hourly_CH.xlsx', 'CH')
        res.saveLoadGenerationTimeseriesNames_CSV(swiss_hourly_demand_EM, 'Demand_hourly_CH.csv',  bus_cantons, bus_names)

        """
        16.7 output: demand in neighbouring countries (hourly)
        """
        FR_hourly_demand = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in load_timeseries.items() if bus in buses_FR}
        IT_hourly_demand = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in load_timeseries.items() if bus in buses_IT}
        AT_hourly_demand = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in load_timeseries.items() if bus in buses_AT}
        DE_hourly_demand = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in load_timeseries.items() if bus in buses_DE}
        res.saveLoadGenerationTimeseries_Excel(FR_hourly_demand, 'Demand_hourly_FR.xlsx', 'FR')
        res.saveLoadGenerationTimeseries_Excel(IT_hourly_demand, 'Demand_hourly_IT.xlsx', 'IT')
        res.saveLoadGenerationTimeseries_Excel(AT_hourly_demand, 'Demand_hourly_AT.xlsx', 'AT')
        res.saveLoadGenerationTimeseries_Excel(DE_hourly_demand, 'Demand_hourly_DE.xlsx', 'DE')
        
        """
		16.8 output: original demand for EM with nodal information and bus names (hourly)
        """
        hourly_demand_original = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in self.original_load.items()}
        res.saveLoadGenerationTimeseriesNames_CSV(hourly_demand_original, 'DemandOriginal_hourly_ALL.csv', regions, names)

        """
        output: nodal pv injection from DistIv in CH with bus/canton info (hourly)
        """
        if len(self.generation_DistIv) == 0:
            print('No DistIv injections')
            swiss_hourly_distIv_inj = {bus:[0] * config.timeperiods for bus in buses_CH}
            res.saveLoadGenerationTimeseriesNames_CSV(swiss_hourly_distIv_inj, 'GenerationDistIv_hourly_CH.csv', bus_cantons, bus_names)
        else:
            swiss_hourly_distIv_inj = {bus:res_change.expand_array([generation for generation in generations.values()]) for bus,generations in self.generation_DistIv.items() if bus in buses_CH}
            res.saveLoadGenerationTimeseriesNames_CSV(swiss_hourly_distIv_inj, 'GenerationDistIv_hourly_CH.csv', bus_cantons, bus_names)
            res.saveLoadGenerationTimeseriesNames_Excel(swiss_hourly_distIv_inj, 'GenerationDistIv_hourly_CH.xlsx', 'CH', bus_cantons, bus_names)
           
        """
        16.9 output: secondary reserve requirement (tpResolution)
        -- needed by DistIv
        """
        df_reserves = pd.DataFrame(reserves_timeseries)
        df_reserves = df_reserves.T
        df_secondary_reserves = df_reserves[['FRRdnReq','FRRupReq']]  
        writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'FRRSystemReserves_tpRes_CH.xlsx'
                ),
                engine='xlsxwriter'
            )
        df_secondary_reserves.to_excel(writer)
        writer.close()
        
        """
        16.10 output: original RES target 
        """
        res.saveScalars_Excel(np.column_stack([original_target]), 'OriginalRESTarget_total_CH.xlsx', 'Original_RES_Target_TWh', 'CH')

        """
        16.11 output: demand scalar from CGE for CH
        """
        res.saveScalars_Excel(np.column_stack([Adjusted_Load_CH]), 'Demand_Scalar.xlsx', 'DemandScalar', 'CH')

        duration_log_dict['16'] = (time.time() - time_restarted)/60 #time in minutes
        logging.debug('Within GetOptimization, 16: Investment and Commitment Results Processing took %.2f minutes' % duration_log_dict['16'])
        time_restarted = time.time()

        """
        17.Solve LP Problem
        """
        # 17.1.Fixing Investment and Commitment Variables for Dual Calculation and LP Re-solve
        # Identifies units not built and P2X units built, Builds lists for further processing:, 
        # Fixes variables for LP re-solve (if duals are required), Handles unit commitment for nuclear units,
        # Prepares for the next optimization step: 
        #   Deactivates and re-activates certain constraints and objective functions (e.g., PV target).
        #   Sets up the objective function for the LP re-solve, depending on whether there are buses with negative demand.
        units_not_built = []
        P2X_built = []
        for Id, row in enumerate(generators_result_list):
            if row['NewInvestment'] == 0 and row['CandidateUnit'] == 1:
                units_not_built.append(Id)
            elif row['NewInvestment'] == 2: #append the P2G2P units
                if row['Pmax_recon_built'] == 0 and row['Pmin_electrolyzer_built'] == 0 and row['Pmax_DACMeth_built'] == 0 and row['Emax_H2storage_built'] == 0:
                    units_not_built.append(Id)
                else:
                    P2X_built.append(Id)
        
        combined_list_built = [item for item in combined_list if item not in units_not_built and item not in P2X_built] #we have a different cost f-n for P2X, NET not included here in combined_list
        NET_built = [item for item in net if item not in units_not_built]
        
        if config.duals_required:
            #fix all binary variables (UC and investments in conventional units and/or transmission lines) as well as all continuous variables relating to investments in continuous generators/storages
            UnitsBuilt = cg_CH_noUC.get_gens_built()
            for Id, value in UnitsBuilt.items():
                cg_CH_noUC.model.GenBuild[Id].fix(value)

            NuclearUnitsBuilt = CH_nuclear_invest.get_gens_built_nuclear()
            for Id, value in NuclearUnitsBuilt.items():
                CH_nuclear_invest.model.GenBuildNuclear[Id].fix(value)
                
            CapacitiesBuilt = investment.get_capacity_built({k:self.generators[k]['Pmax']/baseMVA for k in candidates_nondisp})
            for Id, value in CapacitiesBuilt.items():
                if round(value,3) <= 0.001:
                    investment.model.CandCapacityNonDisp[Id].fix(0.0)
                else:
                    investment.model.CandCapacityNonDisp[Id].fix(value)

            if candidates_batteries:
                BatteriesBuilt = batteries2.get_batteries_built()
                for Id, value in BatteriesBuilt.items():
                    batteries2.model.BattInv[Id].fix(value)
            
            if candidates_NET:
                NETUnitsBuilt = NET1.get_DAC_inv()
                for Id, value in NETUnitsBuilt.items():
                    if value < 0.001:
                        NET1.model.DACInvest[Id].fix(0.0)
                    else:
                        NET1.model.DACInvest[Id].fix(value)        

            if candidate_lines:
                LinesBuilt = DCPF.get_lines_built()
                for Id, value in LinesBuilt.items():
                    if value <= 0.49999:
                        DCPF.model.LineBuild[Id].fix(0)
                    else:
                        DCPF.model.LineBuild[Id].fix(1)
            
            if candidates_P2X:
                P2XBuilt_EL = P2X1.get_H2_EL_inv()
                P2XBuilt_H2Storage = P2X1.get_H2_storage_inv()
                P2XBuilt_Reconversion = P2X1.get_H2_gen_inv()
                P2XBuilt_CH4DAC = P2X1.get_CH4DAC_inv()
                for Id, value in P2XBuilt_EL.items():
                    if value < 0.001:
                        P2X1.model.H2ElectrolizerInvest[Id].fix(0.0)
                    else:
                        P2X1.model.H2ElectrolizerInvest[Id].fix(value)
                for Id, value in P2XBuilt_H2Storage.items():
                    if value < 0.001:
                        P2X1.model.H2StorageInvest[Id].fix(0.0)
                        P2X1.deactivateStorageConstraint3(Id)
                        for t in range(self.timeperiods):
                            P2X1.deactivateStorageConstraint1(Id,t)
                            P2X1.deactivateStorageConstraint2(Id,t)
                    else:
                        P2X1.model.H2StorageInvest[Id].fix(value)
                for Id, value in P2XBuilt_Reconversion.items():
                    if value < 0.001:
                       P2X1.model.H2GenInvest[Id].fix(0.0)
                    else:
                       P2X1.model.H2GenInvest[Id].fix(value)
                for Id, value in P2XBuilt_CH4DAC.items():
                    if value < 0.001:
                       P2X1.model.DACCH4Invest[Id].fix(0.0)
                    else:
                       P2X1.model.DACCH4Invest[Id].fix(value)

            if not config.contnuclear_required:
                UnitsOnOff = {}
                UnitsStart = {}
                UnitsStop = {}
                for i in nuclear_CH_exist:
                    UnitsOnOff[i] = list(cg.unit_on_off(i))
                    UnitsStart[i] = list(cg.unit_start(i))
                    UnitsStop[i] = list(cg.unit_stop(i))

                for Id, row in UnitsOnOff.items():
                    for t in range(self.timeperiods):
                        cg.model.UnitOn[Id,t].fix(row[t])
            
                for Id, row in UnitsStart.items():
                    for t in range(self.timeperiods):
                        cg.model.StartUp[Id,t].fix(row[t])
        
                for Id, row in UnitsStop.items():
                    for t in range(self.timeperiods):
                        cg.model.ShutDown[Id,t].fix(row[t])

            duration_log_dict['17-1'] = (time.time() - time_restarted)/60 #time in minutes
            logging.debug('Within GetOptimization, 17.1: Fixing Investment and Commitment Variables for Dual Calculation and LP Re-solve took %.2f minutes' % duration_log_dict['17-1'])
            time_restarted = time.time()

            #opt.model.preprocess() 
            # 17.2 Setting up the objective function for the LP re-solve
            opt.deactivate_obj1()
            DCPF.deactivate_PV_target()
            if base_rooftop_PV_target_on_potential_generation:
                DCPF.set_rooftop_PV_target_potential(
                    rooftop_pv_CH_exist,
                    rooftop_pv_CH_cand,
                    config.targetRESPV,
                    config.tpResolution,
                    baseMVA
                )
            else:
                # this sets the target based on the actual generation
                DCPF.set_rooftop_PV_target(
                    rooftop_pv_CH,
                    config.targetRESPV,
                    config.tpResolution,
                    baseMVA
                )
            DCPF.activate_PV_target()
            DCPF.deactivate_RES_target()

            if any(len(x) != 0 for x in nodemand_buses):
                print('.... There are buses with negative demand...solving the LP by adding a cost component for DistIv injections...')    
                #set objective function which does not multiply by tpRes and considers only production costs 
                opt.set_objective_function_LP(
                    opt.get_operational_costs_disagg_LP(combined_list_built,
                        {k:fuelprice_timeseries[k] for k in combined_list_built},
                        {k:self.generators[k]['GenEffic'] for k in combined_list_built},
                        {k:co2price_timeseries[k] for k in combined_list_built},
                        {k:self.generators[k]['CO2Rate'] for k in combined_list_built}, 
                        {k:self.generators[k]['VOM_Cost'] for k in combined_list_built},
                        baseMVA)                                                               +
                    P2X1.get_operational_costs_P2G2P_disagg_LP(P2X_built,
                        {k:fuelprice_timeseries[k] for k in P2X_built},
                        {k:self.generators[k]['GenEffic'] for k in P2X_built},
                        {k:co2price_timeseries[k] for k in P2X_built},
                        {k:self.generators[k]['CO2Rate'] for k in P2X_built},
                        {k:self.generators[k]['VOM_Cost'] for k in P2X_built},
                        {k:self.generators[k]['VOM_methdac'] for k in P2X_built},
                        baseMVA)                                                               +
                    NET1.get_operational_costs_NET_disagg_LP(NET_built,
                        {k:fuelprice_timeseries[k] for k in NET_built},
                        {k:self.generators[k]['GenEffic'] for k in NET_built},
                        {k:co2price_timeseries[k] for k in NET_built},
                        {k:self.generators[k]['CO2Rate'] for k in NET_built}, 
                        {k:self.generators[k]['VOM_Cost'] for k in NET_built},
                        baseMVA)                                                               +                        
                    P2X1.get_import_costs_P2G2P_disagg_LP(candidates_P2X,  #still over the whole list of P2X units because you may not build any infrastructure at the import nodes but could still import
                        {k:h2importprice_timeseries[k] for k in candidates_P2X},
                        {k:ch4importprice_timeseries[k] for k in candidates_P2X},
                        baseMVA)                                                                +
                    DCPF.get_lossload_cost(config.loadShedding_cost*baseMVA, 
                        1)                                                                     +
                    DCPF.get_cost_distIv_injection({k:self.buses[k]['PayInjection']*baseMVA for k in self.bus_id}, 
                        1)                                                                     + 
                    cg.get_operational_costs_conv_disagg_LP(nuclear_CH_exist,
                        {k:self.generators[k]['StartCost'] for k in nuclear_CH_exist},
                        {k:fuelprice_timeseries[k] for k in nuclear_CH_exist},
                        {k:self.generators[k]['GenEffic'] for k in nuclear_CH_exist},
                        {k:co2price_timeseries[k] for k in nuclear_CH_exist},
                        {k:self.generators[k]['CO2Rate'] for k in nuclear_CH_exist},
                        {k:self.generators[k]['VOM_Cost'] for k in nuclear_CH_exist},
                        baseMVA)                                                                -
                    hydro.set_hydro_storage_incentive(all_hydro_list, 1, 1*10**(-9))            - 
                    P2X1.get_CH4_revenue_LP(P2X_built, 
                        {k:fuelpriceSELL_timeseries[k] for k in P2X_built},
                        baseMVA)                                                                - 
                    P2X1.get_H2_revenue_LP(P2X_built, 
                        {k:h2priceSELL_timeseries[k] for k in P2X_built},
                        baseMVA)                                                                - 
                    P2X1.get_CO2_revenue_LP(P2X_built, 
                        {k:co2price_timeseries[k] for k in P2X_built}, 
                        {k:self.generators[k]['Conv_methdac_co2'] for k in P2X_built},
                        baseMVA))
            else:
                print('.... No buses with negative demand...solving the LP...')
                opt.set_objective_function_LP(
                    opt.get_operational_costs_disagg_LP(combined_list_built,
                        {k:fuelprice_timeseries[k] for k in combined_list_built},
                        {k:self.generators[k]['GenEffic'] for k in combined_list_built},
                        {k:co2price_timeseries[k] for k in combined_list_built},
                        {k:self.generators[k]['CO2Rate'] for k in combined_list_built}, 
                        {k:self.generators[k]['VOM_Cost'] for k in combined_list_built},
                        baseMVA)                                                                +
                    P2X1.get_operational_costs_P2G2P_disagg_LP(P2X_built,
                        {k:fuelprice_timeseries[k] for k in P2X_built},
                        {k:self.generators[k]['GenEffic'] for k in P2X_built},
                        {k:co2price_timeseries[k] for k in P2X_built},
                        {k:self.generators[k]['CO2Rate'] for k in P2X_built},
                        {k:self.generators[k]['VOM_Cost'] for k in P2X_built},
                        {k:self.generators[k]['VOM_methdac'] for k in P2X_built},
                        baseMVA)                                                                +
                    P2X1.get_import_costs_P2G2P_disagg_LP(candidates_P2X,  #still over the whole list of P2X units because you may not build any infrastructure at the import nodes but could still import
                        {k:h2importprice_timeseries[k] for k in candidates_P2X},
                        {k:ch4importprice_timeseries[k] for k in candidates_P2X},
                        baseMVA)                                                                +
                    NET1.get_operational_costs_NET_disagg_LP(NET_built,
                        {k:fuelprice_timeseries[k] for k in NET_built},
                        {k:self.generators[k]['GenEffic'] for k in NET_built},
                        {k:co2price_timeseries[k] for k in NET_built},
                        {k:self.generators[k]['CO2Rate'] for k in NET_built}, 
                        {k:self.generators[k]['VOM_Cost'] for k in NET_built},
                        baseMVA)                                                               +                           
                    DCPF.get_lossload_cost(config.loadShedding_cost*baseMVA, 
                        1)                                                                      +
                    cg.get_operational_costs_conv_disagg_LP(nuclear_CH_exist,
                        {k:self.generators[k]['StartCost'] for k in nuclear_CH_exist},
                        {k:fuelprice_timeseries[k] for k in nuclear_CH_exist},
                        {k:self.generators[k]['GenEffic'] for k in nuclear_CH_exist},
                        {k:co2price_timeseries[k] for k in nuclear_CH_exist},
                        {k:self.generators[k]['CO2Rate'] for k in nuclear_CH_exist},
                        {k:self.generators[k]['VOM_Cost'] for k in nuclear_CH_exist},
                        baseMVA)                                                                -
                    hydro.set_hydro_storage_incentive(all_hydro_list, 1, 1*10**(-9))            - 
                    P2X1.get_CH4_revenue_LP(P2X_built, 
                        {k:fuelpriceSELL_timeseries[k] for k in P2X_built},
                        baseMVA)                                                                - 
                    P2X1.get_H2_revenue_LP(P2X_built, 
                        {k:h2priceSELL_timeseries[k] for k in P2X_built}, 
                        baseMVA)                                                                - 
                    P2X1.get_CO2_revenue_LP(P2X_built, 
                        {k:co2price_timeseries[k] for k in P2X_built}, 
                        {k:self.generators[k]['Conv_methdac_co2'] for k in P2X_built},
                        baseMVA))
            opt.activate_obj2()
            opt.solve_linear_LP(threads=config.threads)

            duration_log_dict['17-2'] = (time.time() - time_restarted)/60 #time in minutes
            logging.debug('Within GetOptimization, 17.2, Setting up the objective function for the LP re-solve took : %f minutes' % duration_log_dict['17-2'])
            time_restarted = time.time()

            # 17.3 Saving the results of the LP re-solve (reserve prices, electricity prices, duals, etc.)
            # Exporting results to CSV files (including all parameters and variables and some duals, faster than the main approach, disconnected from the webviewer)
            logging.debug("Saving all variables and parameters to CSV files...")
            saveVarParDualsCsv(opt.model, results_folder)
            duration_log_dict['17-3Ali'] = (time.time() - time_restarted)/60 #time in minutes
            logging.debug("Within GetOptimization, 17.3 Saving all variables and parameters to CSV files took %.2f minutes" % duration_log_dict['17-3Ali'])
            time_restarted = time.time()
            logging.debug("Original exporting approach started ...")
            """
            output: reserve prices
            """      
            res.savePriceTimeseries(opt.get_FRRup_dual(baseMVA),'FRRupPrice_tpRes_CH.xlsx', 'FRRup_price_CHF_per_MWh', 'CH')
            res.savePriceTimeseries(opt.get_FRRdown_dual(baseMVA),'FRRdownPrice_tpRes_CH.xlsx', 'FRRdown_price_CHF_per_MWh', 'CH')
            res.savePriceTimeseries(opt.get_RRup_dual(baseMVA), 'RRupPrice_tpRes_CH.xlsx', 'RRup_price_CHF_per_MWh', 'CH')            
            res.savePriceTimeseries(opt.get_RRdown_dual(baseMVA),'RRdownPrice_tpRes_CH.xlsx',  'RRdown_price_CHF_per_MWh', 'CH')

            """
            output: electricity prices
            """      
            swiss_buses = set()             #NOTE: why redefined? 
            neighbours_buses = set()        #NOTE: why redefined? 
            for Id, row in enumerate(self.buses):
                if row['Country'] == 'CH':
                    swiss_buses.add(Id)
                else: 
                    neighbours_buses.add(Id)
            res.savePriceTimeseries(opt.get_nodal_dual(swiss_buses, baseMVA, config.single_electric_node), 'ElPrice_tpRes_CH.xlsx')
            res.savePriceTimeseries(opt.get_nodal_dual(neighbours_buses, baseMVA), 'ElPrice_tpRes_Neighbours.xlsx') 

            nodal_CH_hourly = {}
            for key,row in opt.get_nodal_dual(swiss_buses, baseMVA, config.single_electric_node).items():
                nodal_CH_hourly[key] = list(res_change.expand_array(row))
            res.savePriceTimeseries(nodal_CH_hourly, 'ElPrice_hourly_CH.xlsx')

            nodal_CH_tpRes = {}
            for key,row in opt.get_nodal_dual(swiss_buses, baseMVA, config.single_electric_node).items():
                nodal_CH_tpRes[key] = row
            
            replace_price_CH = 2*281.3 #NOTE: must be a parameter! (twice as expensive as the most expensive non-DNS unit in CH for the given scenario year)
            nodal_CH_hourly_adjusted_DistIvABM = {}
            for k, v in nodal_CH_hourly.items():
                nodal_CH_hourly_adjusted_DistIvABM[k]=list(min(replace_price_CH, i) for i in v) #cap the price
            for k, v in nodal_CH_hourly_adjusted_DistIvABM.items():
                nodal_CH_hourly_adjusted_DistIvABM[k]=list(max(0, i) for i in v) #no negative prices sent to DistIv/ABM (each negative price entry is substituted with 0)
            res.savePriceTimeseries(nodal_CH_hourly_adjusted_DistIvABM, 'ElPrice_hourly_adjustedDistIvABM_CH.xlsx') 

            nodal_CH_tpRes_adjusted_DistIvABM = {}
            for k, v in nodal_CH_tpRes.items():
                nodal_CH_tpRes_adjusted_DistIvABM[k]=list(min(replace_price_CH, i) for i in v)
            for k, v in nodal_CH_tpRes_adjusted_DistIvABM.items():
                nodal_CH_tpRes_adjusted_DistIvABM[k]=list(max(0, i) for i in v) #no negative prices sent to DistIv/ABM (each negative price entry is substituted with 0)  
            res.savePriceTimeseries(nodal_CH_tpRes_adjusted_DistIvABM, 'ElPrice_tpRes_adjustedDistIvABM_CH.xlsx')

            nodal_neighbours_hourly = {}
            for key,row in opt.get_nodal_dual(neighbours_buses, baseMVA).items():
                nodal_neighbours_hourly[key] = list(res_change.expand_array(row))
            res.savePriceTimeseries(nodal_neighbours_hourly, 'ElPrice_hourly_Neighbours.xlsx')
            
            replace_price_neighbours = 2*324.9 #NOTE: must be a parameter! (twice as expensive as the most expensive non-DNS unit in the neighbouring country for the given scenario year)
            nodal_neighbours_hourly_adjusted_DistIvABM = {}
            for k, v in nodal_neighbours_hourly.items():
                nodal_neighbours_hourly_adjusted_DistIvABM[k]=list(min(replace_price_neighbours, i) for i in v)
            for k, v in nodal_neighbours_hourly_adjusted_DistIvABM.items():
                nodal_neighbours_hourly_adjusted_DistIvABM[k]=list(max(0, i) for i in v) #no negative prices sent to DistIv/ABM (each negative price entry is substituted with 0)     
            res.savePriceTimeseries(nodal_neighbours_hourly_adjusted_DistIvABM, 'ElPrice_hourly_adjustedDistIvABM_Neighbours.xlsx')

            nodal_neighbours_tpRes = {}
            for key,row in opt.get_nodal_dual(neighbours_buses, baseMVA).items():
                nodal_neighbours_tpRes[key] = row

            nodal_neighbours_tpRes_adjusted_DistIvABM = {}
            for k, v in nodal_neighbours_tpRes.items():
                nodal_neighbours_tpRes_adjusted_DistIvABM[k]=list(min(replace_price_neighbours, i) for i in v)
            for k, v in nodal_neighbours_tpRes_adjusted_DistIvABM.items():
                nodal_neighbours_tpRes_adjusted_DistIvABM[k]=list(max(0, i) for i in v) #no negative prices sent to DistIv/ABM (each negative price entry is substituted with 0)
            res.savePriceTimeseries(nodal_neighbours_tpRes_adjusted_DistIvABM, 'ElPrice_tpRes_adjustedDistIvABM_Neighbours.xlsx')

            """
            output: winter net import dual
            """
            if config.winterNetImport != 0:
                opt.print_duals_net_winter_import(baseMVA)

            """
            output: generation per generator ALL (hourly) and also per country (hourly)
            """
            country_info = [{k:self.generators[k]['Country'] for k in range(len(self.generators))}]
            cantonal_info = [{k:self.generators[k]['SubRegion'] for k in range(len(self.generators))}]
            technology_info = [{k:self.generators[k]['Technology'] for k in range(len(self.generators))}]
            name_info = [{k:self.generators[k]['GenName'] for k in range(len(self.generators))}]
            d_generation_per_gen_full_LP = {}
            d_consumption_per_gen_full_LP = {}
            d_curtailment_per_gen_full_LP = {}
            for i in range(len(self.generators)):
                d_generation_per_gen_full_LP[i] = list(res_change.expand_array(opt.get_generator_power(i)*baseMVA))
                d_consumption_per_gen_full_LP[i] = list(res_change.expand_array(opt.get_generator_power_consumed(i)*baseMVA))
            for Id, row in enumerate(self.generators):
                if row['UnitType'] == 'Dispatchable' and row['Technology'] not in hydro_storage_technologies:
                    d_curtailment_per_gen_full_LP[Id] = [0] * config.timeperiods #no curtailments for dispatchable gens (could have for Dam and Pump)
                if row['UnitType'] == 'Dispatchable' and row['Technology'] in hydro_storage_technologies: 
                    d_curtailment_per_gen_full_LP[Id] = list(res_change.expand_array(hydro.get_spill(Id)*baseMVA))  #Hydro Dam and Pump
                if row['UnitType'] == 'NonDispatchable' and row['Technology'] == 'RoR':
                    d_curtailment_per_gen_full_LP[Id] = list(res_change.expand_array(RES.get_ROR_curtailments(Id)*baseMVA))    #existing RoR
                if row['UnitType'] == 'NonDispatchable' and row['Technology'] in wind_technologies and row['CandidateUnit'] == 0: 
                    d_curtailment_per_gen_full_LP[Id] = list(res_change.expand_array(RES.get_wind_curtailments(Id)*baseMVA))   #existing wind
                if row['UnitType'] == 'NonDispatchable' and row['Technology'] in solar_technologies and row['CandidateUnit'] == 0: 
                    d_curtailment_per_gen_full_LP[Id] = list(res_change.expand_array(RES.get_solar_curtailments(Id)*baseMVA))  #existing solar
                if row['UnitType'] == 'NonDispatchable' and row['CandidateUnit'] == 1:
                    d_curtailment_per_gen_full_LP[Id] = list(res_change.expand_array(investment.get_curtailments_newRES(Id)*baseMVA))  #all candidate non-dispatchable
            
            d_generation_consumption_per_gen_full_LP = {}
            for k, list_timeseries in d_generation_per_gen_full_LP.items():
                d_generation_consumption_per_gen_full_LP[k] = [x - y for x,y in zip(list_timeseries,d_consumption_per_gen_full_LP[k])]
                    
            res.saveLoadGenerationTimeseriesNames_Excel(d_generation_per_gen_full_LP, 'GenerationPerGen_hourly_ALL_LP.xlsx', 'Generation_MWh', country_info, technology_info)
            res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_generation_per_gen_full_LP, 'GenerationPerGenGasNet_hourly_ALL_LP.xlsx', 'Generation_MWh', country_info, technology_info, name_info)
            res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_generation_per_gen_full_LP, 'GenerationPerGenCANTONS_hourly_ALL_LP.xlsx', 'Generation_MWh', country_info, cantonal_info, technology_info)
            res.saveLoadGenerationTimeseriesNames_Excel(d_consumption_per_gen_full_LP, 'ConsumptionPerGen_full_ALL_LP.xlsx', 'Consumption_MWh', country_info, technology_info)
            res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_consumption_per_gen_full_LP, 'ConsumptionPerGenCANTONS_hourly_ALL_LP.xlsx', 'Consumption_MWh', country_info, cantonal_info, technology_info)
            res.saveLoadGenerationTimeseriesNames_Excel(d_generation_consumption_per_gen_full_LP, 'GenerationConsumptionPerGen_hourly_ALL_LP.xlsx', 'GenerationConsumption_MWh', country_info, technology_info)
            res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_generation_consumption_per_gen_full_LP, 'GenerationConsumptionPerGenGenNames_hourly_ALL_LP.xlsx', 'GenerationConsumption_MWh', country_info, technology_info, name_info)
            res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_curtailment_per_gen_full_LP, 'CurtailmentPerGen_hourly_ALL_LP.xlsx', 'Curtailment_MWh', country_info, technology_info, name_info)
            
            #required by the webviewer
            d_generation_per_gen_full_LP_df = pd.read_excel(
                os.path.join(
                    results_folder,
                    "GenerationPerGen_hourly_ALL_LP.xlsx"
                )
            )
            d_generation_per_gen_full_LP_df_t = d_generation_per_gen_full_LP_df.transpose()
            #only CH generators/storages
            d_generation_per_gen_full_LP_df_CH = d_generation_per_gen_full_LP_df_t[d_generation_per_gen_full_LP_df_t[0] == "CH"]
            d_generation_per_gen_full_LP_df_CH2 = d_generation_per_gen_full_LP_df_CH.groupby([1]).sum()
            d_generation_per_gen_full_LP_df_CH_f = d_generation_per_gen_full_LP_df_CH2.transpose()
            d_generation_per_gen_full_LP_df_CH_f.to_csv(
                os.path.join(
                    results_folder,
                    "GenerationPerGenType_hourly_CH_LP.csv"
                )
            ) #CH tag is copied 
            #only DE generators/storages
            d_generation_per_gen_full_LP_df_DE = d_generation_per_gen_full_LP_df_t[d_generation_per_gen_full_LP_df_t[0] == "DE"]
            d_generation_per_gen_full_LP_df_DE2 = d_generation_per_gen_full_LP_df_DE.groupby([1]).sum()
            d_generation_per_gen_full_LP_df_DE_f = d_generation_per_gen_full_LP_df_DE2.transpose()
            d_generation_per_gen_full_LP_df_DE_f.to_csv(
                os.path.join(
                    results_folder,
                    "GenerationPerGenType_hourly_DE_LP.csv"
                )
            )
            #only IT generators/storages
            d_generation_per_gen_full_LP_df_IT = d_generation_per_gen_full_LP_df_t[d_generation_per_gen_full_LP_df_t[0] == "IT"]
            d_generation_per_gen_full_LP_df_IT2 = d_generation_per_gen_full_LP_df_IT.groupby([1]).sum()
            d_generation_per_gen_full_LP_df_IT_f = d_generation_per_gen_full_LP_df_IT2.transpose()
            d_generation_per_gen_full_LP_df_IT_f.to_csv(
                os.path.join(
                    results_folder,
                    "GenerationPerGenType_hourly_IT_LP.csv"
                )
            )
            #only AT generators/storages
            d_generation_per_gen_full_LP_df_AT = d_generation_per_gen_full_LP_df_t[d_generation_per_gen_full_LP_df_t[0] == "AT"]
            d_generation_per_gen_full_LP_df_AT2 = d_generation_per_gen_full_LP_df_AT.groupby([1]).sum()
            d_generation_per_gen_full_LP_df_AT_f = d_generation_per_gen_full_LP_df_AT2.transpose()
            d_generation_per_gen_full_LP_df_AT_f.to_csv(
                os.path.join(
                    results_folder,
                    "GenerationPerGenType_hourly_AT_LP.csv"
                )
            )
            #only FR generators/storages
            d_generation_per_gen_full_LP_df_FR = d_generation_per_gen_full_LP_df_t[d_generation_per_gen_full_LP_df_t[0] == "FR"]
            d_generation_per_gen_full_LP_df_FR2 = d_generation_per_gen_full_LP_df_FR.groupby([1]).sum()
            d_generation_per_gen_full_LP_df_FR_f = d_generation_per_gen_full_LP_df_FR2.transpose()
            d_generation_per_gen_full_LP_df_FR_f.to_csv(
                os.path.join(
                    results_folder,
                    "GenerationPerGenType_hourly_FR_LP.csv"
                )
            )

            #curtailment per node
            #dict with keys - node names and values all gen Ids of the gens at the node - TO DO

            """
            output: operation costs per generator (hourly)
            """ 
            d_generationcost_timeseries_hourly_LP = {}
            for Id, row in enumerate(self.generators):
                if Id in combined_list_built:
                    d_generationcost_timeseries_hourly_LP[Id] = list(res_change.expand_array(opt.get_hourly_gencost_LP(Id)))
                if Id in P2X_built: 
                    d_generationcost_timeseries_hourly_LP[Id] = list(res_change.expand_array(P2X1.get_hourly_gencost_P2G2P_LP(Id)))
                if Id in NET_built:
                    d_generationcost_timeseries_hourly_LP[Id] = list(res_change.expand_array(NET1.get_hourly_gencost_NET_LP(Id)))    
                if Id in nuclear_CH_exist: 
                    d_generationcost_timeseries_hourly_LP[Id] = list(res_change.expand_array(cg.get_hourly_gencost_conv_LP(Id)))
                if Id in units_not_built: #unit is not built (incl P2G2P not built)
                    d_generationcost_timeseries_hourly_LP[Id] = [0] * config.timeperiods
            res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_generationcost_timeseries_hourly_LP, 'GenerationCostPerGen_hourly_ALL_LP.xlsx', 'GenCost_EUR', country_info, technology_info, name_info)

            
            """
            output: batteries state of charge in CH (hourly)
            """
            storage_value_batt_exist_CH_LP = {}
            for i in existing_batteries:
                storage_value_batt_exist_CH_LP[i] = list(res_change.expand_array(batteries1.get_battery_SoC(i)*baseMVA))# MWh
            df_storageBattExistCH = pd.DataFrame.from_dict(storage_value_batt_exist_CH_LP)
            storage_value_batt_cand_CH_LP = {}
            for i in candidates_batteries:
                storage_value_batt_cand_CH_LP[i] = list(res_change.expand_array(batteries2.get_battery_SoC_inv(i)*baseMVA))# MWh
            df_storageBattCandCH = pd.DataFrame.from_dict(storage_value_batt_cand_CH_LP)
            res.saveExportsImportsMultiple_Excel([df_storageBattExistCH,df_storageBattCandCH],['BattSoC_hourly_existing','BattSoC_hourly_candidate'],'BattSoC_hourly_ALL_LP.xlsx')

            """
            output: cumulative storage level in CH (hourly and monthly)  
            """
            hourly_inflows_dams = {k:renewables_timeseries[k] for k in hydro_Dam}  # timeseries of dam inflows
            storage_value_dams_CH_LP = {}
            for i in hydro_Dam_CH:
                if i in hourly_inflows_dams:
                    storage_value_dams_CH_LP[i] = list(res_change.expand_soc_array(hydro.get_battery_state(i)*baseMVA))# MWh
            df_storageCH = pd.DataFrame.from_dict(storage_value_dams_CH_LP)
        
            hourly_inflows_pumps = {k:renewables_timeseries[k] for k in hydro_Pumped_CH_notdaily}  # timeseries of dam inflows
            storage_value_pumps_CH_LP = {}
            for i in hydro_Pumped_CH_notdaily:
                if i in hourly_inflows_pumps:
                    storage_value_pumps_CH_LP[i] = list(res_change.expand_soc_array(hydro.get_battery_state(i)*baseMVA))# MWh
            df_storage2CH = pd.DataFrame.from_dict(storage_value_pumps_CH_LP)
        
            hourly_inflows_pumps_daily = {k:renewables_timeseries[k] for k in hydro_Pumped_CH_daily}  # timeseries of dam inflows
            storage_value_pumps_daily_CH_LP = {}
            for i in hydro_Pumped_CH_daily:
                if i in hourly_inflows_pumps_daily:
                    storage_value_pumps_daily_CH_LP[i] = list(res_change.expand_soc_array(hydro.get_battery_state(i)*baseMVA/config.tpResolution))# MWh        
            df_storage3CH = pd.DataFrame.from_dict(storage_value_pumps_daily_CH_LP)  
              
            res.saveExportsImportsMultiple_Excel([df_storageCH,df_storage2CH,df_storage3CH],['DamLevel_hourly_CH','PumpLevel_hourly_CH','DailyPumpLevel_hourly_CH'],'Reservoirs_hourly_CH_LP.xlsx')

            #monthly
            monthHoursComul = [24 * numOfDay - 1 for numOfDay in [31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]]
            storagelevel_monthly_dams_LP = [sum([x[i] if i < len(x) else x[len(x) - 1] for x in storage_value_dams_CH_LP.values()]) for i in monthHoursComul]
            storagelevel_monthly_pumps_LP = [sum([x[i] if i < len(x) else x[len(x) - 1] for x in storage_value_pumps_CH_LP.values()]) for i in monthHoursComul]
            storagelevel_monthly_dailypumps_LP = [sum([x[i] if i < len(x) else x[len(x) - 1] for x in storage_value_pumps_daily_CH_LP.values()]) for i in monthHoursComul]
            storagelevel_monthly_all_LP = [sum(x) for x in zip(storagelevel_monthly_dams_LP, storagelevel_monthly_pumps_LP, storagelevel_monthly_dailypumps_LP)]
        
            #concatinate with monthNames
            df_monthNames = pd.DataFrame(monthNames)         
            df_storagelevel_monthly_dams_LP = pd.DataFrame(storagelevel_monthly_dams_LP)
            df_storagelevel_monthly_all_LP = pd.DataFrame(storagelevel_monthly_all_LP)
            df_storagelevel_monthly_dams_monthNames_LP = pd.concat([df_monthNames,df_storagelevel_monthly_dams_LP], axis=1)  
            df_storagelevel_monthly_all_monthNames_LP = pd.concat([df_monthNames,df_storagelevel_monthly_all_LP], axis=1)  
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'CumulativeStorageLevels_monthly_CH_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_storagelevel_monthly_all_monthNames_LP.to_excel(writer, sheet_name='MWh')
            writer.close()
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'DamStorageLevels_monthly_CH_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_storagelevel_monthly_dams_monthNames_LP.to_excel(writer, sheet_name='MWh')
            writer.close()
            df_storagelevel_monthly_dams_monthNames_LP.to_csv(
                os.path.join(
                    results_folder,
                    'DamStorageLevels_monthly_CH_LP.csv'
                ),
                header=True
            )

            #only update the generated/pumped power
            for Id, row in enumerate(generators_result_list):
                generated = [n if n > 0 else 0 for n in list(res_change.expand_array(opt.get_generator_power(Id))*baseMVA)]
                pumped = [n if n > 0 else 0 for n in list(res_change.expand_array(opt.get_generator_power_consumed(Id))*baseMVA)]
                # Fill generated and pumped with zeros for one year.
                generated = generated + [0 for _ in range(min(0, numOfHours[12] - len(generated)))]
                pumped = pumped + [0 for _ in range(min(0, numOfHours[12] - len(pumped)))]
                row.update({u'Tot_Pgen_MWh':int(sum(generated))})
                row.update({u'Tot_Ppump_MWh':int(sum(pumped))})
                for i in range(12):
                    row.update({u'Pgen_'+monthNames[i]+u'_MWh':int(sum(generated[numOfHours[i]:numOfHours[i+1]]))})
                    row.update({u'Ppump_'+monthNames[i]+u'_MWh':int(sum(pumped[numOfHours[i]:numOfHours[i+1]]))})
                if Id in combined_list_built:
                    gencost = [n if n > 0 else 0 for n in list(res_change.expand_array(opt.get_hourly_gencost_LP(Id)))]
                    row.update({u'Tot_OpCost_CHF':int(sum(gencost))})
                if Id in P2X_built:
                    gencost = [n if n > 0 else 0 for n in list(res_change.expand_array(P2X1.get_hourly_gencost_P2G2P_LP(Id)))]
                    row.update({u'Tot_OpCost_CHF':int(sum(gencost))})
                if Id in NET_built:
                    gencost = [n if n > 0 else 0 for n in list(res_change.expand_array(NET1.get_hourly_gencost_NET_LP(Id)))]
                    row.update({u'Tot_OpCost_CHF':int(sum(gencost))})                    
                if Id in nuclear_CH_exist: 
                    gencost = [n if n > 0 else 0 for n in list(res_change.expand_array(cg.get_hourly_gencost_conv_LP(Id)))]
                    row.update({u'Tot_OpCost_CHF':int(sum(gencost))})
                if Id in units_not_built: #unit is not built (incl P2G2P not built)
                    row.update({u'Tot_OpCost_CHF':0}) 
                #for i in existing_units:
                #    if Id == i:
                #        tot_operational_cost_EUR[Id] = row['Tot_Pgen_MWh']*row['TotVarCost'] #need to update the TotVarCost
                #for i,inv in new_units_built.items(): 
                #    if Id == i: 
                #        tot_operational_cost_EUR[Id] = row['Tot_Pgen_MWh']*row['TotVarCost'] #need to update the TotVarCost
                #for i, inv in new_nuclearunits_built.items():
                #    if Id == i:
                #        tot_operational_cost_EUR[Id] = row['Tot_Pgen_MWh']*row['TotVarCost'] #need to update the TotVarCost
                #for i,inv in new_capacities_built.items():
                #    if Id == i:
                #        tot_operational_cost_EUR[Id] = row['Tot_Pgen_MWh']*row['TotVarCost'] #need to update the TotVarCost
                #for i,inv in new_batteries_built.items():
                #    if Id == i:
                #        tot_operational_cost_EUR[Id] = row['Tot_Pgen_MWh']*row['TotVarCost'] #need to update the TotVarCost
                #if row['NewInvestment'] == 2: #P2G2P units
                #    tot_operational_cost_EUR[Id] = row['Tot_Pgen_MWh']*row['TotVarCost'] #only production costs from reconversion technology need to update the TotVarCost
                #P2X operational costs
                #row.update({u'Tot_OpCost_CHF':tot_operational_cost_EUR[Id]})
                if row['NewInvestment'] == 2:
                    row['Tot_InvCost_CHF'] == row['InvCost_recon_built'] + row['InvCost_electrolyzer_built']
                    row['Tot_FOpCost_CHF'] == row['FOMCost_electrolyzer_built'] + row['FOMCost_recon_built']
                    if row['Pmax_recon_built'] == 0 and row['Pmin_electrolyzer_built'] == 0 and row['Pmax_DACMeth_built'] == 0 and row['Emax_H2storage_built'] == 0:
                        row.clear()
                for k,v in new_units_built.items():
                    if k == Id and v == 0: 
                        row.clear()#delete rows with candidate units which are not built
                for k,v in new_nuclearunits_built.items():
                    if k == Id and v == 0: 
                        row.clear()#delete rows with nuclear candidate units which are not built
                for k,v in new_capacities_built.items():
                    if k == Id and v == 0: 
                        row.clear()#delete rows with candidate non dispatchable capacities which are not built
                for k,v in new_batteries_built.items():
                    if k == Id and v == 0:
                        row.clear()#delete rows with candidate batteries which are not built

            df = pd.DataFrame(generators_result_list)
            df_CH = df[(df.Country == 'CH')] #only generators at Swiss nodes
            df_CH_gens = df_CH[['idGen','Technology','Tot_Pgen_MWh','Tot_Ppump_MWh'] + ['Pgen_' + x + '_MWh' for x in monthNames] + ['Ppump_' + x + '_MWh' for x in monthNames]]
            df_dam_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'Dam']
            df_pumped_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'Pump']
            df_ror_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'RoR']
            df_nuclear_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'Nuclear']
            df_biomass_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'Biomass']
            df_gassc_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'GasSC']
            df_gascc_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'GasCC']
            df_gasCCS_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'GasCC-CCS']
            df_solar_gens = df_CH_gens[df_CH_gens['Technology'].isin(solar_technologies)]
            df_wind_gens = df_CH_gens[df_CH_gens['Technology'].isin(wind_technologies)]
            df_oil_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'Oil']
            df_batt_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'BattTSO']
            df_p2g2p = df_CH_gens.loc[df_CH_gens['Technology'] == 'P2G2P']
            df_dac_gens = df_CH_gens.loc[df_CH_gens['Technology'] == 'DAC']

            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'GenerationConsumption_monthly_CH_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_CH_gens.to_excel(writer, sheet_name='CH')
            df_dam_gens.to_excel(writer, sheet_name='CH_Dams')
            df_pumped_gens.to_excel(writer,sheet_name='CH_Pumped')
            df_ror_gens.to_excel(writer,sheet_name='CH_RoR')
            df_nuclear_gens.to_excel(writer,sheet_name='CH_Nuclear')
            df_biomass_gens.to_excel(writer,sheet_name='CH_Biomass')
            df_gassc_gens.to_excel(writer,sheet_name='CH_GasSC')
            df_gascc_gens.to_excel(writer,sheet_name='CH_GasCC')
            df_gasCCS_gens.to_excel(writer,sheet_name='CH_GasCC-CCS')
            df_solar_gens.to_excel(writer,sheet_name='CH_PV')
            df_wind_gens.to_excel(writer,sheet_name='CH_Wind')
            df_oil_gens.to_excel(writer,sheet_name='CH_Oil')
            df_batt_gens.to_excel(writer,sheet_name='CH_Batt')
            df_p2g2p.to_excel(writer,sheet_name='CH_P2G2P')
            df_dac_gens.to_excel(writer,sheet_name='CH_DAC')
            writer.close()

            #for the interface with Gemel and DistIv
            df_CH_gens_CGE = df_CH[['Technology','Tot_Pgen_MWh','Tot_Ppump_MWh','Tot_InvCost_CHF','Tot_OpCost_CHF','Tot_FOpCost_CHF']]  #need to update the TotVarCost
            df_CH_gens_CGE = df_CH_gens_CGE.sort_values(by=['Technology'])
            df_CH_gens_CGE = df_CH_gens_CGE.groupby(['Technology']).agg({'Tot_Pgen_MWh':sum,'Tot_Ppump_MWh':sum,'Tot_InvCost_CHF':sum,'Tot_OpCost_CHF':sum,'Tot_FOpCost_CHF':sum})
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'GenerationConsumption_total_CH_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_CH_gens_CGE.to_excel(writer, sheet_name='CH')
            writer.close()
            
            """
            output: generation/consumption per technology type in DE,AT,FR,IT (total and monthly)
            """
            #DE
            df_DE = df[(df.Country == 'DE')] #only generators at DE nodes
            df_DE_gens = df_DE[['idGen','Technology','Tot_Pgen_MWh','Tot_Ppump_MWh'] + ['Pgen_' + x + '_MWh' for x in monthNames] + ['Ppump_' + x + '_MWh' for x in monthNames]]
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'GenerationConsumption_monthly_DE_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_DE_gens.to_excel(writer, sheet_name='DE')
            writer.close()
            #AT
            df_AT = df[(df.Country == 'AT')] #only generators at AT nodes
            df_AT_gens = df_AT[['idGen','Technology','Tot_Pgen_MWh','Tot_Ppump_MWh'] + ['Pgen_' + x + '_MWh' for x in monthNames] + ['Ppump_' + x + '_MWh' for x in monthNames]]
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'GenerationConsumption_monthly_AT_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_AT_gens.to_excel(writer, sheet_name='AT')
            writer.close()
            #FR
            df_FR = df[(df.Country == 'FR')] #only generators at FR nodes
            df_FR_gens = df_FR[['idGen','Technology','Tot_Pgen_MWh','Tot_Ppump_MWh'] + ['Pgen_' + x + '_MWh' for x in monthNames] + ['Ppump_' + x + '_MWh' for x in monthNames]]
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'GenerationConsumption_monthly_FR_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_FR_gens.to_excel(writer, sheet_name='FR')
            writer.close()
            #IT
            df_IT = df[(df.Country == 'IT')] #only generators at IT nodes
            df_IT_gens = df_IT[['idGen','Technology','Tot_Pgen_MWh','Tot_Ppump_MWh'] + ['Pgen_' + x + '_MWh' for x in monthNames] + ['Ppump_' + x + '_MWh' for x in monthNames]]
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'GenerationConsumption_monthly_IT_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_IT_gens.to_excel(writer, sheet_name='IT')
            writer.close()
            
            """
            output: generators and costs in CH for eMark
            """
            df_EM = df_CH[['idGen','generatorPmax_MW','generatorPmin_MW','TotVarCost']] #need to update the totVarCost
            df_EM = df_EM.rename(columns={'generatorPmax_MW':'Pmax'})
            df_EM = df_EM.rename(columns={'generatorPmin_MW':'Pmin'})
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'Generators_EM.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_EM.to_excel(writer, sheet_name='CH')
            writer.close()

            df_EM1 = df_CH[['idGen','generatorPmax_MW','generatorPmin_MW','TotVarCost','Technology']] #need to update the totVarCost
            df_EM1 = df_EM1.rename(columns={'generatorPmax_MW':'Pmax'})
            df_EM1 = df_EM1.rename(columns={'generatorPmin_MW':'Pmin'})
            df_EM1 = df_EM1.groupby('Technology').sum()
            df_EM1.to_csv(
                os.path.join(
                    results_folder,
                    "Generators_EM_agg.csv"
                )
            )
                
            
            """
            output: LCOE information - new investments only
            -- for P2G2P we add only costs related to electrolyzer and reconversion (i.e. treated as a battery)
            """   
            df_CH_gens_DGEP = df_CH[['Technology','Tot_FOpCost_CHF','Tot_InvCost_CHF','Tot_Pgen_MWh','NewInvestment']]
            df_CH_gens_DGEP = df_CH_gens_DGEP[(df_CH_gens_DGEP.NewInvestment >= 1)] #only newly built generators or newly built P2G2P
            df_CH_gens_DGEP = df_CH_gens_DGEP.sort_values(by=['Technology'])
            df_CH_gens_DGEP = df_CH_gens_DGEP.groupby(['Technology']).agg({'Tot_FOpCost_CHF':sum,'Tot_InvCost_CHF':sum,'Tot_Pgen_MWh':sum})
            writer = pd.ExcelWriter(
                os.path.join(
                    results_folder,
                    'Investments_total_CH_LP.xlsx'
                ),
                engine='xlsxwriter'
            )
            df_CH_gens_DGEP.to_excel(writer, sheet_name='CH')
            writer.close()
            
            """
            output: RES curtailment of nodal distIv injections (hourly)
            """
            #sign is negative
            if any(len(x) != 0 for x in nodemand_buses):
                print('Possible curtailments in file REScurtailmentDistIv_hourly_ALL_LP.csv')
                d_distIv_curtailment_per_bus = {}
                for i in range(len(self.buses)):
                    d_distIv_curtailment_per_bus[i] = list(res_change.expand_array(DCPF.get_distIv_curtailment(i)*baseMVA))  
                res.saveLoadGenerationTimeseriesNames_Excel(d_distIv_curtailment_per_bus, 'REScurtailmentDistIv_hourly_ALL_LP.xlsx', 'MW', regions, names)
                res.saveLoadGenerationTimeseriesNames_CSV(d_distIv_curtailment_per_bus, 'REScurtailmentDistIv_hourly_ALL_LP.csv', regions, names)
            else:
                d_distIv_curtailment_per_bus = {}
                for i in range(len(self.buses)):
                    d_distIv_curtailment_per_bus[i] = [0] * config.timeperiods 
                print('REScurtailmentDistIv_hourly_ALL_LP.csv contains zeros')
                res.saveLoadGenerationTimeseriesNames_CSV(d_distIv_curtailment_per_bus, 'REScurtailmentDistIv_hourly_ALL_LP.csv', regions, names)
 
            """
            output: nodal load shedding for all nodes including neighbours (hourly)
            """
            d_load_shedding_per_bus = {}
            for i in range(len(self.buses)):
                d_load_shedding_per_bus[i] = list(res_change.expand_array(DCPF.get_load_shedding(i)*baseMVA))   
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shedding_per_bus, 'LoadShedding_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_CSV(d_load_shedding_per_bus, 'LoadShedding_hourly_ALL_LP.csv', regions, names)
            
            """
            output: nodal load shifting for all nodes including neighbours (hourly)
            """
            d_load_shift_up_dsm_per_bus = {}
            d_load_shift_down_dsm_per_bus = {}
            d_load_shift_up_emob_per_bus = {}
            d_load_shift_down_emob_per_bus = {}
            d_load_emob_beforeshift_per_bus = {} # JG: input data: before shifting the EV demand
            d_load_emob_aftershift_per_bus = {} # JG: result of the optimization: after shifting the EV demand
            d_load_flex_heatpump_per_bus = {} # output of the optimization: optimally chosen portion of the HP demand (flexible portion of the HP demand)
            d_load_unflex_heatpump_per_bus = {} # unflexible portion of the HP demand (defined by the 1 - flexibily percentage)
            d_load_uncontrolled_heatpump_per_bus = {} # uncontrolled demand (input demand - just to compare witht the final controlled demand)
            d_load_controlled_heatpump_per_bus = {} # sum of the unflexible plus the flexible demand of HP after the optimization
            d_load_shift_up_heatpump_per_bus = {} # "shift up" of the controlled heatpump demand relative to the uncontrolled heat pump demand 
            d_load_shift_down_heatpump_per_bus = {} # "shift down" of the controlled heatpump demand relative to the uncontrolled heat pump demand 
            d_load_shift_up_per_bus_total = {}
            d_load_shift_down_per_bus_total = {}
            d_load_emob_PmaxLimit_per_bus = {}
            d_load_emob_PminLimit_per_bus = {}
            d_load_hp_EmaxCumulativeLimit_per_bus = {}
            d_load_hp_EminCumulativeLimit_per_bus = {}
                        
            for i in range(len(self.buses)):
                d_load_shift_up_dsm_per_bus[i] = list(res_change.expand_array(DCPF.get_loadshift_up(i)*baseMVA))
                d_load_shift_down_dsm_per_bus[i] = list(res_change.expand_array(DCPF.get_loadshift_down(i)*baseMVA))   
                d_load_shift_up_emob_per_bus[i] = list(res_change.expand_array(DCPF.get_emobloadshift_up(i)*baseMVA))
                d_load_shift_down_emob_per_bus[i] = list(res_change.expand_array(DCPF.get_emobloadshift_down(i)*baseMVA))
                d_load_emob_beforeshift_per_bus[i] = list(res_change.expand_array(DCPF.get_emobload_beforeshift(i)*baseMVA))
                d_load_emob_aftershift_per_bus[i] = list(res_change.expand_array(DCPF.get_emobload_aftershift(i)*baseMVA))
                d_load_flex_heatpump_per_bus[i] = list(res_change.expand_array(DCPF.get_hpflexload(i)*baseMVA))
                d_load_uncontrolled_heatpump_per_bus[i] = list(res_change.expand_array(self.adjusted_uncontrolledheatpumpload.get(i, [0.0]*int(self.timeperiods))))  # expanding the uncontrolled heat pump demand - this is for fair comparison when tpRes>1, JG: this is the full HP demand before shifting
                d_load_unflex_heatpump_per_bus[i] = list(res_change.expand_array(self.adjusted_heatpumpload.get(i, [0.0]*int(self.timeperiods)))) # expanding the unflex heat pump demand
                d_load_emob_PmaxLimit_per_bus[i] = list(res_change.expand_array(DCPF.get_emobload_PmaxHourlyLimit(i)*baseMVA))
                d_load_emob_PminLimit_per_bus[i] = list(res_change.expand_array(DCPF.get_emobload_PminHourlyLimit(i)*baseMVA))
                d_load_hp_EmaxCumulativeLimit_per_bus[i] = list(res_change.expand_array(DCPF.get_hpload_EmaxHourlyCumulativeLimit(i)*baseMVA))
                d_load_hp_EminCumulativeLimit_per_bus[i] = list(res_change.expand_array(DCPF.get_hpload_EminHourlyCumulativeLimit(i)*baseMVA))
         
            for k, list_timeseries in d_load_flex_heatpump_per_bus.items(): # calculating the controlled total heat pump power demand (unflexible + flexible portions)
                d_load_controlled_heatpump_per_bus[k] = [x + y for x,y in zip(list_timeseries, d_load_unflex_heatpump_per_bus[k])]
            
            for k, list_timeseries in d_load_uncontrolled_heatpump_per_bus.items():
                d_load_shift_up_heatpump_per_bus[k] = [abs(x - y) if (x-y < 0) else 0 for x,y in zip(list_timeseries, d_load_controlled_heatpump_per_bus[k]) ] # calculates the "shift up" of the final total controlled HP demand, with respect to the uncontrolled HP demand
            for k, list_timeseries in d_load_uncontrolled_heatpump_per_bus.items():
                d_load_shift_down_heatpump_per_bus[k] = [abs(x - y) if (x-y >= 0) else 0 for x,y in zip(list_timeseries, d_load_controlled_heatpump_per_bus[k]) ] # calculates the "shift down" of the final total controlled HP demand, with respect to the uncontrolled HP demand

            for k, list_timeseries in d_load_shift_up_dsm_per_bus.items():
                d_load_shift_up_per_bus_total[k] = [x + y + z for x,y,z in zip(list_timeseries,d_load_shift_up_emob_per_bus[k], d_load_shift_up_heatpump_per_bus[k])] # added the HP "shift" in the mix
            for k, list_timeseries in d_load_shift_down_dsm_per_bus.items():
                d_load_shift_down_per_bus_total[k] = [x + y + z for x,y, z in zip(list_timeseries,d_load_shift_down_emob_per_bus[k], d_load_shift_down_heatpump_per_bus[k])] # added the HP "shift" in the mix
            
            heatpump_controlled_demand_CH_hourly = {bus:[demand for demand in demands] for bus,demands in d_load_controlled_heatpump_per_bus.items() if bus in buses_CH} # Generating the "only CH" output 
            res.saveLoadGenerationTimeseries_Excel(heatpump_controlled_demand_CH_hourly, 'heatpumpDemand_hourly_CH.xlsx', 'CH')
            
            # JG: add more HP results outputs
            heatpump_demand_aftershift_All_hourly = {bus:[demand for demand in demands] for bus,demands in d_load_controlled_heatpump_per_bus.items()} # JG: All HP demand (flexible + inflexible portions) after shifting, for all nodes
            res.saveLoadGenerationTimeseriesNames_CSV(heatpump_demand_aftershift_All_hourly, 'LoadHeatPump_AfterShift_hourly_ALL_LP.csv', regions, names)
            heatpump_demand_flexible_portion_aftershift_All_hourly = {bus:[demand for demand in demands] for bus,demands in d_load_flex_heatpump_per_bus.items()} # JG: HP demand flexible portions after shifting, for all nodes
            res.saveLoadGenerationTimeseriesNames_CSV(heatpump_demand_flexible_portion_aftershift_All_hourly, 'LoadHeatPump_AfterShift_FlexiblePortion_hourly_ALL_LP.csv', regions, names)

            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shift_up_dsm_per_bus, 'LoadShiftDSMUp_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shift_down_dsm_per_bus, 'LoadShiftDSMDown_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shift_up_emob_per_bus, 'LoadShiftEmobUp_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shift_down_emob_per_bus, 'LoadShiftEmobDown_hourly_ALL_LP.xlsx', 'MW', regions, names)
            #res.saveLoadGenerationTimeseriesNames_Excel(d_load_emob_beforeshift_per_bus, 'LoadEmob_BeforeShift_hourly_ALL_LP.xlsx', 'MW', regions, names) # JG: done above, see emobility_demand_All_hourly
            #res.saveLoadGenerationTimeseriesNames_CSV(d_load_emob_beforeshift_per_bus, 'LoadEmob_BeforeShift_hourly_ALL_LP.csv', regions, names) # JG: done above, see emobility_demand_All_hourly
            res.saveLoadGenerationTimeseriesNames_CSV(d_load_emob_aftershift_per_bus, 'LoadEmob_AfterShift_hourly_ALL_LP.csv', regions, names) # JG: EV demand after shifting, for all nodes
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_flex_heatpump_per_bus, 'FlexLoadHeatPump_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_controlled_heatpump_per_bus, 'heatpumpDemand_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shift_up_heatpump_per_bus, 'LoadShiftHpUp_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_Excel(d_load_shift_down_heatpump_per_bus, 'LoadShiftHpDown_hourly_ALL_LP.xlsx', 'MW', regions, names)
            res.saveLoadGenerationTimeseriesNames_CSV(d_load_emob_PmaxLimit_per_bus, 'LoadEmob_PmaxLimit_hourly_ALL_LP.csv', regions, names)    # JG: EV constraint, upper limit on hourly power demand per node
            res.saveLoadGenerationTimeseriesNames_CSV(d_load_emob_PminLimit_per_bus, 'LoadEmob_PminLimit_hourly_ALL_LP.csv', regions, names)    # JG: EV constraint, lower limit on hourly power demand per node
            res.saveLoadGenerationTimeseriesNames_CSV(d_load_hp_EmaxCumulativeLimit_per_bus, 'LoadHeatPump_EmaxCumulativeLimit_hourly_ALL_LP.csv', regions, names)  # JG: HP constraint, upper limit on hourly cumulative energy consumption over each day per node
            res.saveLoadGenerationTimeseriesNames_CSV(d_load_hp_EminCumulativeLimit_per_bus, 'LoadHeatPump_EminCumulativeLimit_hourly_ALL_LP.csv', regions, names)  # JG: HP constraint, lower limit on hourly cumulative energy consumption over each day per node

            """
            output: final load for all nodes including neighbors (load - load shedding + upshifting - downshifting)
            -- if p2g2p units are built their hourly production/consumption is also subtracted/added to the final load sent to Cascades
            """
            load_timeseries_allnodes = {bus:res_change.expand_array([demand for demand in demands.values()]) for bus,demands in load_timeseries.items()}
            d_load_minus_loadshedding_full_LP = {}
            d_load_plus_upshifting = {}
            d_final_load_per_node_full_LP = {}
            for k, list_timeseries in load_timeseries_allnodes.items():
                d_load_minus_loadshedding_full_LP[k] = [x - y for x,y in zip(list_timeseries,d_load_shedding_per_bus[k])]
            for k, list_timeseries in d_load_minus_loadshedding_full_LP.items():
                d_load_plus_upshifting[k] = [x + y for x,y in zip(list_timeseries,d_load_shift_up_per_bus_total[k])]
            for k, list_timeseries in d_load_plus_upshifting.items():
                d_final_load_per_node_full_LP[k] = [x - y for x,y in zip(list_timeseries,d_load_shift_down_per_bus_total[k])]
            res.saveLoadGenerationTimeseriesNames_Excel(d_final_load_per_node_full_LP, 'FinalLoadCascades_hourly_ALL_LP.xlsx', 'MW', regions, names)
            
            #p2g2p production/consumption must be nodal in order to subtract them

            """
            output: dam production - CH and AT, DE, FR, IT (hourly)
            """
            dict_dams_CH = {}
            dict_dams_ATDEFRIT = {}
            hydro_Dam_notCH = [item for item in hydro_Dam if item in non_swiss_gens]
            for i in hydro_Dam_CH:
                dict_dams_CH[i] = list(res_change.expand_array(opt.get_generator_power(i)*baseMVA))
            for i in hydro_Dam_notCH:
                dict_dams_ATDEFRIT[i] = list(res_change.expand_array(opt.get_generator_power(i)*baseMVA))
            res.saveLoadGenerationTimeseries_Excel(dict_dams_CH, 'DamGeneration_hourly_CH_LP.xlsx', 'CH')
            res.saveLoadGenerationTimeseries_Excel(dict_dams_CH, 'DamGeneration_hourly_Neighbours_LP.xlsx', 'AT_DE_FR_IT')
    
            """
            output: pump production/consumption - CH and AT, DE, FR, IT (hourly)
            """
            dict_pumps_CH_generation = {}
            dict_pumps_CH_consumption = {}
            dict_pumps_ATDEFRIT_generation = {}
            dict_pumps_ATDEFRIT_consumption = {}
            hydro_Pumped_notCH = [item for item in hydro_Pumped if item in non_swiss_gens]
            for i in hydro_Pumped_CH:
                dict_pumps_CH_generation[i] = list(res_change.expand_array(opt.get_generator_power(i)*baseMVA))
                dict_pumps_CH_consumption[i] =list(res_change.expand_array(opt.get_generator_power_consumed(i)*baseMVA))
            for i in hydro_Pumped_notCH:
                dict_pumps_ATDEFRIT_generation[i] = list(res_change.expand_array(opt.get_generator_power(i)*baseMVA))
                dict_pumps_ATDEFRIT_consumption[i] = list(res_change.expand_array(opt.get_generator_power_consumed(i)*baseMVA))
            res.saveLoadGenerationTimeseries_Excel(dict_pumps_CH_generation, 'PumpGeneration_hourly_CH_LP.xlsx', 'CH')
            res.saveLoadGenerationTimeseries_Excel(dict_pumps_CH_consumption, 'PumpConsumption_hourly_CH_LP.xlsx', 'CH')
            res.saveLoadGenerationTimeseries_Excel(dict_pumps_ATDEFRIT_generation, 'PumpGeneration_hourly_Neighbours_LP.xlsx', 'AT_DE_FR_IT')
            res.saveLoadGenerationTimeseries_Excel(dict_pumps_ATDEFRIT_consumption, 'PumpConsumption_hourly_Neighbours_LP.xlsx', 'AT_DE_FR_IT')
        
            """
            output: RES production (pv, wind and biomass) in CH (total)
            """
            dfRES = df_CH[['Technology','Tot_Pgen_MWh']]
            dfRES_1 = dfRES[dfRES.Technology == "WindOn"]
            dfRES_2 = dfRES[dfRES.Technology == "PV"]
            dfRES_3 = dfRES[dfRES.Technology == "Biomass"]
            dfRES_new = pd.concat([dfRES_1, dfRES_2, dfRES_3])
            TOTALRES = dfRES_new['Tot_Pgen_MWh'].sum()
            res.saveScalars_Excel(np.column_stack([TOTALRES]), 'RESGeneration_total_CH_LP.xlsx', 'Tot_RES_Production_MWh', 'CH')
            
            """
            output: exports&imports CH to neighbours / neighbours to neighbours (total)
            """
            all_lines_tot_ActivePowerFlow = {}
            for i in DCPF.model.Lines:
                try:
                    all_lines_tot_ActivePowerFlow[i] = list(res_change.expand_array(DCPF.get_branch_flows(i))*baseMVA)
                except ValueError:
                    print(f"Error expanding branch flows for line {i}. This line may not be active or may not have valid flow data.")
    
            #lines in database always defined as from CH to surrounding countries
            cross_border_lines_CH_AT = []
            cross_border_lines_CH_DE = []
            cross_border_lines_CH_FR = []
            cross_border_lines_CH_IT = []
    
            #from neighbouring country to neighbouring country
            cross_border_lines_DE_FR = []
            cross_border_lines_DE_AT = []
            cross_border_lines_FR_IT = []
            cross_border_lines_IT_AT = []
    
            for Id, row in enumerate(self.lines):
                if row['tapRatio'] == 1: #this is a line (transformers have tapRatios != 1)
                    if row['Ind_CrossBord'] == 0:
                        if row['FromCountry'] == 'AT' and row['ToCountry'] == 'AT':
                            cross_border_lines_CH_AT.append(Id)
                        if row['FromCountry'] == 'DE' and row['ToCountry'] == 'DE':
                            cross_border_lines_CH_DE.append(Id)
                        if row['FromCountry'] == 'FR' and row['ToCountry'] == 'FR':
                            cross_border_lines_CH_FR.append(Id)
                        if row['FromCountry'] == 'IT' and row['ToCountry'] == 'IT':
                            cross_border_lines_CH_IT.append(Id)
                    if row['Ind_CrossBord'] == 1:
                        if row['FromCountry'] == 'DE' and row['ToCountry'] == 'FR':
                            cross_border_lines_DE_FR.append(Id)
                        if row['FromCountry'] == 'DE' and row['ToCountry'] == 'AT':
                            cross_border_lines_DE_AT.append(Id)
                        if row['FromCountry'] == 'FR' and row['ToCountry'] == 'IT':
                            cross_border_lines_FR_IT.append(Id)
                        if row['FromCountry'] == 'IT' and row['ToCountry'] == 'AT':
                            cross_border_lines_IT_AT.append(Id)
            
            cross_border_lines_CH = sorted(cross_border_lines_CH_AT+cross_border_lines_CH_DE+cross_border_lines_CH_FR+cross_border_lines_CH_IT)
    
            #CH
            Tot_Export_MWh_per_line_CH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_CH] #total exports from CH to Neighbours
            Tot_Import_MWh_per_line_CH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_CH] #total imports from Neighbours to CH
            #CH-AT
            Tot_Export_MWh_per_line_fromAT_toCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_CH_AT]
            Tot_Import_MWh_per_line_toAT_fromCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_CH_AT]
            df_CH_AT = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromAT_toCH)),abs(int(sum(Tot_Import_MWh_per_line_toAT_fromCH)))]),
                                        columns = ['Tot_Export_fAT_tCH_MWh','Tot_Import_tAT_fCH_MWh'])
            #CH-DE
            Tot_Export_MWh_per_line_fromDE_toCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_CH_DE]
            Tot_Import_MWh_per_line_toDE_fromCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_CH_DE]
            df_CH_DE = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromDE_toCH)),abs(int(sum(Tot_Import_MWh_per_line_toDE_fromCH)))]),
                                        columns = ['Tot_Export_fDE_tCH_MWh','Tot_Import_tDE_fCH_MWh'])
            #CH-FR
            Tot_Export_MWh_per_line_fromFR_toCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_CH_FR]
            Tot_Import_MWh_per_line_toFR_fromCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_CH_FR]
            df_CH_FR = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromFR_toCH)),abs(int(sum(Tot_Import_MWh_per_line_toFR_fromCH)))]),
                                               columns = ['Tot_Export_fFR_tCH_MWh','Tot_Import_tFR_fCH_MWh'])
            #CH-IT
            Tot_Export_MWh_per_line_fromIT_toCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_CH_IT]
            Tot_Import_MWh_per_line_toIT_fromCH = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_CH_IT]
            df_CH_IT = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromIT_toCH)),abs(int(sum(Tot_Import_MWh_per_line_toIT_fromCH)))]),
                                               columns = ['Tot_Export_fIT_tCH_MWh','Tot_Import_tIT_fCH_MWh'])
            #DE-FR
            Tot_Export_MWh_per_line_fromDE_toFR = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_DE_FR]
            Tot_Import_MWh_per_line_toDE_fromFR = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_DE_FR]
            df_DE_FR = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromDE_toFR)),abs(int(sum(Tot_Import_MWh_per_line_toDE_fromFR)))]),
                                               columns = ['Tot_Export_fDE_tFR_MWh','Tot_Import_tDE_fFR_MWh'])
            #DE-AT
            Tot_Export_MWh_per_line_fromDE_toAT = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_DE_AT]
            Tot_Import_MWh_per_line_toDE_fromAT = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_DE_AT]
            df_DE_AT = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromDE_toAT)),abs(int(sum(Tot_Import_MWh_per_line_toDE_fromAT)))]),
                                               columns = ['Tot_Export_fDE_tAT_MWh','Tot_Import_tDE_fAT_MWh'])
            #FR-IT
            Tot_Export_MWh_per_line_fromFR_toIT = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_FR_IT]
            Tot_Import_MWh_per_line_toFR_fromIT = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_FR_IT]
            df_FR_IT = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromFR_toIT)),abs(int(sum(Tot_Import_MWh_per_line_toFR_fromIT)))]),
                                               columns = ['Tot_Export_fFR_tIT_MWh','Tot_Import_tFR_fIT_MWh'])
            #IT-AT
            Tot_Export_MWh_per_line_fromIT_toAT = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v > 0) for i in cross_border_lines_IT_AT]
            Tot_Import_MWh_per_line_toIT_fromAT = [sum(v for v in all_lines_tot_ActivePowerFlow[i] if v < 0) for i in cross_border_lines_IT_AT]
            df_IT_AT = pd.DataFrame(np.column_stack([int(sum(Tot_Export_MWh_per_line_fromIT_toAT)),abs(int(sum(Tot_Import_MWh_per_line_toIT_fromAT)))]),
                                               columns = ['Tot_Export_fIT_tAT_MWh','Tot_Import_tIT_fAT_MWh'])
            
            res.saveExportsImports_Excel(np.column_stack([int(sum(Tot_Export_MWh_per_line_CH)),abs(int(sum(Tot_Import_MWh_per_line_CH)))]), 'ExportImport_total_CH_LP.xlsx', ['Tot_Export_CH_MWh','Tot_Import_CH_MWh'], 'CH_TOTAL')
            res.saveExportsImportsMultiple_Excel([df_CH_AT,df_CH_DE,df_CH_FR,df_CH_IT,df_DE_FR,df_DE_AT,df_FR_IT,df_IT_AT], ['CH_AT','CH_DE','CH_FR','CH_IT','DE_FR','DE_AT','FR_IT','IT_AT'],'ExportImport_total_ALL.xlsx')
     
            """
            output: cross-border flows CH (hourly)
            """
            d_branchflows_CrossCH_LP = {}
            for i in DCPF.model.Lines:
                if i in cross_border_lines_CH:
                    try:
                        d_branchflows_CrossCH_LP[i] = list(res_change.expand_array(DCPF.get_branch_flows(i)*baseMVA))  
                    except ValueError:
                        print("temp solution 1.")
            res.saveLoadGenerationTimeseries_Excel(d_branchflows_CrossCH_LP, 'CrossBorderBranchFlows_hourly_CH_LP.xlsx', 'MW')
            
            d_branchflows_CrossCH_LP_df = pd.DataFrame(d_branchflows_CrossCH_LP)
            d_branchflows_CrossCH_LP_df_export = d_branchflows_CrossCH_LP_df[d_branchflows_CrossCH_LP_df>0].sum(1)
            # need exports to be negative for post processing
            d_branchflows_CrossCH_LP_df_export_2 = d_branchflows_CrossCH_LP_df_export*-1
            d_branchflows_CrossCH_LP_df_export_2.to_csv(
                os.path.join(
                    results_folder,
                    "CH_exports.csv"
                ),
                header=True
            )
            d_branchflows_CrossCH_LP_df_import = d_branchflows_CrossCH_LP_df[d_branchflows_CrossCH_LP_df < 0].sum(1)
            # need imports to be positive for post processing
            d_branchflows_CrossCH_LP_df_import_2 = d_branchflows_CrossCH_LP_df_import*-1
            d_branchflows_CrossCH_LP_df_import_2.to_csv(
                os.path.join(
                    results_folder,
                    "CH_imports.csv"
                ),
                header=True
            )
            print(len(self.lines))
            print(len(self.line_id))
            """
            output: active power per branch ALL (hourly)
            """
            line_names = [{k:self.lines[k]['LineName'] for k in self.line_id}]
            line_rating = [{k:self.lines[k]['rateA'] for k in self.line_id}]
            d_branchflows = {}
            for i in DCPF.model.Lines:
                try:
                    d_branchflows[i] = list(res_change.expand_array(DCPF.get_branch_flows(i)*baseMVA))   
                except ValueError:
                    print(f"Temp solution 2.")
            res.saveLoadGenerationTimeseriesNames_Excel(d_branchflows, 'BranchFlows_hourly_ALL_LP.xlsx', 'MW', line_rating, line_names)

            """
            output: reserve contribution per generator (hourly)
            """
            dict_dams_FRRUp = {}
            dict_dams_FRRDown = {}
            dict_dams_RRUp = {}
            dict_dams_RRDown = {}
            dict_pumps_FRRUp = {}
            dict_pumps_FRRDown = {}
            dict_pumps_RRUp = {}
            dict_pumps_RRDown = {}
            dict_pumps_daily_FRRUp = {}
            dict_pumps_daily_FRRDown = {}
            dict_pumps_daily_RRUp = {}
            dict_pumps_daily_RRDown = {}
            dict_nuc_FRRUp = {}
            dict_nuc_FRRDown = {}
            dict_nuc_RRUp = {}
            dict_nuc_RRDown = {}
            dict_nuc_FRRUp_cand = {}
            dict_nuc_FRRDown_cand = {}
            dict_nuc_RRUp_cand = {}
            dict_nuc_RRDown_cand = {}
            dict_conv_FRRUp = {}
            dict_conv_FRRDown = {}
            dict_conv_RRUp = {}
            dict_conv_RRDown = {}

            for i in hydro_Dam_CH:
                dict_dams_FRRUp[i] = list(res_change.expand_array(hydro.frr_up_hydro_dam(i)*baseMVA))
                dict_dams_FRRDown[i] = list(res_change.expand_array(hydro.frr_down_hydro_dam(i)*baseMVA))
                dict_dams_RRUp[i] = list(res_change.expand_array(hydro.rr_up_hydro_dam(i)*baseMVA))
                dict_dams_RRDown[i] = list(res_change.expand_array(hydro.rr_down_hydro_dam(i)*baseMVA))
            for i in hydro_Pumped_CH_notdaily:
                dict_pumps_FRRUp[i] = list(res_change.expand_array(hydro.frr_up_hydro_pumped(i)*baseMVA))
                dict_pumps_FRRDown[i] = list(res_change.expand_array(hydro.frr_down_hydro_pumped(i)*baseMVA))
                dict_pumps_RRUp[i] = list(res_change.expand_array(hydro.rr_up_hydro_pumped(i)*baseMVA))
                dict_pumps_RRDown[i] = list(res_change.expand_array(hydro.rr_down_hydro_pumped(i)*baseMVA))
            for i in hydro_Pumped_CH_daily:
                dict_pumps_daily_FRRUp[i] = list(res_change.expand_array(hydro.frr_up_hydro_pumped_daily(i)*baseMVA))
                dict_pumps_daily_FRRDown[i] = list(res_change.expand_array(hydro.frr_down_hydro_pumped_daily(i)*baseMVA))
                dict_pumps_daily_RRUp[i] = list(res_change.expand_array(hydro.rr_up_hydro_pumped_daily(i)*baseMVA))
                dict_pumps_daily_RRDown[i] = list(res_change.expand_array(hydro.rr_down_hydro_pumped_daily(i)*baseMVA))   
            for i in nuclear_CH_exist:
                dict_nuc_FRRUp[i] = list(res_change.expand_array(cg.frr_up_conv(i)*baseMVA))
                dict_nuc_FRRDown[i] = list(res_change.expand_array(cg.frr_down_conv(i)*baseMVA))
                dict_nuc_RRUp[i] = list(res_change.expand_array(cg.rr_up_conv(i)*baseMVA))
                dict_nuc_RRDown[i] = list(res_change.expand_array(cg.rr_down_conv(i)*baseMVA))
            for i in nuclear_CH_candidate:
                dict_nuc_FRRUp_cand[i] = list(res_change.expand_array(CH_nuclear_invest.frr_up_conv_CHNuclear(i)*baseMVA))
                dict_nuc_FRRDown_cand[i] = list(res_change.expand_array(CH_nuclear_invest.frr_down_conv_CHNuclear(i)*baseMVA))
                dict_nuc_RRUp_cand[i] = list(res_change.expand_array(CH_nuclear_invest.rr_up_conv_CHNuclear(i)*baseMVA))
                dict_nuc_RRDown_cand[i] = list(res_change.expand_array(CH_nuclear_invest.rr_down_conv_CHNuclear(i)*baseMVA))
            for i in conv_CH_and_biomassCH_and_geothermalCH:
                dict_conv_FRRUp[i] = list(res_change.expand_array(cg_CH_noUC.frr_up_conv_CHLinear(i)*baseMVA))
                dict_conv_FRRDown[i] = list(res_change.expand_array(cg_CH_noUC.frr_down_conv_CHLinear(i)*baseMVA))
                dict_conv_RRUp[i] = list(res_change.expand_array(cg_CH_noUC.rr_up_conv_CHLinear(i)*baseMVA)) 
                dict_conv_RRDown[i] = list(res_change.expand_array(cg_CH_noUC.rr_down_conv_CHLinear(i)*baseMVA)) 
            
            df_dams_FRRUp = pd.DataFrame.from_dict(dict_dams_FRRUp)
            df_dams_FRRDown = pd.DataFrame.from_dict(dict_dams_FRRDown)
            df_dams_RRUp = pd.DataFrame.from_dict(dict_dams_RRUp)
            df_dams_RRDown = pd.DataFrame.from_dict(dict_dams_RRDown)
            df_pumps_FRRUp = pd.DataFrame.from_dict(dict_pumps_FRRUp)
            df_pumps_FRRDown = pd.DataFrame.from_dict(dict_pumps_FRRDown)
            df_pumps_RRUp = pd.DataFrame.from_dict(dict_pumps_RRUp)
            df_pumps_RRDown = pd.DataFrame.from_dict(dict_pumps_RRDown)
            df_pumps_daily_FRRUp = pd.DataFrame.from_dict(dict_pumps_daily_FRRUp)
            df_pumps_daily_FRRDown = pd.DataFrame.from_dict(dict_pumps_daily_FRRDown)
            df_pumps_daily_RRUp = pd.DataFrame.from_dict(dict_pumps_daily_RRUp)
            df_pumps_daily_RRDown = pd.DataFrame.from_dict(dict_pumps_daily_RRDown)
            df_nuc_FRRUp = pd.DataFrame.from_dict(dict_nuc_FRRUp)
            df_nuc_FRRDown = pd.DataFrame.from_dict(dict_nuc_FRRDown)
            df_nuc_RRUp = pd.DataFrame.from_dict(dict_nuc_RRUp)
            df_nuc_RRDown = pd.DataFrame.from_dict(dict_nuc_RRDown)
            df_nuc_FRRUp_cand = pd.DataFrame.from_dict(dict_nuc_FRRUp_cand)
            df_nuc_FRRDown_cand = pd.DataFrame.from_dict(dict_nuc_FRRDown_cand)
            df_nuc_RRUp_cand = pd.DataFrame.from_dict(dict_nuc_RRUp_cand)
            df_nuc_RRDown_cand = pd.DataFrame.from_dict(dict_nuc_RRDown_cand)
            df_conv_FRRUp = pd.DataFrame.from_dict(dict_conv_FRRUp)
            df_conv_FRRDown = pd.DataFrame.from_dict(dict_conv_FRRDown)
            df_conv_RRUp = pd.DataFrame.from_dict(dict_conv_RRUp)
            df_conv_RRDown = pd.DataFrame.from_dict(dict_conv_RRDown)
            
            res.saveExportsImportsMultiple_Excel([df_dams_FRRUp,df_dams_FRRDown,df_pumps_FRRUp,df_pumps_FRRDown,df_pumps_daily_FRRUp,df_pumps_daily_FRRDown,df_nuc_FRRUp,df_nuc_FRRDown,df_nuc_FRRUp_cand,df_nuc_FRRDown_cand,df_conv_FRRUp,df_conv_FRRDown],['FRRup_Dam','FRRdown_Dam','FRRup_Pump','FRRdown_Pump','FRRup_DailyPump','FRRdown_DailyPump','FRRup_Nuc','FRRdown_Nuc','FRRup_NucNEW','FRRdown_NucNEW','FRRup_Conv','FRRdown_Conv'],'ReserveContributionFRR_hourly_CH_LP.xlsx')
            res.saveExportsImportsMultiple_Excel([df_dams_RRUp,df_dams_RRDown,df_pumps_RRUp,df_pumps_RRDown,df_pumps_daily_RRUp,df_pumps_daily_RRDown,df_nuc_RRUp,df_nuc_RRDown,df_nuc_RRUp_cand,df_nuc_RRDown_cand,df_conv_RRUp,df_conv_RRDown],['RRup_Dam','RRdown_Dam','RRup_Pump','RRdown_Pump','RRup_DailyPump','RRdown_DailyPump','RRup_Nuc','RRdown_Nuc','RRup_NucNEW','RRdown_NucNEW','RRup_Conv','RRdown_Conv'],'ReserveContributionRR_hourly_CH_LP.xlsx')
            
            """
            output: all P2G2P time series
            """
            d_CH4_per_P2G2Punit = {}
            d_H2_per_P2G2Punit = {}
            d_CO2captured_per_P2G2Punit = {}
            d_Electrolyzer_consumption_per_P2G2Punit = {}
            d_CH4DAC_consumption_per_P2G2Punit = {}
            d_reconv_generation_per_P2G2Punit = {}
            if candidates_P2X:
                for Id, row in enumerate(self.generators):
                    if row['Technology'] == 'P2G2P':
                        d_CH4_per_P2G2Punit[Id] = list(res_change.expand_array(P2X1.get_CH4ForMarket_inv(Id)*baseMVA)) #MWh_th
                        d_H2_per_P2G2Punit[Id] = list(res_change.expand_array(P2X1.get_H2ForMarket_inv(Id)*baseMVA)) #tonne_H2
                        d_CO2captured_per_P2G2Punit[Id] = list(res_change.expand_array(P2X1.get_CO2captured_CH4DAC_inv(Id)*baseMVA)) #tonne_CO2
                        d_Electrolyzer_consumption_per_P2G2Punit[Id] = list(res_change.expand_array(P2X1.get_consumption_EL_inv(Id)*baseMVA)) #tonne_CO2
                        d_CH4DAC_consumption_per_P2G2Punit[Id] = list(res_change.expand_array(P2X1.get_CH4DAC_consumption_inv(Id)*baseMVA)) #MWh
                        d_reconv_generation_per_P2G2Punit[Id] = list(res_change.expand_array(P2X1.get_generation_reconv_inv(Id)*baseMVA)) #MWh
                    else:
                        d_CH4_per_P2G2Punit[Id] = [0] * config.timeperiods
                        d_H2_per_P2G2Punit[Id] = [0] * config.timeperiods
                        d_CO2captured_per_P2G2Punit[Id] = [0] * config.timeperiods   
                        d_Electrolyzer_consumption_per_P2G2Punit[Id] = [0] * config.timeperiods    
                        d_CH4DAC_consumption_per_P2G2Punit[Id] = [0] * config.timeperiods
                        d_reconv_generation_per_P2G2Punit[Id] = [0] * config.timeperiods
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_CH4_per_P2G2Punit, 'P2G2P_CH4Market_hourly_CH_LP.xlsx', 'MWh_th', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_H2_per_P2G2Punit, 'P2G2P_H2Market_hourly_CH_LP.xlsx', 'tonne_H2', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_CO2captured_per_P2G2Punit, 'P2G2P_CO2captured_hourly_CH_LP.xlsx', 'tonne_CO2', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_Electrolyzer_consumption_per_P2G2Punit, 'P2G2P_ELconsumption_hourly_CH_LP.xlsx', 'MWh', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_CH4DAC_consumption_per_P2G2Punit, 'P2G2P_CH4DACconsumption_hourly_CH_LP.xlsx', 'MWh', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_reconv_generation_per_P2G2Punit, 'P2G2P_REgeneration_hourly_CH_LP.xlsx', 'MWh', country_info, technology_info, name_info)
            else:
                for Id, row in enumerate(self.generators):
                    d_CH4_per_P2G2Punit[Id] = [0] * config.timeperiods
                    d_H2_per_P2G2Punit[Id] = [0] * config.timeperiods
                    d_CO2captured_per_P2G2Punit[Id] = [0] * config.timeperiods   
                    d_Electrolyzer_consumption_per_P2G2Punit[Id] = [0] * config.timeperiods    
                    d_CH4DAC_consumption_per_P2G2Punit[Id] = [0] * config.timeperiods
                    d_reconv_generation_per_P2G2Punit[Id] = [0] * config.timeperiods
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_CH4_per_P2G2Punit, 'P2G2P_CH4Market_hourly_CH_LP.xlsx', 'MWh_th', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_H2_per_P2G2Punit, 'P2G2P_H2Market_hourly_CH_LP.xlsx', 'tonne_H2', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_CO2captured_per_P2G2Punit, 'P2G2P_CO2captured_hourly_CH_LP.xlsx', 'tonne_CO2', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_Electrolyzer_consumption_per_P2G2Punit, 'P2G2P_ELconsumption_hourly_CH_LP.xlsx', 'MWh', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_CH4DAC_consumption_per_P2G2Punit, 'P2G2P_CH4DACconsumption_hourly_CH_LP.xlsx', 'MWh', country_info, technology_info, name_info)
                res.saveLoadGenerationTimeseriesNamesPlus_Excel(d_reconv_generation_per_P2G2Punit, 'P2G2P_REgeneration_hourly_CH_LP.xlsx', 'MWh', country_info, technology_info, name_info)
            
            """
            output: state of charge of H2 storages in CH (hourly)
            """
            if candidates_P2X:
                storage_value_H2_all_CH_LP = {}
                for i in candidates_P2X:
                    storage_value_H2_all_CH_LP[i] = list(res_change.expand_soc_array(P2X1.get_H2_SoC_inv(i)*baseMVA))# tonnes of H2
                df_storageH2CH = pd.DataFrame.from_dict(storage_value_H2_all_CH_LP)
                storage_value_H2_built_CH_LP = {}
                for i in P2X_built:
                    storage_value_H2_built_CH_LP[i] = list(res_change.expand_soc_array(P2X1.get_H2_SoC_inv(i)*baseMVA))# tonnes of H2
                df_storageH2built = pd.DataFrame.from_dict(storage_value_H2_built_CH_LP)
                res.saveExportsImportsMultiple_Excel([df_storageH2CH,df_storageH2built],['H2SoC_hourly_all','H2SoC_hourly_built'],'P2G2P_H2SoC_hourly_ALL_LP.xlsx')
            
            """
            output: H2/CH4 imports and costs + Revenue from sold H2/CH4 + Revenue from sequestered CO2
            """
            if candidates_P2X:
                import_value_H2_all_CH_LP = {}
                import_value_CH4_all_CH_LP = {}
                for i in candidates_P2X:
                    import_value_H2_all_CH_LP[i] = list(res_change.expand_array(P2X1.get_H2imports(i)*baseMVA))# tonnes of H2
                    import_value_CH4_all_CH_LP[i] = list(res_change.expand_array(P2X1.get_CH4imports(i)*baseMVA))# MWh-LHV
                df_H2importCH = pd.DataFrame.from_dict(import_value_H2_all_CH_LP)
                df_CH4importCH = pd.DataFrame.from_dict(import_value_CH4_all_CH_LP)
                importcost_value_H2_all_CH_LP = {}
                importcost_value_CH4_all_CH_LP  = {}
                revenue_H2_LP = {}
                revenue_CH4_LP = {}
                revenue_CO2_LP = {}
                for i in candidates_P2X:
                    importcost_value_H2_all_CH_LP[i] = list(res_change.expand_array(P2X1.get_H2imports_costs(i)))# EUR
                    importcost_value_CH4_all_CH_LP[i] = list(res_change.expand_array(P2X1.get_CH4imports_costs(i)))# EUR
                for i in P2X_built:
                    revenue_H2_LP[i] = list(res_change.expand_array(P2X1.get_H2sell_revenue(i)))# EUR
                    revenue_CH4_LP[i] = list(res_change.expand_array(P2X1.get_CH4sell_revenue(i)))# EUR
                    revenue_CO2_LP[i] = list(res_change.expand_array(P2X1.get_CO2store_revenue(i)))# EUR
                df_H2importcostCH = pd.DataFrame.from_dict(importcost_value_H2_all_CH_LP)
                df_CH4importcostCH = pd.DataFrame.from_dict(importcost_value_CH4_all_CH_LP)
                df_H2revenueCH = pd.DataFrame.from_dict(revenue_H2_LP)
                df_CH4revenueCH = pd.DataFrame.from_dict(revenue_CH4_LP)
                df_CO2revenueCH = pd.DataFrame.from_dict(revenue_CO2_LP)
                res.saveExportsImportsMultiple_Excel([df_H2importCH,df_H2importcostCH,df_CH4importCH,df_CH4importcostCH],['H2import_hourly_all','H2importcost_hourly_all','CH4import_hourly_all','CH4importcost_hourly_all'],'P2G2P_ImportsCosts_hourly_ALL_LP.xlsx')
                res.saveExportsImportsMultiple_Excel([df_H2revenueCH,df_CH4revenueCH,df_CO2revenueCH],['H2revenue_hourly_all','CH4revenue_hourly_all', 'CO2revenue_hourly_all'],'P2G2P_RevenuesCarriers_hourly_ALL_LP.xlsx')

            """
            output: H2 variables (non-import) all together
            """
            if candidates_P2X:
                H2_from_EL = {}
                H2_for_CH4 = {}
                H2_for_Market = {}
                H2_for_Gen = {}
                for i in candidates_P2X:
                    H2_from_EL[i] = list(res_change.expand_array(P2X1.get_H2production_EL(i)*baseMVA))# tonnes of H2
                    H2_for_CH4[i] = list(res_change.expand_array(P2X1.get_H2production_4Meth(i)*baseMVA))# tonnes of H2
                    H2_for_Market[i] = list(res_change.expand_array(P2X1.get_H2ForMarket_inv(i)*baseMVA))# tonnes of H2
                    H2_for_Gen[i] = list(res_change.expand_array(P2X1.get_H2production_GEN(i)*baseMVA))# tonnes of H2
                df_H2ELCH = pd.DataFrame.from_dict(H2_from_EL)
                df_H2CH4CH = pd.DataFrame.from_dict(H2_for_CH4)
                df_H2MarketCH = pd.DataFrame.from_dict(H2_for_Market)
                df_H2GENCH = pd.DataFrame.from_dict(H2_for_Gen)
                res.saveExportsImportsMultiple_Excel([df_H2ELCH,df_H2CH4CH,df_H2MarketCH,df_H2GENCH],['H2byEL_hourly_all','H2meth_hourly_all','H2market_hourly_all','H2recon_hourly_all'],'P2G2P_H2vars_hourly_LP.xlsx')
            
            # Only for matlab workflow
            if __name__ == "__main__":
                #get results and save them in a datetime stamped folder
                datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                new_results_folder = os.path.join("results", datestring)
                os.makedirs(new_results_folder, exist_ok=True)
                csv_extension = "csv"
                result_csv = glob.glob(f"*.{csv_extension}")
                xlsx_extension = "xlsx"
                result_xlsx = glob.glob(f"*.{xlsx_extension}")
                for f in result_csv:
                    shutil.copy(f, new_results_folder)
                for f in result_xlsx:
                    shutil.copy(f, new_results_folder)
            
        duration_log_dict['17-3'] = (time.time() - time_restarted) / 60
        logging.debug('Within GetOptimization, the total time for 17-3 was: ' + str(duration_log_dict['17-3']) + ' minutes')
        time_restarted = time.time()

        # Export stats, including timing information
        saveSelectedStats(opt, duration_log_dict, results_folder)

class CentIvModule:
    """Run CentIv simulations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.model = DataImport(config.timeperiods)

    def run(self) -> None:

        if self.config.single_electric_node:
            logging.warning("Single electric node mode is enabled. This will not run the full CentIv simulation.")
        else:
            logging.warning("Single electric node mode is disabled. Running full CentIv simulation.")                
        print("Starting CentIv simulation...")

        duration_dict = {}
        start_time = time.time()
        print("Starting MySQLConnect" + 70 * "-")
        self.model.MySQLConnect(
            self.config.DB_host,
            self.config.DB_name,
            self.config.DB_user,
            self.config.DB_pwd)
        duration_dict['MySQLConnect'] = time.time() - start_time
        logging.debug('MySQLConnect took ' + str(duration_dict['MySQLConnect']) + ' minutes')

        logging.debug("Starting LoadDistIvResults_MAT")
        self.model.LoadDistIvResults_MAT(self.config.distivresults_directory)
        duration_dict['LoadDistIvResults_MAT'] = (time.time() - start_time)/60

        logging.debug("Starting LoadABMResults_MAT")
        self.model.LoadABMResults_MAT(self.config.abmresults_directory)
        duration_dict['LoadABMResults_MAT'] = (time.time() - start_time)/60
        logging.debug("LoadABMResults_MAT took " + str(duration_dict['LoadABMResults_MAT']) + " minutes")

        print("Starting LoadCascadesResults_CSV")
        self.model.LoadCascadesResults_CSV(self.config.cascadesresults_directory)
        duration_dict['LoadCascadesResults_CSV'] = (time.time() - start_time)/60
        logging.debug("LoadCascadesResults_CSV took " + str(duration_dict['LoadCascadesResults_CSV']) + " minutes")
        
        print("Starting GetScenario")
        self.model.GetScenario(self.config)
        duration_dict['GetScenario'] = (time.time() - start_time)/60
        logging.debug("GetScenario took " + str(duration_dict['GetScenario']) + " minutes")

        logging.debug("Starting GetOptimization")
        self.model.GetOptimization(self.config)
        duration_dict['GetOptimization'] = (time.time() - start_time)/60
        logging.debug("GetOptimization took " + str(duration_dict['GetOptimization']) + " minutes")

        logging.debug("CentIv simulation completed in " + str((time.time() - start_time)/60) + " minutes.")
        logging.debug("Duration log in the run method duration_dict is:")
        logging.debug(duration_dict)


def parse_script_arguments() -> argparse.Namespace:
    """Automatically configure and parse script arguments so the result can
    be used to populate a Config object.
    
    A given Config attribute:
        > Config.any_attribute

    Will corresponds to a script argument similar to:
        > python create_scenario_fast.py --any-attribute
    """
    argp = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    default_config = Config()
    for field in dataclasses.fields(default_config):
        script_argument = f'--{field.name.replace("_", "-")}'
        argp.add_argument(
            script_argument,
            type=field.type,
            default=getattr(default_config, field.name))
    return argp.parse_args()


def main() -> None:
    args = parse_script_arguments()
    config = Config(**vars(args))
    simulation = CentIvModule(config)
    simulation.run()


if __name__ == "__main__":
    main()
