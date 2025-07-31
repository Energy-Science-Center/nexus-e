% =========================================================================
%>
%> (c)ETH Zurich 2022
%>
%>
%>
%> author : Jared Garrison
%> email  : garrison@fen.ethz.ch
%>
%> project : Nexus-e SWEET PATHFNDR
%>
%>
%> ========================================================================
%>
%> @brief: process & prep all data from CSV files from Calliope simulations
%>
%> @notes: uses the function datapackage (https://github.com/KrisKusano/datapackage)
%>       : uses normalized e-mobility demand profile from EXPANSE
%>       : currently this process runs for one user-specified scenario
%>       : assumes CHE is provided as 1 region only
%>       : assumes NTCs are same in both directions
%>
%> @versions:
%>
%>
%> ========================================================================


%% Clear / Close
clear all;
close all;
clc;

%% Begin
% begin timer
strData = tic;
disp(' ')
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp('Begin processing the Calliope data...')


%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Define Input Parameters
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% #1 OLD
% set the value for the NTC multiplier desired = 1, 0.3, or 0 
% (0 uses the values provided by Calliope (ExpandedNTCs))
% (1 and 0.3 replaces the NTCs with the most recent Nexus-e values and also
% multiplies these by 1 (CurrentNTCs) or 0.3 (ReducedNTCs))
%scen_opt_1_NTCmult = 'current';            	% value = 'current' or 'reduced' or 'expanded'

% #1
% set the value for the CO2 compensation option = 'abroad' or 'domestic'
% 'abroad' means the Swiss CO2 target can be compensated outside CH
% 'domestic' meand the Swiss CO2 tafter can only be compensated inside CH
scen_opt_1_Co2Comp = 'domestic';   	% value = 'abroad' or 'domestic'

% #2
% set the value for the energy market integration option = 'high' or 'low' 
% 'high' means current NTCs and ability to import fuels
% 'low' means reduced NTCs and no importing of fuels
scen_opt_2_MarketIntegr = 'low';

% #3
% set the value for the technology development option = 'progressive' or 'conservative'
% 'progressive' means 
% 'conservative' means...
scen_opt_3_TechDev = 'conservative';	% value = 'progressive' or 'conservative'

%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Data import
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('The pulling of the Calliope data files begins... ')
% Initilize
strPullData = tic;

% add path to datapackage function for reading 'friendly_data' type
addpath('/Users/jared/Documents/MATLAB/datapackage/')   % Macbook Pro
%addpath('/Users/jaredg/Documents/MATLAB/datapackage/')  % iMac

% set path to Calliope results folder
%datapath = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/CalliopeData_2022.01.31/friendly_data_nexus/';   	%FEN network drive from Jared's Mac
%datapath = '~/Documents/Research/ETH_FEN/GitLab/CalliopeData/';   	%local drive on Jared's iMac
datapath = '/Users/jared/Documents/Research/ETH_FEN/GitLab/EuroCalliope_Data/';     % path on jared's Macbook Pro
%datapath = '/Users/jaredg/Documents/Research/ETH_FEN/GitLab/CalliopeData/';         % path on jared's iMac


% define which set of results to import (which folder)
%folder = 'friendly_storylines_2016_1H_2030';
%folder = 'CalliopeData_2022.01.31/friendly_data_nexus';
%folder = 'ScenarioData_01/2050-comp-abroad_v3';        % co2abroad, Jan 2023
%folder = 'ScenarioData_01/2050-no-comp-abroad_v3';     % co2swiss, Jan 2023
%folder = 'ScenarioData_02_c/2050-compensation_abroad-x30_ntc-progressive-1h';    	% co2abroad, ntc reduced, tech progressive
%folder = 'ScenarioData_02_c/2050-compensation_abroad-dynamic_ntc-progressive-1h';	% co2abroad, ntc current, tech progressive
%folder = 'ScenarioData_03b/2050-compensation_abroad-high-progressive-1h';            % co2abroad, ntc current, tech progressive, Nov 2023
%folder = 'ScenarioData_03b/2050-compensation_abroad-high-conservative-1h';           % co2abroad, ntc current, tech conservative, Nov 2023
%folder = 'ScenarioData_03b/2050-no_compensation_abroad-low-progressive-1h';          % co2domestic, ntc reduced, tech progressive, Nov 2023
folder = 'ScenarioData_03b/2050-no_compensation_abroad-low-conservative-1h';         % co2domestic, ntc reduced, tech conservative, Nov 2023

% create full path to datafiles
readfile = strcat(datapath,folder,'/');

% import all data to Matlab
%[data, meta] = datapackage('http://data.okfn.org/data/core/gdp/');
[table_data, meta_data] = datapackage(readfile);

% add path to EXPANSE profile
%path2 = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/';   	%FEN network drive from Jared's Mac
ReadFile2 = strcat(datapath,'CreateEmobility_DemandProfile.xlsx');
% import EXPANSE normalized profile for e-mobility (normailzed by the
% annual total, unitless)
[Expanse_emobility_normalized] = xlsread(ReadFile2,'Expanse','B2:B8761');

% add path to normalized RAIL demand profile
% DON'T NEED ANYMORE, since Calliope includes rail demand in normal data
%path2 = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/';   	%FEN network drive from Jared's Mac
%ReadFile3 = strcat(datapath,'Manually_BaseDemand/rail_electricity_profile_v2.csv');
% import EXPANSE normalized profile for e-mobility (normailzed by the
% annual total, unitless)
%[rail_demand_profile] = readtable(ReadFile3);

disp(' ')
disp(['The total execution time for pulling all data files is: ', num2str(toc(strPullData)), ' (s) '])
disp('=========================================================================')

%}

%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Gather and organize some extra info
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('Data reduction begin... ')
% Initilize
strInit = tic;

% number of tables
ntables = length(table_data);

% names of tables in order the are in data
for i1=1:ntables
    table_names{i1,1} = meta_data.resources{i1}.name;
end


%% Version for CH2040
%{
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Data reduction
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% tables I need
%   - flow_in.csv                               -> electrified demand profiles          -> (by scenario)                ; (USED)                
%   - base_electricity_demand.csv               -> base electricity demand profiles     -> (independent of scenario)    ; (USED)
%   - rail_electricity_consumption.csv       	-> CH rail electricity demand profile   -> (independent of scenario)    ; (USED)
%   - net_import.csv                            -> nonCH-EU XB flow profiles            -> (by scenario)                ; (USED)
%   - nameplate_capacity.csv                    -> generator capacities                 -> (by scenario)                ; (USED)
%   - cost_per_nameplate_capacity.csv           -> investment costs (lifetime)          -> (independent of scenario)    ; (NOT USED)
%   - cost_per_flow_out.csv                     -> VOM costs                            -> (independent of scenario)    ; (NOT USED)
%   - annual_cost_per_nameplate_capacity.csv    -> FOM costs (annualized)               -> (independent of scenario)    ; (NOT USED)
%   - cost_per_flow_in.csv                      -> Fuel cost                            -> (independent of scenario)    ; (NOT USED) ; (MISSING)
%   - net_transfer_capacity.csv                 -> GTC limits for each XB               -> (by scenario)                ; (USED)
%   - total_system_emissions.csv                -> just to get list of scenarios        -> (by scenario)                ; (NOT USED) ; (MISSING)
%   - names.csv                                 -> descriptions of techs                -> (independent of scenario)    ; (NOT USED)
%   - flow_out.csv                              -> fixed injection profiles             -> (by scenario)                ; (USED)
%   - storage_capacity.csv                      -> energy storage capacities            -> (by scenario)                ; (USED)

% eliminate tables I don't need
tables_need_list = {'flow_in','base_electricity_demand','rail_electricity_consumption','net_import','nameplate_capacity','cost_per_nameplate_capacity','cost_per_flow_out','annual_cost_per_nameplate_capacity','cost_per_flow_in','net_transfer_capacity','total_system_emissions','names','flow_out','storage_capacity'};
table_data(~ismember(table_names,tables_need_list)) = [];
table_names(~ismember(table_names,tables_need_list)) = [];

disp(' ')
disp(['The total processing time for the data reduction is: ', num2str(toc(strInit)), ' (s) '])
disp('=========================================================================')


%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% List of scenarios (total_system_emissions.csv)
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('Processing the list of scenarios begins... ')
% Initilize
strScenarios = tic;

% detect which entry in table_data
idx_table_scenarios = find(strcmp(table_names,'flow_in'));

%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% create a list of the unique scenarios in these Calliope datafiles
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% create a temp table with the list of scenarios (have repeats)
tabledata_scenarios_temp = table_data{idx_table_scenarios}(:,1:5);

% replace NaNs in NTC_multiplier with 0 (not in PATHFNDR scenarios v1)
tabledata_scenarios_temp.NTC_multiplier(isnan(tabledata_scenarios_temp.NTC_multiplier)) = 0;

% get unique list of scenarios
tabledata_scenarios = unique(tabledata_scenarios_temp);

%--------------------------------------------------------------------------
% save processed data for DBcreation
%--------------------------------------------------------------------------

% save list of all the scenarios
CalliopeToNexuse.ScenariosList = tabledata_scenarios;

disp(' ')
disp(['The total processing time for the list of scenarios is: ', num2str(toc(strScenarios)), ' (s) '])
disp('=========================================================================')

%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Preprocessing: replace NaN with 0 (only for needed scenarios)
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('Preprocessing to remove NaNs begins... ')
% Initilize
strNaNs = tic;

% create list of tables that I use that are scenario-based
tablenames_NaNs = {'flow_in';'net_import';'nameplate_capacity';'net_transfer_capacity';'total_system_emissions';'flow_out';'storage_capacity'};

% loop over these tables and replace all instances of NaNs in the
% NTC_multiplier
for i9 = 1:length(tablenames_NaNs)
    
    % detect which entry in table_data
    idx_table_NaNs = find(strcmp(table_names,tablenames_NaNs(i9)));
    
    % replace all instances of NaNs in the NTC_multiplier
    table_data{idx_table_NaNs}.NTC_multiplier(isnan(table_data{idx_table_NaNs}.NTC_multiplier)) = 0;
    
end

disp(' ')
disp(['The total preprocessing time to remove NaNs is: ', num2str(toc(strNaNs)), ' (s) '])
disp('=========================================================================')
%}

%% Version for PATHFNDR Scenarios v1

%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Data reduction
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% tables I need
%   - flow_in.csv                               -> electrified demand profiles          -> (by scenario)                ; (USED, is big)     
%   - flow_in.csv                               -> base demand profiles                 -> (by scenario)                ; (USED, is big) 
%   - net_import.csv                            -> nonCH-EU XB flow profiles            -> (by scenario)                ; (USED, is big)
%   - nameplate_capacity.csv                    -> generator capacities                 -> (by scenario)                ; (USED)
%   - cost_per_nameplate_capacity.csv           -> investment costs (lifetime)          -> (independent of scenario)    ; (NOT USED)
%   - cost_per_flow_out.csv                     -> VOM costs                            -> (independent of scenario)    ; (NOT USED)
%   - annual_cost_per_nameplate_capacity.csv    -> FOM costs (annualized)               -> (independent of scenario)    ; (NOT USED)
%   - cost_per_flow_in.csv                      -> Fuel cost                            -> (independent of scenario)    ; (NOT USED)
%   - net_transfer_capacity.csv                 -> GTC limits for each XB               -> (by scenario)                ; (USED)
%   - total_system_emissions.csv                -> CO2 emitted                        	-> (by scenario)                ; (USED)
%   - names.csv                                 -> descriptions of techs            	-> (independent of scenario)    ; (USED)
%   - flow_out.csv                              -> fixed injection profiles             -> (by scenario)                ; (USED, is big)
%   - flow_out.csv                              -> CO2 captured                         -> (by scenario)                ; (USED, is big)
%   - storage_capacity.csv                      -> energy storage capacities            -> (by scenario)                ; (USED)
%   - final_consumption.csv                     -> annual CH rail demand                -> (by scenario)                ; (USED)
%   - duals.csv                                 -> shadow prices                        -> ()                           ; (USED)

% eliminate tables I don't need
tables_need_list = {'flow_in','net_import','nameplate_capacity','net_transfer_capacity','names','flow_out','storage_capacity','final_consumption','total_system_emissions','duals'};
table_data(~ismember(table_names,tables_need_list)) = [];
table_names(~ismember(table_names,tables_need_list)) = [];

disp(' ')
disp(['The total processing time for the data reduction is: ', num2str(toc(strInit)), ' (s) '])
disp('=========================================================================')

%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% List of scenarios (nameplate_capacity.csv)
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('Processing the list of scenarios begins... ')
% Initilize
strScenarios = tic;

% detect which entry in table_data
idx_table_scenarios = find(strcmp(table_names,'nameplate_capacity'));

%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% create a list of the unique scenarios in these Calliope datafiles
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% create a temp table with the list of scenarios (have repeats)
tabledata_scenarios_temp = table_data{idx_table_scenarios}(:,1:1);

% replace NaNs in NTC_multiplier with 0 (not in PATHFNDR scenarios v1)
%tabledata_scenarios_temp.NTC_multiplier(isnan(tabledata_scenarios_temp.NTC_multiplier)) = 0;

% get unique list of scenarios
tabledata_scenarios = unique(tabledata_scenarios_temp);

%--------------------------------------------------------------------------
% save processed data for DBcreation
%--------------------------------------------------------------------------

% save list of all the scenarios
CalliopeToNexuse.ScenariosList = tabledata_scenarios;

disp(' ')
disp(['The total processing time for the list of scenarios is: ', num2str(toc(strScenarios)), ' (s) '])
disp('=========================================================================')
%}

%% Version for CH2040
%{
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Process data from: base_electricity_demand.csv
%   -base electricity demand for each country (hourly), is combined
%    industry + buildings + rail
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% these profiles are hourly (8760 entries)
% the values in these profiles are NOT doubled
% these profile are independent of scenario

disp(' ')
disp('=========================================================================')
disp('Processing the base demand profiles begins... ')
% Initilize
strBaseLoad = tic;

% detect which entry in table_data
idx_table_baseelecdemand = find(strcmp(table_names,'base_electricity_demand'));

%--------------------------------------------------------------------------
% create identifiers
%--------------------------------------------------------------------------

% identify data for neighboring each country
idx_austria2 = strcmp(table_data{idx_table_baseelecdemand}.locs,'AUT');
idx_germany2 = strcmp(table_data{idx_table_baseelecdemand}.locs,'DEU');
idx_france2 = strcmp(table_data{idx_table_baseelecdemand}.locs,'FRA');
idx_italy2 = strcmp(table_data{idx_table_baseelecdemand}.locs,'ITA');
idx_restofEU2 = strcmp(table_data{idx_table_baseelecdemand}.locs,'rest_of_europe');

% identify data for any CH region
idx_swiss_AllAsSeparate2 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_baseelecdemand}.locs,'un',0),'CHE');

%--------------------------------------------------------------------------
% create hourly profiles for all CH
%--------------------------------------------------------------------------

% get demands for all CH regions
tabledata_elecload_base_CHsep = table_data{idx_table_baseelecdemand}(idx_swiss_AllAsSeparate2,:);

% get unique timesteps (will sort by smallest)
timesteps_num2 = unique(table_data{idx_table_baseelecdemand}.timesteps);
timesteps_vec2 = datevec(timesteps_num2);

% loop over each timestep to add up all CH
for i3 = 1:length(timesteps_num2)
    
    % identify all rows with this timestep
    idx_t_elecload_base     = tabledata_elecload_base_CHsep.timesteps == timesteps_num2(i3);
    
    % sum all entries for these timesteps
    data_elecload_base_CHsum_fullyr(i3,1)    = sum(tabledata_elecload_base_CHsep.base_electricity_demand(idx_t_elecload_base));
    
end

% convert CH elec demand to MWh and round to nearest MWh
data_elecload_base_CHsum_fullyr = round(data_elecload_base_CHsum_fullyr*1000*1000,0);

%--------------------------------------------------------------------------
% create hourly profiles for all other countries
%--------------------------------------------------------------------------

% get entries for each country
tabledata_elecload_base_DE_fullyr = table_data{idx_table_baseelecdemand}(idx_germany2,:);
tabledata_elecload_base_FR_fullyr = table_data{idx_table_baseelecdemand}(idx_france2,:);
tabledata_elecload_base_IT_fullyr = table_data{idx_table_baseelecdemand}(idx_italy2,:);
tabledata_elecload_base_AT_fullyr = table_data{idx_table_baseelecdemand}(idx_austria2,:);
tabledata_elecload_base_EU_fullyr = table_data{idx_table_baseelecdemand}(idx_restofEU2,:);

% get demands for each country, convert units to MWh and round to nearest
% MWh
data_elecload_base_DE_fullyr = round(tabledata_elecload_base_DE_fullyr.base_electricity_demand*1000*1000,0);
data_elecload_base_FR_fullyr = round(tabledata_elecload_base_FR_fullyr.base_electricity_demand*1000*1000,0);
data_elecload_base_IT_fullyr = round(tabledata_elecload_base_IT_fullyr.base_electricity_demand*1000*1000,0);
data_elecload_base_AT_fullyr = round(tabledata_elecload_base_AT_fullyr.base_electricity_demand*1000*1000,0);
data_elecload_base_EU_fullyr = round(tabledata_elecload_base_EU_fullyr.base_electricity_demand*1000*1000,0);

%--------------------------------------------------------------------------
% save processed data for DBcreation
%--------------------------------------------------------------------------
% hourly
CalliopeToNexuse.BaseElecDemand_hrly.CH 	= data_elecload_base_CHsum_fullyr;
CalliopeToNexuse.BaseElecDemand_hrly.DE 	= data_elecload_base_DE_fullyr;
CalliopeToNexuse.BaseElecDemand_hrly.FR 	= data_elecload_base_FR_fullyr;
CalliopeToNexuse.BaseElecDemand_hrly.IT 	= data_elecload_base_IT_fullyr;
CalliopeToNexuse.BaseElecDemand_hrly.AT 	= data_elecload_base_AT_fullyr;
CalliopeToNexuse.BaseElecDemand_hrly.EU 	= data_elecload_base_EU_fullyr;
% annual
CalliopeToNexuse.BaseElecDemand_yrly.CH 	= sum(data_elecload_base_CHsum_fullyr);
CalliopeToNexuse.BaseElecDemand_yrly.DE 	= sum(data_elecload_base_DE_fullyr);
CalliopeToNexuse.BaseElecDemand_yrly.FR 	= sum(data_elecload_base_FR_fullyr);
CalliopeToNexuse.BaseElecDemand_yrly.IT 	= sum(data_elecload_base_IT_fullyr);
CalliopeToNexuse.BaseElecDemand_yrly.AT 	= sum(data_elecload_base_AT_fullyr);
CalliopeToNexuse.BaseElecDemand_yrly.EU 	= sum(data_elecload_base_EU_fullyr);
% units
CalliopeToNexuse.Units.BaseElecDemand = ('MWh');

disp(' ')
disp(['The total processing time for the base demand profiles is: ', num2str(toc(strBaseLoad)), ' (s) '])
disp('=========================================================================')


%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Process data from: rail_electricity_consumption.csv
%   -rail electricity demand for each CH region (hourly)
%   -to be subtracted from base CH electricity demand
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% these profiles are hourly (8760 entries)
% the values in these profiles are NOT doubled
% these profile are independent of scenario

disp(' ')
disp('=========================================================================')
disp('Processing the rail demand profiles begins... ')
% Initilize
strRailLoad = tic;

% detect which entry in table_data
idx_table_railelecdemand = find(strcmp(table_names,'rail_electricity_consumption'));

%--------------------------------------------------------------------------
% create identifiers
%--------------------------------------------------------------------------

% identify data for any CH region
idx_swiss_AllAsSeparate3 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_railelecdemand}.locs,'un',0),'CHE');

%--------------------------------------------------------------------------
% create hourly profiles for all CH
%--------------------------------------------------------------------------

% get demands for all CH regions
tabledata_railload_base_CHsep = table_data{idx_table_railelecdemand}(idx_swiss_AllAsSeparate3,:);

% get unique timesteps (will sort by smallest)
timesteps_num3 = unique(table_data{idx_table_railelecdemand}.timesteps);
timesteps_vec3 = datevec(timesteps_num3);

% loop over each timestep to add up all CH
for i5 = 1:length(timesteps_num3)
    
    % identify all rows with this timestep
    idx_t_railload_base     = tabledata_railload_base_CHsep.timesteps == timesteps_num3(i5);
    
    % sum all entries for these timesteps
    data_railload_base_CHsum_fullyr(i5,1)    = sum(tabledata_railload_base_CHsep.rail_electricity_consumption(idx_t_railload_base));
    
end

% convert CH elec demand to MWh and round to nearest MWh
data_railload_base_CHsum_fullyr = round(data_railload_base_CHsum_fullyr*1000*1000,0);

%--------------------------------------------------------------------------
% save processed data for DBcreation
%--------------------------------------------------------------------------

% hourly
CalliopeToNexuse.RailElecDemand_hrly.CH 	= data_railload_base_CHsum_fullyr;
% annual
CalliopeToNexuse.RailElecDemand_yrly.CH 	= sum(data_railload_base_CHsum_fullyr);
% units
CalliopeToNexuse.Units.RailElecDemand = ('MWh');

disp(' ')
disp(['The total processing time for the rail demand profiles is: ', num2str(toc(strRailLoad)), ' (s) '])
disp('=========================================================================')
%}


%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% organize Fuel costs  (cost_per_flow_in.csv)
%   -Fuel costs by gen type for each country
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% these values are independent of scenario
% assumes each GenType has same Investement cost across all CH regions

% REMOVED: do not need anything from the fuel prices
%{
disp(' ')
disp('=========================================================================')
disp('Processing the generator Fuel costs begins... ')
% Initilize
strGenFuelCost = tic;

% detect which entry in table_data
idx_table_GenFuelCost = find(strcmp(table_names,'cost_per_flow_in'));

%--------------------------------------------------------------------------
% create identifiers
%--------------------------------------------------------------------------





disp(' ')
disp(['The total processing time for the generator Fuel costs is: ', num2str(toc(strGenFuelCost)), ' (s) '])
disp('=========================================================================')
%}


%% Begin Main Loop

for ScenarioNum=1:size(tabledata_scenarios,1)
    
    % Initilize
    strScen = tic;
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Get scenario options
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    
    % first save details of the desired scenario
    CalliopeToNexuse.Scenario_current = tabledata_scenarios(ScenarioNum,:);
    %CalliopeToNexuse.Scenario_current.NTC_multiplier = scen_opt_1_NTCmult;
    
    
    % get scenario params
    scen_opt_1_name = CalliopeToNexuse.Scenario_current.scenario;
    %filename = strcat('CalliopeToNexuse_PATHFNDR_ScenSet1_',scen_opt_1_name,'.mat');
    
    disp(' ')
    disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    disp(['Begin processing for Scenario ', num2str(ScenarioNum),': ',char(scen_opt_1_name)])
    
    % ------------------------------------------------
    % PATHFNDR Scenarios Nov 2023 (depend on CO2 compensation, market integration, tech development)
    % ------------------------------------------------
    % auto detect and set scenario name
    % First: CO2 Compensation  
    if strcmp(scen_opt_1_Co2Comp,'abroad')
            % CH is allowed to compensate abroad
            scen_opt_1_Co2Comp_name = 'CO2CompAbroad'; % value = 'CompAbroad' or 'CompDomest'     
    elseif strcmp(scen_opt_1_Co2Comp,'domestic')
            % CH is NOT allowed to compensate abroad
            scen_opt_1_Co2Comp_name = 'CO2CompDomest'; % value = 'CompAbroad' or 'CompDomest'
    else
            % throw an error message
            disp(' ')
            disp('=========================================================================')
            error('ERROR: improper CO2 compensation option defined, should be: abroad or domestic...')
    end
    
    % Second: Energy Market Integration
    if strcmp(scen_opt_2_MarketIntegr, 'high') 
        % is current grid NTCs
        scen_opt_2_MarketIntegr_name = 'MarketIntHigh';    % value = 'CurrentNTCs', 'ReducedNTCs', or 'ExpandedNTCs'
    elseif strcmp(scen_opt_2_MarketIntegr, 'low')
        % is reduced grid NTCs
        scen_opt_2_MarketIntegr_name = 'MarketIntLow';    % value = 'CurrentNTCs', 'ReducedNTCs', or 'ExpandedNTCs'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper NTC multiplier defined, should be: current or reduced or expanded...') 
    end
    
    % Third: Technology Development
    if strcmp(scen_opt_3_TechDev,'progressive')
        % 
        scen_opt_3_TechDev_name = 'TechProg'; % value = 'progressive' or 'conservative'
    elseif strcmp(scen_opt_3_TechDev,'conservative')
        % 
        scen_opt_3_TechDev_name = 'TechCons'; % value = 'progressive' or 'conservative'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper Technology Development option defined, should be: progressive or conservative...')
    end
    % Set the appropriate filename
    filename = strcat('CalliopeToNexuse_PATHFNDR_ScenSet3b_',scen_opt_1_Co2Comp_name,'_',scen_opt_2_MarketIntegr_name,'_',scen_opt_3_TechDev_name,'.mat');
    
    % store scenario options
    CalliopeToNexuse.Scenario_current.Co2Comp       = scen_opt_1_Co2Comp_name;
    CalliopeToNexuse.Scenario_current.MarketIntegr  = scen_opt_2_MarketIntegr_name;
    CalliopeToNexuse.Scenario_current.TechDev       = scen_opt_3_TechDev_name;
    
    % ------------------------------------------------
    % PATHFNDR Scenarios Aug 2023 (depend on NTC and CO2 compensation)
    % ------------------------------------------------
    %{
    % auto detect and set scenario name
    % First: NTC
    if strcmp(scen_opt_1_NTCmult, 'current') 
        % is current grid NTCs
        scen_opt_1_NTCmult_name = 'CurrentNTCs';    % value = 'CurrentNTCs', 'ReducedNTCs', or 'ExpandedNTCs'
    elseif strcmp(scen_opt_1_NTCmult, 'reduced')
        % is reduced grid NTCs
        scen_opt_1_NTCmult_name = 'ReducedNTCs';    % value = 'CurrentNTCs', 'ReducedNTCs', or 'ExpandedNTCs'
    elseif strcmp(scen_opt_1_NTCmult, 'expanded')
        % is expanded grid NTCs
        scen_opt_1_NTCmult_name = 'ExpandedNTCs';    % value = 'CurrentNTCs', 'ReducedNTCs', or 'ExpandedNTCs'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper NTC multiplier defined, should be: current or reduced or expanded...') 
    end
    % Second: CO2 Compensation  
    if strcmp(scen_opt_2_Co2Comp,'abroad')
            % CH is allowed to compensate abroad
            scen_opt_2_Co2Comp_name = 'CO2CompAbroad'; % value = 'CompAbroad' or 'CompDomest'     
    elseif strcmp(scen_opt_2_Co2Comp,'domestic')
            % CH is NOT allowed to compensate abroad
            scen_opt_2_Co2Comp_name = 'CO2CompDomest'; % value = 'CompAbroad' or 'CompDomest'
    else
            % throw an error message
            disp(' ')
            disp('=========================================================================')
            error('ERROR: improper CO2 compensation option defined, should be: abroad or domestic...')
    end
    % Third: Technology Development
    if strcmp(scen_opt_3_TechDev,'progressive')
        % 
        scen_opt_3_TechDev_name = 'TechProg'; % value = 'progressive' or 'conservative'
    elseif strcmp(scen_opt_3_TechDev,'conservative')
        % 
        scen_opt_3_TechDev_name = 'TechCons'; % value = 'progressive' or 'conservative'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper Technology Development option defined, should be: progressive or conservative...')
    end
    % Set the appropriate filename
    filename = strcat('CalliopeToNexuse_PATHFNDR_ScenSet2_',scen_opt_1_NTCmult_name,'_',scen_opt_2_Co2Comp_name,'_',scen_opt_3_TechDev_name,'.mat');
    %}
    
    % ------------------------------------------------    
    % PATHFNDR Scenarios Jan 2023 (only depend on NTC)
    % ------------------------------------------------
    %{
    % auto detect and set scenario name
    if scen_opt_1_NTCmult == 1
        % is current grid NTCs
        % set filename for saving processed results based on scenario
        filename = strcat('CalliopeToNexuse_PATHFNDR_ScenSet1_','CurrentNTCs_',scen_opt_1_name,'.mat');
    elseif scen_opt_1_NTCmult == 0.3
        % is reduced grid NTCs
        % set filename for saving processed results based on scenario
        filename = strcat('CalliopeToNexuse_PATHFNDR_ScenSet1_','ReducedNTCs_',scen_opt_1_name,'.mat');
    elseif scen_opt_1_NTCmult == 0
        % is expanded grid NTCs
        % set filename for saving processed results based on scenario
        filename = strcat('CalliopeToNexuse_PATHFNDR_ScenSet1_','ExpandedNTCs_',scen_opt_1_name,'.mat');
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        %disp('ERROR: number of timesteps in Demand data is less than 8760...')
        error('ERROR: improper NTC multiplier defined, should be: 1.0, 0.3 or 0...') 
    end
    %}
            
            
    % ------------------------------------------------    
    % CH2040 Apr 2021
    % ------------------------------------------------
    %{
    % #1
    % GTC_limit ('constr' or 'unconstr')
    scen_opt_1_GTClim       = CalliopeToNexuse.Scenario_current.GTC_limit{1};
    % #2
    % NTC_multiplier (1 or 0.3 or 0)
    scen_opt_2_NTCmult      = CalliopeToNexuse.Scenario_current.NTC_multiplier;
    % #3
    % heat_and_transport_electrification_limit_fraction (1)
    scen_opt_3_eleclim      = CalliopeToNexuse.Scenario_current.heat_and_transport_electrification_limit_fraction;
    % #4
    % swiss_fuel_autarky ('Shared' or 'Autarkic')
    scen_opt_4_fuelautky    = CalliopeToNexuse.Scenario_current.swiss_fuel_autarky{1};
    % #5
    % swiss_net_transfer_constraint ('NonZero' or 'Zero')
    scen_opt_5_balimpexp    = CalliopeToNexuse.Scenario_current.swiss_net_transfer_constraint{1};
    %}
    %{
    % auto detect and set scenario name
    if strcmp(scen_opt_1_GTClim,'constr')
        if scen_opt_2_NTCmult == 1
            if strcmp(scen_opt_4_fuelautky,'Shared')
                if strcmp(scen_opt_5_balimpexp,'NonZero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_1_','FullGrid_','WithFuelImport_','NoImpExpBal','.mat');
                elseif strcmp(scen_opt_5_balimpexp,'Zero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_2_','FullGrid_','WithFuelImport_','WithImpExpBal','.mat');
                else
                    error('ERROR during Calliope scenario params processing: unknown NetTransfer Constraint from Calliope')
                end
                
            elseif strcmp(scen_opt_4_fuelautky,'Autarkic')
                if strcmp(scen_opt_5_balimpexp,'NonZero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_7_','FullGrid_','NoFuelImport_','NoImpExpBal','.mat');
                elseif strcmp(scen_opt_5_balimpexp,'Zero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_8_','FullGrid_','NoFuelImport_','WithImpExpBal','.mat');
                else
                    error('ERROR during Calliope scenario params processing: unknown NetTransfer Constraint from Calliope')
                end
                
            else
                error('ERROR during Calliope scenario params processing: unknown Fuel Autarky from Calliope')
            end
            
        elseif scen_opt_2_NTCmult == 0.3
            if strcmp(scen_opt_4_fuelautky,'Shared')
                if strcmp(scen_opt_5_balimpexp,'NonZero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_3_','ReducedGrid_','WithFuelImport_','NoImpExpBal','.mat');
                elseif strcmp(scen_opt_5_balimpexp,'Zero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_4_','ReducedGrid_','WithFuelImport_','WithImpExpBal','.mat');
                else
                    error('ERROR during Calliope scenario params processing: unknown NetTransfer Constraint from Calliope')
                end
                
            elseif strcmp(scen_opt_4_fuelautky,'Autarkic')
                if strcmp(scen_opt_5_balimpexp,'NonZero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_9_','ReducedGrid_','NoFuelImport_','NoImpExpBal','.mat');
                elseif strcmp(scen_opt_5_balimpexp,'Zero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_10_','ReducedGrid_','NoFuelImport_','WithImpExpBal','.mat');
                else
                    error('ERROR during Calliope scenario params processing: unknown NetTransfer Constraint from Calliope')
                end
                
            else
                error('ERROR during Calliope scenario params processing: unknown Fuel Autarky from Calliope')
            end
            
        elseif scen_opt_2_NTCmult == 0
            error('ERROR during Calliope scenario params processing: Incompatible combination of GTC & NTC scenario parameters from Calliope')
        else
            error('ERROR during Calliope scenario params processing: unknown NTC Multiplier from Calliope')
        end
        
    elseif strcmp(scen_opt_1_GTClim,'unconstr')
        if scen_opt_2_NTCmult == 1
            error('ERROR during Calliope scenario params processing: Incompatible combination of GTC & NTC scenario parameters from Calliope')
        elseif scen_opt_2_NTCmult == 0.3
            error('ERROR during Calliope scenario params processing: Incompatible combination of GTC & NTC scenario parameters from Calliope')
        elseif scen_opt_2_NTCmult == 0
            if strcmp(scen_opt_4_fuelautky,'Shared')
                if strcmp(scen_opt_5_balimpexp,'NonZero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_5_','ExpandedGrid_','WithFuelImport_','NoImpExpBal','.mat');
                elseif strcmp(scen_opt_5_balimpexp,'Zero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_6_','ExpandedGrid_','WithFuelImport_','WithImpExpBal','.mat');
                else
                    error('ERROR during Calliope scenario params processing: unknown NetTransfer Constraint from Calliope')
                end
                
            elseif strcmp(scen_opt_4_fuelautky,'Autarkic')
                if strcmp(scen_opt_5_balimpexp,'NonZero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_11_','ExpandedGrid_','NoFuelImport_','NoImpExpBal','.mat');
                elseif strcmp(scen_opt_5_balimpexp,'Zero')
                    % set filename for saving processed results based on scenario
                    filename = strcat('CalliopeToNexuse_12_','ExpandedGrid_','NoFuelImport_','WithImpExpBal','.mat');
                else
                    error('ERROR during Calliope scenario params processing: unknown NetTransfer Constraint from Calliope')
                end
                
            else
                error('ERROR during Calliope scenario params processing: unknown Fuel Autarky from Calliope')
            end
            
        else
            error('ERROR during Calliope scenario params processing: unknown NTC Multiplier from Calliope')
        end
        
    else
        error('ERROR during Calliope scenario params processing: unknown GTC Limit from Calliope')
    end
    %}
    
    
    %%
    
    % REMOVED: this manual method was used before looping and automating
    % the scenario naming
    
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Define scenario options and other options
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    %{
    % set desired scenario
    % ScenarioNum = 1;      % Full GTC,      can import fuels,  and can Import more than Export
    % ScenarioNum = 2;      % Full GTC,      can import fuels,  and Import/Export must balance
    % ScenarioNum = 3;      % Reduced GTC,   can import fuels,  and can Import more than Export
    % ScenarioNum = 4;      % Reduced GTC,   can import fuels,  and Import/Export must balance
    % ScenarioNum = 5;      % Unlimited GTC, can import fuels,  and can Import more than Export
    % ScenarioNum = 6;      % Unlimited GTC, can import fuels,	and Import/Export must balance
    % ScenarioNum = 7;      % Full GTC,      no import fuels,   and can Import more than Export
    % ScenarioNum = 8;      % Full GTC,      no import fuels,   and Import/Export must balance
    % ScenarioNum = 9;      % Reduced GTC,   no import fuels,   and can Import more than Export
    % ScenarioNum = 10;     % Reduced GTC,   no import fuels,   and Import/Export must balance
    % ScenarioNum = 11;     % Unlimited GTC, no import fuels,   and can Import more than Export
    % ScenarioNum = 12;     % Unlimited GTC, no import fuels,	and Import/Export must balance
    
    % set all params for selected scenario
    switch ScenarioNum
        case 1
            % Full GTC, can import fuels, and can Import more than Export
            
            % set scenario option params
            % #1
            % GTC_limit ('constr' or 'unconstr')
            scen_opt_1_GTClim = 'constr';
            % #2
            % NTC_multiplier (1 or 0.3 or NaN)
            % will replace NaNs with 0s
            scen_opt_2_NTCmult = 1;
            % #3
            % heat_and_transport_electrification_limit_fraction (1)
            scen_opt_3_eleclim = 1;
            % #4
            % swiss_fuel_autarky ('Shared' or 'Autarkic')
            scen_opt_4_fuelautky = 'Shared';
            % #5
            % swiss_net_transfer_constraint ('NonZero' or 'Zero')
            scen_opt_5_balimpexp = 'NonZero';
            
            % set filename for saving processed results based on scenario
            filename = strcat('CalliopeToNexuse_1_','FullGrid_','WithFuelImport_','NoImpExpBal','.mat');
            
        case 2
            % Full GTC, can import fuels, and Import/Export must balance
            
            % set scenario option params
            % #1
            % GTC_limit ('constr' or 'unconstr')
            scen_opt_1_GTClim = 'constr';
            % #2
            % NTC_multiplier (1 or 0.3 or NaN)
            % will replace NaNs with 0s
            scen_opt_2_NTCmult = 1;
            % #3
            % heat_and_transport_electrification_limit_fraction (1)
            scen_opt_3_eleclim = 1;
            % #4
            % swiss_fuel_autarky ('Shared' or 'Autarkic')
            scen_opt_4_fuelautky = 'Shared';
            % #5
            % swiss_net_transfer_constraint ('NonZero' or 'Zero')
            scen_opt_5_balimpexp = 'Zero';
            
            % set filename for saving processed results based on scenario
            filename = strcat('CalliopeToNexuse_2_','FullGrid_','WithFuelImport_','WithImpExpBal','.mat');
            
        case 3
            % Reduced GTC, can import fuels, and can Import more than Export
            
            % set scenario option params
            % #1
            % GTC_limit ('constr' or 'unconstr')
            scen_opt_1_GTClim = 'constr';
            % #2
            % NTC_multiplier (1 or 0.3 or NaN)
            % will replace NaNs with 0s
            scen_opt_2_NTCmult = 0.3;
            % #3
            % heat_and_transport_electrification_limit_fraction (1)
            scen_opt_3_eleclim = 1;
            % #4
            % swiss_fuel_autarky ('Shared' or 'Autarkic')
            scen_opt_4_fuelautky = 'Shared';
            % #5
            % swiss_net_transfer_constraint ('NonZero' or 'Zero')
            scen_opt_5_balimpexp = 'NonZero';
            
            % set filename for saving processed results based on scenario
            filename = strcat('CalliopeToNexuse_3_','ReducedGrid_','WithFuelImport_','NoImpExpBal','.mat');
            
        case 4
            % Reduced GTC, can import fuels, and Import/Export must balance
            
            % set scenario option params
            % #1
            % GTC_limit ('constr' or 'unconstr')
            scen_opt_1_GTClim = 'constr';
            % #2
            % NTC_multiplier (1 or 0.3 or NaN)
            % will replace NaNs with 0s
            scen_opt_2_NTCmult = 0.3;
            % #3
            % heat_and_transport_electrification_limit_fraction (1)
            scen_opt_3_eleclim = 1;
            % #4
            % swiss_fuel_autarky ('Shared' or 'Autarkic')
            scen_opt_4_fuelautky = 'Shared';
            % #5
            % swiss_net_transfer_constraint ('NonZero' or 'Zero')
            scen_opt_5_balimpexp = 'Zero';
            
            % set filename for saving processed results based on scenario
            filename = strcat('CalliopeToNexuse_4_','ReducedGrid_','WithFuelImport_','WithImpExpBal','.mat');
            
        case 5
            % Unlimited GTC, can import fuels, and can Import more than Export
            
            % set scenario option params
            % #1
            % GTC_limit ('constr' or 'unconstr')
            scen_opt_1_GTClim = 'unconstr';
            % #2
            % NTC_multiplier (1 or 0.3 or NaN)
            % will replace NaNs with 0s
            scen_opt_2_NTCmult = 0;
            % #3
            % heat_and_transport_electrification_limit_fraction (1)
            scen_opt_3_eleclim = 1;
            % #4
            % swiss_fuel_autarky ('Shared' or 'Autarkic')
            scen_opt_4_fuelautky = 'Shared';
            % #5
            % swiss_net_transfer_constraint ('NonZero' or 'Zero')
            scen_opt_5_balimpexp = 'NonZero';
            
            % set filename for saving processed results based on scenario
            filename = strcat('CalliopeToNexuse_5_','ReducedGrid_','WithFuelImport_','NoImpExpBal','.mat');
            
        case 6
            % Unlimited GTC, can import fuels, and Import/Export must balance
            
            % set scenario option params
            % #1
            % GTC_limit ('constr' or 'unconstr')
            scen_opt_1_GTClim = 'unconstr';
            % #2
            % NTC_multiplier (1 or 0.3 or NaN)
            % will replace NaNs with 0s
            scen_opt_2_NTCmult = 0;
            % #3
            % heat_and_transport_electrification_limit_fraction (1)
            scen_opt_3_eleclim = 1;
            % #4
            % swiss_fuel_autarky ('Shared' or 'Autarkic')
            scen_opt_4_fuelautky = 'Shared';
            % #5
            % swiss_net_transfer_constraint ('NonZero' or 'Zero')
            scen_opt_5_balimpexp = 'Zero';
            
            % set filename for saving processed results based on scenario
            filename = strcat('CalliopeToNexuse_6_','ReducedGrid_','WithFuelImport_','WithImpExpBal','.mat');
            
        otherwise
            
    end
    %}
    
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Other columns
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    
    % Other #1
    % techs
    %   for electrified loads (flow_in.csv)
    %      -group into: base, electrolysis, heat pump, emobility
    %   for producing electricity, there are 17 techs with carrier = electricity:
    %      -battery, biofuel_to_liquids(ignore), ccgt, ccgt_cccs, 
    %       chp_biofuel_extraction, chp_wte_back_pressure, chp_wte_back_pressure_ccs,
    %       coal, coal_ccs, hydro_reservoir, hydro_run_of_river, nuclear, 
    %       open_field_pv, pumped_hydro, roof_mounted_pv, wind_offshore, 
    %       wind_onshore
    %      -also, 'dac' and 'electric_hob' show up in capacities data, but
    %       these are electric loads not generators, so ignore them also.
    %
    % Other #2
    % locs ('AUT', 'DEU', 'FRA', 'ITA', 'CHE_1' -to- 'CHE_20', 'rest_of_europe')
    %
    % Other #3
    % carriers ('electricity', don't need others)
    
    
    %% Version for PATHFNDR Scenarios v1
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: final_consumption.csv
    %   -rail electricity demand for each CH region (hourly)
    %   -to be subtracted from base CH electricity demand
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % this value is annual (1 entry)
    % the values in these profiles are NOT doubled
    % the annual total could depend on the scenario
    %{
    disp(' ')
    disp('=========================================================================')
    disp('Processing the rail demand profiles begins... ')
    % Initilize
    strRailLoad = tic;
    
    % detect which entry in table_data
    idx_table_railelecdemand = find(strcmp(table_names,'final_consumption'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario2 = strcmp(table_data{idx_table_railelecdemand}.scenario,scen_opt_1_name);
    
    % identify data for any CH region (if only one region), should only be one
    % entry for full year
    idx_swiss3 = strcmp(table_data{idx_table_railelecdemand}.locs,'CHE');
    % identify data for any CH region
    %idx_swiss_AllAsSeparate3 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_railelecdemand}.locs,'un',0),'CHE');
    
    % identify data for Rail demand
    idx_elecload_rail1 = strcmp(table_data{idx_table_railelecdemand}.subsector,'Rail');
    
    % create identifiers for CH and rail demand
    idx_elecload_rail_CH       = idx_scenario2  & idx_elecload_rail1 	& idx_swiss3;
    
    %--------------------------------------------------------------------------
    % create hourly profile for CH
    %--------------------------------------------------------------------------
    
    % first, get annual demand for CH region (is in TWh)
    tabledata_rail_demand_annual_CH = table_data{idx_table_railelecdemand}(idx_elecload_rail_CH,:);
    
    % already have manually pulled the normalized rail profile from Francesco,
    % now I just need to scale it to the CH annual rail demand
    data_railload_base_CHsum_fullyr = tabledata_rail_demand_annual_CH.final_consumption * rail_demand_profile.rail_electricity_profile;
    
    % check if normalized profile is 8760 entries, edit or stop if needed
    if length(data_railload_base_CHsum_fullyr) > 8760
        % more than expected time steps...just ignore the last ones to keep
        % only 8760
        data_railload_base_CHsum_fullyr = data_railload_base_CHsum_fullyr(1:8760);
    elseif length(data_railload_base_CHsum_fullyr) < 8760
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        %disp('ERROR: number of timesteps in Demand data is less than 8760...')
        error('ERROR: number of timesteps in Rail Demand data is less than 8760...')
    else
        % is ok to proceed
    end
    
    % convert CH elec demand to MWh and round to nearest MWh
    data_railload_base_CHsum_fullyr = round(data_railload_base_CHsum_fullyr*1000*1000,0);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    
    % hourly
    CalliopeToNexuse.RailElecDemand_hrly.CH 	= data_railload_base_CHsum_fullyr;
    % annual
    CalliopeToNexuse.RailElecDemand_yrly.CH 	= sum(data_railload_base_CHsum_fullyr);
    % units
    CalliopeToNexuse.Units.RailElecDemand = ('MWh');
    
    disp(' ')
    disp(['The total processing time for the rail demand profiles is: ', num2str(toc(strRailLoad)), ' (s) '])
    disp('=========================================================================')
    %}
    
    %% Version for PATHFNDR Scenarios v2
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: flow_in.csv
    %   -base electricity demands for each country, no rail (hourly)
    %   -rail electricity demands for each country (hourly)
    %   -electrified hydrogen demands for each country (hourly)
    %   -electrified mobility demands for each country (hourly)
    %   -electrified heatpump demands for each country (hourly)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are hourly (8760 entries)
    % the values in these profiles are NOT doubled
    % these profiles depend on the scenario
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the electric demand profiles begins... ')
    % Initilize
    strElecLoad = tic;
    
    % detect which entry in table_data
    idx_table_flowin = find(strcmp(table_names,'flow_in'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    %z1 = table_data{29}(strcmp(table_data{29}.techs,'demand_elec') & strcmp(table_data{29}.locs,'CHE'));
    
    % identify data for desired scenario
    idx_scenario1 = strcmp(table_data{idx_table_flowin}.scenario,scen_opt_1_name);
    %idx_scenario1 = strcmp(table_data{idx_table_flowin}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_flowin}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_flowin}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_flowin}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_flowin}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for Base demand
    techs_base_demand = {'demand_elec','electric_heater','electric_hob','dac','flexibility_electricity'};
    idx_elecload_base = ismember(table_data{idx_table_flowin}.techs,techs_base_demand);
    %idx_elecload_base1 = strcmp(table_data{idx_table_flowin}.techs,'demand_elec');
    %idx_elecload_base2 = strcmp(table_data{idx_table_flowin}.techs,'electric_heater');
    %idx_elecload_base3 = strcmp(table_data{idx_table_flowin}.techs,'electric_hob');
    %idx_elecload_base4 = strcmp(table_data{idx_table_flowin}.techs,'dac');
    % identify data for hydrogen demand
    techs_hydrogen_demand = {'electrolysis','hydrogen_to_liquids'};
    idx_elecload_hydrogen = ismember(table_data{idx_table_flowin}.techs,techs_hydrogen_demand);
    %idx_elecload_hydrogen1 = strcmp(table_data{idx_table_flowin}.techs,'electrolysis');
    %idx_elecload_hydrogen2 = strcmp(table_data{idx_table_flowin}.techs,'hydrogen_to_liquids');
    % identify data for e-mobility demand
    techs_emobility_demand = {'heavy_transport_ev','light_transport_ev'};
    idx_elecload_emobility = ismember(table_data{idx_table_flowin}.techs,techs_emobility_demand);
    %idx_elecload_emobility1 = strcmp(table_data{idx_table_flowin}.techs,'heavy_transport_ev');
    %idx_elecload_emobility2 = strcmp(table_data{idx_table_flowin}.techs,'light_transport_ev');
    % identify data for heatpump demand
    techs_heatpump_demand = {'hp'};
    idx_elecload_heatpump = ismember(table_data{idx_table_flowin}.techs,techs_heatpump_demand);
    %idx_elecload_heatpump = strcmp(table_data{idx_table_flowin}.techs,'hp');
    techs_rail_demand = {'demand_rail'};
    idx_elecload_rail = ismember(table_data{idx_table_flowin}.techs,techs_rail_demand);
    
    % identify data for neighboring each country
    idx_austria1 = strcmp(table_data{idx_table_flowin}.locs,'AUT');
    idx_germany1 = strcmp(table_data{idx_table_flowin}.locs,'DEU');
    idx_france1 = strcmp(table_data{idx_table_flowin}.locs,'FRA');
    idx_italy1 = strcmp(table_data{idx_table_flowin}.locs,'ITA');
    
    % identify data for any CH region (if only one region)
    idx_swiss1 = strcmp(table_data{idx_table_flowin}.locs,'CHE');
    % identify data for any CH region (if multiple regions)
    %idx_swiss1 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_flowin}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % get timesteps
    %--------------------------------------------------------------------------
    
    % get unique timesteps (will sort by smallest)
    timesteps_num = unique(table_data{idx_table_flowin}.timesteps);
    timesteps_vec = datevec(timesteps_num);
    % test if number of hours is 8760, edit or stop if needed
    if length(timesteps_num) > 8760
        % more than expected time steps...just ignore the last one to keep
        % only 8760
        timesteps_num = timesteps_num(1:8760);
        timesteps_vec = datevec(timesteps_num);
    elseif length(timesteps_num) < 8760
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        %disp('ERROR: number of timesteps in Demand data is less than 8760...')
        error('ERROR: number of timesteps in Demand data is less than 8760...')
    else
        % is ok to proceed
    end
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all CH
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allCH_hrly_TWh, data_elecload_hydrogen_Scen1_allCH_hrly_TWh, data_elecload_emobility_Scen1_allCH_hrly_TWh, data_elecload_heatpump_Scen1_allCH_hrly_TWh, data_elecload_rail_Scen1_allCH_hrly_TWh] = Func_GetCalliopeDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all DE
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allDE_hrly_TWh, data_elecload_hydrogen_Scen1_allDE_hrly_TWh, data_elecload_emobility_Scen1_allDE_hrly_TWh, data_elecload_heatpump_Scen1_allDE_hrly_TWh, data_elecload_rail_Scen1_allDE_hrly_TWh] = Func_GetCalliopeDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_germany1,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all FR
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allFR_hrly_TWh, data_elecload_hydrogen_Scen1_allFR_hrly_TWh, data_elecload_emobility_Scen1_allFR_hrly_TWh, data_elecload_heatpump_Scen1_allFR_hrly_TWh, data_elecload_rail_Scen1_allFR_hrly_TWh] = Func_GetCalliopeDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_france1,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all IT
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allIT_hrly_TWh, data_elecload_hydrogen_Scen1_allIT_hrly_TWh, data_elecload_emobility_Scen1_allIT_hrly_TWh, data_elecload_heatpump_Scen1_allIT_hrly_TWh, data_elecload_rail_Scen1_allIT_hrly_TWh] = Func_GetCalliopeDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_italy1,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all AT
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allAT_hrly_TWh, data_elecload_hydrogen_Scen1_allAT_hrly_TWh, data_elecload_emobility_Scen1_allAT_hrly_TWh, data_elecload_heatpump_Scen1_allAT_hrly_TWh, data_elecload_rail_Scen1_allAT_hrly_TWh] = Func_GetCalliopeDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_austria1,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail);
    
    
    
    %--------------------------------------------------------------------------
    % CH,DE,FR,IT,AT convert units to MWh and round to nearest MWh
    %--------------------------------------------------------------------------
    % CH
    [data_elecload_base_Scen1_CHsum_MWh, data_elecload_hydrogen_Scen1_CHsum_MWh, data_elecload_emobility_Scen1_CHsum_MWh, data_elecload_heatpump_Scen1_CHsum_MWh, data_elecload_rail_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allCH_hrly_TWh, data_elecload_hydrogen_Scen1_allCH_hrly_TWh, data_elecload_emobility_Scen1_allCH_hrly_TWh, data_elecload_heatpump_Scen1_allCH_hrly_TWh, data_elecload_rail_Scen1_allCH_hrly_TWh);
    % DE
    [data_elecload_base_Scen1_DEsum_MWh, data_elecload_hydrogen_Scen1_DEsum_MWh, data_elecload_emobility_Scen1_DEsum_MWh, data_elecload_heatpump_Scen1_DEsum_MWh, data_elecload_rail_Scen1_DEsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allDE_hrly_TWh, data_elecload_hydrogen_Scen1_allDE_hrly_TWh, data_elecload_emobility_Scen1_allDE_hrly_TWh, data_elecload_heatpump_Scen1_allDE_hrly_TWh, data_elecload_rail_Scen1_allDE_hrly_TWh);
    % FR
    [data_elecload_base_Scen1_FRsum_MWh, data_elecload_hydrogen_Scen1_FRsum_MWh, data_elecload_emobility_Scen1_FRsum_MWh, data_elecload_heatpump_Scen1_FRsum_MWh, data_elecload_rail_Scen1_FRsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allFR_hrly_TWh, data_elecload_hydrogen_Scen1_allFR_hrly_TWh, data_elecload_emobility_Scen1_allFR_hrly_TWh, data_elecload_heatpump_Scen1_allFR_hrly_TWh, data_elecload_rail_Scen1_allFR_hrly_TWh);
    % IT
    [data_elecload_base_Scen1_ITsum_MWh, data_elecload_hydrogen_Scen1_ITsum_MWh, data_elecload_emobility_Scen1_ITsum_MWh, data_elecload_heatpump_Scen1_ITsum_MWh, data_elecload_rail_Scen1_ITsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allIT_hrly_TWh, data_elecload_hydrogen_Scen1_allIT_hrly_TWh, data_elecload_emobility_Scen1_allIT_hrly_TWh, data_elecload_heatpump_Scen1_allIT_hrly_TWh, data_elecload_rail_Scen1_allIT_hrly_TWh);
    % AT
    [data_elecload_base_Scen1_ATsum_MWh, data_elecload_hydrogen_Scen1_ATsum_MWh, data_elecload_emobility_Scen1_ATsum_MWh, data_elecload_heatpump_Scen1_ATsum_MWh, data_elecload_rail_Scen1_ATsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allAT_hrly_TWh, data_elecload_hydrogen_Scen1_allAT_hrly_TWh, data_elecload_emobility_Scen1_allAT_hrly_TWh, data_elecload_heatpump_Scen1_allAT_hrly_TWh, data_elecload_rail_Scen1_allAT_hrly_TWh);
    
    
    %--------------------------------------------------------------------------
    % create different e-mobility profile using EXPANSE normalized profile
    %--------------------------------------------------------------------------
    
    % use the annual total for each country and the Expanse normalized profile
    % create a new version of the e-mobility demand profile
    % also round to the nearest MWh
    data_elecload_emobility_Scen1_CHsum_MWh_EXP = round(sum(data_elecload_emobility_Scen1_CHsum_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_DEsum_MWh_EXP = round(sum(data_elecload_emobility_Scen1_DEsum_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_FRsum_MWh_EXP = round(sum(data_elecload_emobility_Scen1_FRsum_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_ITsum_MWh_EXP = round(sum(data_elecload_emobility_Scen1_ITsum_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_ATsum_MWh_EXP = round(sum(data_elecload_emobility_Scen1_ATsum_MWh)*Expanse_emobility_normalized,0);
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Base Demand)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.BaseElecDemand_hrly.CH 	= data_elecload_base_Scen1_CHsum_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.DE 	= data_elecload_base_Scen1_DEsum_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.FR 	= data_elecload_base_Scen1_FRsum_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.IT 	= data_elecload_base_Scen1_ITsum_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.AT 	= data_elecload_base_Scen1_ATsum_MWh;
    %CalliopeToNexuse.BaseElecDemand_hrly.EU 	= data_elecload_base_Scen1_EUsum_fullyr;    % this was from CH2040 datafiles    
    % annual
    CalliopeToNexuse.BaseElecDemand_yrly.CH 	= sum(data_elecload_base_Scen1_CHsum_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.DE 	= sum(data_elecload_base_Scen1_DEsum_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.FR 	= sum(data_elecload_base_Scen1_FRsum_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.IT 	= sum(data_elecload_base_Scen1_ITsum_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.AT 	= sum(data_elecload_base_Scen1_ATsum_MWh);
    %CalliopeToNexuse.BaseElecDemand_yrly.EU 	= sum(data_elecload_base_Scen1_EUsum_fullyr);    % this was from CH2040 datafiles
    % units
    CalliopeToNexuse.Units.BaseElecDemand = ('MWh');
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Rail Demand)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.RailElecDemand_hrly.CH 	= data_elecload_rail_Scen1_CHsum_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.DE 	= data_elecload_rail_Scen1_DEsum_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.FR 	= data_elecload_rail_Scen1_FRsum_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.IT 	= data_elecload_rail_Scen1_ITsum_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.AT 	= data_elecload_rail_Scen1_ATsum_MWh;    
    % annual
    CalliopeToNexuse.RailElecDemand_yrly.CH 	= sum(data_elecload_rail_Scen1_CHsum_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.DE 	= sum(data_elecload_rail_Scen1_DEsum_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.FR 	= sum(data_elecload_rail_Scen1_FRsum_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.IT 	= sum(data_elecload_rail_Scen1_ITsum_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.AT 	= sum(data_elecload_rail_Scen1_ATsum_MWh);
    % units
    CalliopeToNexuse.Units.RailElecDemand = ('MWh');
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Electrified Demands)
    %--------------------------------------------------------------------------
    % units
    CalliopeToNexuse.Units.ElectrifiedDemands = ('MWh');
    
    % next save the electrified demand profiles (hourly)
    % CH
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen             = data_elecload_hydrogen_Scen1_CHsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Calliope 	 = data_elecload_emobility_Scen1_CHsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Expanse	 = data_elecload_emobility_Scen1_CHsum_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump             = data_elecload_heatpump_Scen1_CHsum_MWh;
    % DE
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_hydrogen             = data_elecload_hydrogen_Scen1_DEsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_Calliope 	 = data_elecload_emobility_Scen1_DEsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_Expanse    = data_elecload_emobility_Scen1_DEsum_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_heatpump             = data_elecload_heatpump_Scen1_DEsum_MWh;
    % FR
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_hydrogen             = data_elecload_hydrogen_Scen1_FRsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_Calliope   = data_elecload_emobility_Scen1_FRsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_Expanse    = data_elecload_emobility_Scen1_FRsum_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_heatpump             = data_elecload_heatpump_Scen1_FRsum_MWh;
    % IT
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_hydrogen             = data_elecload_hydrogen_Scen1_ITsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_Calliope 	 = data_elecload_emobility_Scen1_ITsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_Expanse    = data_elecload_emobility_Scen1_ITsum_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_heatpump             = data_elecload_heatpump_Scen1_ITsum_MWh;
    % AT
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_hydrogen             = data_elecload_hydrogen_Scen1_ATsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_Calliope 	 = data_elecload_emobility_Scen1_ATsum_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_Expanse    = data_elecload_emobility_Scen1_ATsum_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_heatpump             = data_elecload_heatpump_Scen1_ATsum_MWh;
    
    % next save the electrified demand profiles (annual)
    % CH
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_hydrogen          	= sum(data_elecload_hydrogen_Scen1_CHsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_Calliope	= sum(data_elecload_emobility_Scen1_CHsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_Expanse	= sum(data_elecload_emobility_Scen1_CHsum_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_heatpump           	= sum(data_elecload_heatpump_Scen1_CHsum_MWh);
    % DE
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_hydrogen          	= sum(data_elecload_hydrogen_Scen1_DEsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_Calliope 	= sum(data_elecload_emobility_Scen1_DEsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_Expanse  	= sum(data_elecload_emobility_Scen1_DEsum_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_heatpump        	= sum(data_elecload_heatpump_Scen1_DEsum_MWh);
    % FR
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_hydrogen          	= sum(data_elecload_hydrogen_Scen1_FRsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_Calliope	= sum(data_elecload_emobility_Scen1_FRsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_Expanse 	= sum(data_elecload_emobility_Scen1_FRsum_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_heatpump        	= sum(data_elecload_heatpump_Scen1_FRsum_MWh);
    % IT
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_hydrogen         	= sum(data_elecload_hydrogen_Scen1_ITsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_Calliope 	= sum(data_elecload_emobility_Scen1_ITsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_Expanse	= sum(data_elecload_emobility_Scen1_ITsum_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_heatpump        	= sum(data_elecload_heatpump_Scen1_ITsum_MWh);
    % AT
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_hydrogen            = sum(data_elecload_hydrogen_Scen1_ATsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_Calliope 	= sum(data_elecload_emobility_Scen1_ATsum_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_Expanse	= sum(data_elecload_emobility_Scen1_ATsum_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_heatpump         	= sum(data_elecload_heatpump_Scen1_ATsum_MWh);
    
    disp(' ')
    disp(['The total processing time for the electrified demand profiles is: ', num2str(toc(strElecLoad)), ' (s) '])
    disp('=========================================================================')
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: net_import.csv
    %   -list of country-to-country borders (exporting to importing)
    %   -power flows From/To each country (hourly)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are hourly (8760 entries)
    % the values in these profiles are NOT doubled
    % these profiles depend on the scenario
    
    % Note: each border connection is listed twice (once for each
    % direction) but the data are just an identical repeat with opposite
    % sign.  So, really only need to pay attention to one set of hourly
    % flows for each pair that represent a given border.
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the Cross-Border flow profiles begins... ')
    % Initilize
    strXBflows = tic;
    
    % detect which entry in table_data
    idx_table_impexp = find(strcmp(table_names,'net_import'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for electricity flows
    idx_elecflows = strcmp(table_data{idx_table_impexp}.carriers,'electricity');
    % identify data for desired scenario
    idx_scenario4 = strcmp(table_data{idx_table_impexp}.scenario,scen_opt_1_name);
    %idx_scenario4 = strcmp(table_data{idx_table_impexp}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_impexp}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_impexp}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_impexp}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_impexp}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify all entries not internal to CH
    idx_impexp_notCHinternal = ~( strcmp(cellfun(@(x) x(1:3),table_data{idx_table_impexp}.exporting_region,'un',0),'CHE') & strcmp(cellfun(@(x) x(1:3),table_data{idx_table_impexp}.importing_region,'un',0),'CHE'));
    
    % create identifiers for all nonCH borders for the desired scenario
    % that are electricity flows
    idx_impexp_Scen4_notCHinternal_elecflows = idx_scenario4 & idx_impexp_notCHinternal & idx_elecflows;
    
    % create a list of all border crossings
    %borders_list = unique(table_data{idx_table_impexp}(idx_impexp_Scen4_notCHinternal,6:7));
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all CH
    %--------------------------------------------------------------------------
    
    % get XB flows for all nonCH borders for the desired scenario that are
    % electricity flows
    tabledata_impexp_Scen4_notCHinternal_elecflows = table_data{idx_table_impexp}(idx_impexp_Scen4_notCHinternal_elecflows,:);
    
    % reset all CHE_X border to be just CHE so I can sum them up
    idx_exp_Scen4_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_impexp_Scen4_notCHinternal_elecflows.exporting_region,'un',0),'CHE');
    idx_imp_Scen4_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_impexp_Scen4_notCHinternal_elecflows.importing_region,'un',0),'CHE');
    tabledata_impexp_Scen4_notCHinternal_elecflows.exporting_region(idx_exp_Scen4_notCHinternal) = {'CHE'};
    tabledata_impexp_Scen4_notCHinternal_elecflows.importing_region(idx_imp_Scen4_notCHinternal) = {'CHE'};
    
    % also get a list of the unique border crossings
    borders_list = unique(tabledata_impexp_Scen4_notCHinternal_elecflows(:,2:3));
    
    % get unique timesteps (will sort by smallest)
    timesteps_num4 = unique(tabledata_impexp_Scen4_notCHinternal_elecflows.timesteps);
    timesteps_vec4 = datevec(timesteps_num4);
    % test if number of hours is 8760, edit or stop if needed
    if length(timesteps_num4) > 8760
        % more than expected time steps...just ignore the last one to keep
        % only 8760
        timesteps_num4 = timesteps_num4(1:8760);
        timesteps_vec4 = datevec(timesteps_num4);
    elseif length(timesteps_num4) < 8760
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        %disp('ERROR: number of timesteps in Demand data is less than 8760...')
        error('ERROR: number of timesteps in Net Import data is less than 8760...')
    else
        % is ok to proceed
    end
    
    %--------------------------------------------------------------------------
    % reduce list of borders to only those desired
    %--------------------------------------------------------------------------
    
    % define countries to keep
    countries_desired = {'CHE','DEU','FRA','ITA','AUT'};
    
    % FIRST reduce the borders_list and replace any non-desired country
    % with 'rest_of_europe'
    % identify any entries with exporting_region of desired countries
    idx_borderkeep_exp = ismember(borders_list.exporting_region,countries_desired);
    % identify any entries with importing_region of desired countries
    idx_borderkeep_imp = ismember(borders_list.importing_region,countries_desired);
    % combine index for any entry with export or import or both
    idx_borderkeep = or(idx_borderkeep_exp,idx_borderkeep_imp);
    % replace any non-desired countries with 'rest_of_europe'
    borders_list.exporting_region(~idx_borderkeep_exp) = {'rest_of_europe'};
    borders_list.importing_region(~idx_borderkeep_imp) = {'rest_of_europe'};
    % reduce the borders list, also eliminates the 'rest_of_europe' to 'rest_of_europe' border
    borders_list(~idx_borderkeep,:) = [];
    % only keep the unique entries (no need to list more than once)
    borders_list = unique(borders_list);
    % save for DBcreation
    CalliopeToNexuse.ImpExp_Borders_all = borders_list;
    
    % SECOND reduce the hourly net_import data and replace any non-desired country
    % with 'rest_of_europe'
    % identify any entries with exporting_region of desired countries
    idx_borderdatakeep_exp = ismember(tabledata_impexp_Scen4_notCHinternal_elecflows.exporting_region,countries_desired);
    % identify any entries with importing_region of desired countries
    idx_borderdatakeep_imp = ismember(tabledata_impexp_Scen4_notCHinternal_elecflows.importing_region,countries_desired);
    % combine index for any entry with export or import or both
    idx_borderdatakeep = or(idx_borderdatakeep_exp,idx_borderdatakeep_imp);
    % replace any non-desired countries with 'rest_of_europe'
    tabledata_impexp_Scen4_notCHinternal_elecflows.exporting_region(~idx_borderdatakeep_exp) = {'rest_of_europe'};
    tabledata_impexp_Scen4_notCHinternal_elecflows.importing_region(~idx_borderdatakeep_imp) = {'rest_of_europe'};
    % reduce the borders list, also eliminates the 'rest_of_europe' to 'rest_of_europe' border
    tabledata_impexp_Scen4_notCHinternal_elecflows(~idx_borderdatakeep,:) = [];
    
    %--------------------------------------------------------------------------
    % create hourly profiles for each border
    %--------------------------------------------------------------------------
    
    % loop over each border
    for i4a = 1:size(borders_list,1)
        
        % identify all rows with this border
        idx_t_border = strcmp(tabledata_impexp_Scen4_notCHinternal_elecflows.exporting_region,borders_list.exporting_region(i4a)) & strcmp(tabledata_impexp_Scen4_notCHinternal_elecflows.importing_region,borders_list.importing_region(i4a));
        
        % loop over each timestep to add up all (if multiple entries for 
        % this border)
        for i4b = 1:length(timesteps_num4)
            
            % identify all rows with this timestep and this border
            idx_t_border_time = tabledata_impexp_Scen4_notCHinternal_elecflows.timesteps == timesteps_num4(i4b) & idx_t_border;
            
            % sum all entries for this timestep and this border
            data_impexp_Scen4_CHsum_fullyrTWh(i4b,i4a)    = sum(tabledata_impexp_Scen4_notCHinternal_elecflows.net_import(idx_t_border_time));
            
        end
        
        % NEW just convert to MWh and round to nearest MWh
        data_impexp_Scen4_CHsum_MWh(:,i4a) = Func_ConvertCalliopeProfile_TWh(data_impexp_Scen4_CHsum_fullyrTWh(:,i4a),0);
        
    end
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    
    % note that I make these profiles as rows so I can identify which From/To
    % country they are with the corresponding CalliopeToNexuse.ImpExp_Borders
    CalliopeToNexuse.ImpExp_profiles_all = data_impexp_Scen4_CHsum_MWh';
    
    %--------------------------------------------------------------------------
    % separate the Exp from DE,FR,IT,AT that is Imp to EU
    %--------------------------------------------------------------------------
    
    % separate only the exports from neighbors going as imports to rest_of_EU
    % this step is to identify the correct one of the duplicate entries in
    % the borders data
    % (already removed the EU -> EU entry)
    % Note: In the Nexus-e database creation I will add the export from one
    % of the Swiss neighbors to 'rest_of_europe' to that countries demand, 
    % so I want to keep the export profile (it will have proper + - signs) 
    % and not the import profile.
    % note this profile then included exports (as +) and imports (as -)
    idx_borders_ImpEU = strcmp(borders_list.importing_region,'rest_of_europe') & ~strcmp(borders_list.exporting_region,'rest_of_europe');
    
    % save list of these borders and profiles for DBcreation
    CalliopeToNexuse.ImpExp_Borders_need  = borders_list(idx_borders_ImpEU,:);
    CalliopeToNexuse.ImpExp_profiles_need = data_impexp_Scen4_CHsum_MWh(:,idx_borders_ImpEU)';
    
    %--------------------------------------------------------------------------
    % create annual sum of import and export for each border
    %--------------------------------------------------------------------------
    
    % create temporary copy of all Imp/Exp data
    temp1_data_impexp_Scen4_CHsum_fullyr = data_impexp_Scen4_CHsum_MWh;
    temp2_data_impexp_Scen4_CHsum_fullyr = data_impexp_Scen4_CHsum_MWh;
    % replace negitives/positives with 0
    temp1_data_impexp_Scen4_CHsum_fullyr(temp1_data_impexp_Scen4_CHsum_fullyr < 0) = 0;
    temp2_data_impexp_Scen4_CHsum_fullyr(temp2_data_impexp_Scen4_CHsum_fullyr > 0) = 0;
    % sum to get the total imports and total exports for each border
    data_impexp_Scen4_ImpTot_all = sum(temp1_data_impexp_Scen4_CHsum_fullyr,1)';    % only (+), for borders kept this will mean from CH-neighbor --> to EU (in direction as noted by column headings)
    data_impexp_Scen4_ExpTot_all = sum(temp2_data_impexp_Scen4_CHsum_fullyr,1)';    % only (-), for borders kept this will mean from EU --> to CH-neighbor (in opposite direction as column headings)
    % also get totals for the borders 'needed'
    data_impexp_Scen4_ImpTot_need = data_impexp_Scen4_ImpTot_all(idx_borders_ImpEU,:);
    data_impexp_Scen4_ExpTot_need = data_impexp_Scen4_ExpTot_all(idx_borders_ImpEU,:);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    CalliopeToNexuse.ImpExp_YrlyTot_all.ExpRegion2ImpRegion = data_impexp_Scen4_ImpTot_all; % only (+), in direction as noted by column headings
    CalliopeToNexuse.ImpExp_YrlyTot_all.ImpRegion2ExpRegion = data_impexp_Scen4_ExpTot_all; % only (-), in opposite direction as column headings
    CalliopeToNexuse.ImpExp_YrlyTot_need.XX2EU = data_impexp_Scen4_ImpTot_need;
    CalliopeToNexuse.ImpExp_YrlyTot_need.EU2XX = data_impexp_Scen4_ExpTot_need;
    % units
    CalliopeToNexuse.Units.ImpExp = ('MWh');
    
    disp(' ')
    disp(['The total processing time for the Cross-Border flow profiles is: ', num2str(toc(strXBflows)), ' (s) '])
    disp('=========================================================================')
    
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize non-CH generator capacities (nameplate_capacity.csv)
    %   -list of electric generator types
    %   -installed capacities by gen type for each country
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values depend on the scenario
    % any capacity value < 5 is ignored (set to 0)
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the generator capacities begins... ')
    % Initilize
    strGenCap = tic;
    
    % detect which entry in table_data
    idx_table_GenCapacities = find(strcmp(table_names,'nameplate_capacity'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario3 = strcmp(table_data{idx_table_GenCapacities}.scenario,scen_opt_1_name);
    %idx_scenario3 = strcmp(table_data{idx_table_GenCapacities}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_GenCapacities}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_GenCapacities}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_GenCapacities}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_GenCapacities}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for electricity production
    idx_elecgen = strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity');
    % ignore some tech types ('biofuel_to_liquids' is not in New data files)
    techs_ignore_list = {'dac','electric_hob','biofuel_to_liquids'};
    idx_elecgen(ismember(table_data{idx_table_GenCapacities}.techs,techs_ignore_list)) = false;
    
    % identify data for neighboring each country
    idx_austria3 = strcmp(table_data{idx_table_GenCapacities}.locs,'AUT');
    idx_germany3 = strcmp(table_data{idx_table_GenCapacities}.locs,'DEU');
    idx_france3 = strcmp(table_data{idx_table_GenCapacities}.locs,'FRA');
    idx_italy3 = strcmp(table_data{idx_table_GenCapacities}.locs,'ITA');
    %idx_restofEU3 = strcmp(table_data{idx_table_GenCapacities}.locs,'rest_of_europe');
    
    % identify data for any CH region
    idx_swiss3 = strcmp(table_data{idx_table_GenCapacities}.locs,'CHE');
    %idx_swiss_AllAsSeparate3 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_GenCapacities}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % get electricity generator types
    %--------------------------------------------------------------------------
    
    % separate only electricity generators
    tabledata_elecgens_Scen1 = table_data{idx_table_GenCapacities}(idx_elecgen,:);
    % get list of techs for electricity generators
    techs_elecgens = unique(tabledata_elecgens_Scen1.techs,'stable');
    % save this list for DB creation
    CalliopeToNexuse.techs_list_elec = techs_elecgens;
    
    %--------------------------------------------------------------------------
    % separate electricity Gen Capacities for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for each country's generators
    idx_elecgens_Scen1_CH   = idx_elecgen & idx_scenario3 & idx_swiss3;
    idx_elecgens_Scen1_DE   = idx_elecgen & idx_scenario3 & idx_germany3;
    idx_elecgens_Scen1_FR   = idx_elecgen & idx_scenario3 & idx_france3;
    idx_elecgens_Scen1_IT   = idx_elecgen & idx_scenario3 & idx_italy3;
    idx_elecgens_Scen1_AT   = idx_elecgen & idx_scenario3 & idx_austria3;
    %idx_elecgens_Scen1_EU   = idx_elecgen & idx_scenario3 & idx_restofEU3;
    
    % get entries for each country in given scenario
    tabledata_elecgens_Scen1_CH = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_CH,2:6);
    tabledata_elecgens_Scen1_DE = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_DE,2:6);
    tabledata_elecgens_Scen1_FR = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_FR,2:6);
    tabledata_elecgens_Scen1_IT = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_IT,2:6);
    tabledata_elecgens_Scen1_AT = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_AT,2:6);
    %tabledata_elecgens_Scen1_EU = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_EU,6:10);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_elecgens_Scen1_CH.nameplate_capacity = round(tabledata_elecgens_Scen1_CH.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_DE.nameplate_capacity = round(tabledata_elecgens_Scen1_DE.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_FR.nameplate_capacity = round(tabledata_elecgens_Scen1_FR.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_IT.nameplate_capacity = round(tabledata_elecgens_Scen1_IT.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_AT.nameplate_capacity = round(tabledata_elecgens_Scen1_AT.nameplate_capacity*1000*1000,1);
    %tabledata_elecgens_Scen1_EU.nameplate_capacity = round(tabledata_elecgens_Scen1_EU.nameplate_capacity*1000*1000,1);
    % also modify 'unit' column
    tabledata_elecgens_Scen1_CH.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_CH,1),1);
    tabledata_elecgens_Scen1_DE.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_DE,1),1);
    tabledata_elecgens_Scen1_FR.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_FR,1),1);
    tabledata_elecgens_Scen1_IT.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_IT,1),1);
    tabledata_elecgens_Scen1_AT.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_AT,1),1);
    %tabledata_elecgens_Scen1_EU.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_EU,1),1);
    
    %--------------------------------------------------------------------------
    % set any capacities < 5 MW to = 0 (eliminate really small capacities)
    %--------------------------------------------------------------------------
    tabledata_elecgens_Scen1_DE.nameplate_capacity( tabledata_elecgens_Scen1_DE.nameplate_capacity < 5 ) = 0;
    tabledata_elecgens_Scen1_FR.nameplate_capacity( tabledata_elecgens_Scen1_FR.nameplate_capacity < 5 ) = 0;
    tabledata_elecgens_Scen1_IT.nameplate_capacity( tabledata_elecgens_Scen1_IT.nameplate_capacity < 5 ) = 0;
    tabledata_elecgens_Scen1_AT.nameplate_capacity( tabledata_elecgens_Scen1_AT.nameplate_capacity < 5 ) = 0;
    tabledata_elecgens_Scen1_CH.nameplate_capacity( tabledata_elecgens_Scen1_CH.nameplate_capacity < 5 ) = 0;
    
    %--------------------------------------------------------------------------
    % separate each country by tech type - quick check for if capacities change
    %--------------------------------------------------------------------------
    
    tabledata_elecgens_ScenAll_DE = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'DEU') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_DE_sort = sortrows(tabledata_elecgens_ScenAll_DE,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_FR = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'FRA') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_FR_sort = sortrows(tabledata_elecgens_ScenAll_FR,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_IT = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'ITA') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_IT_sort = sortrows(tabledata_elecgens_ScenAll_IT,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_AU = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'AUT') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_AU_sort = sortrows(tabledata_elecgens_ScenAll_AU,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    
    % all non-Swiss regions
    CalliopeToNexuse.GenTypeParams.DE = tabledata_elecgens_Scen1_DE;
    CalliopeToNexuse.GenTypeParams.FR = tabledata_elecgens_Scen1_FR;
    CalliopeToNexuse.GenTypeParams.IT = tabledata_elecgens_Scen1_IT;
    CalliopeToNexuse.GenTypeParams.AT = tabledata_elecgens_Scen1_AT;
    %CalliopeToNexuse.GenTypeParams.EU = tabledata_elecgens_Scen1_EU;
    % also save Swiss regions
    CalliopeToNexuse.GenTypeParams.CH = tabledata_elecgens_Scen1_CH;
    %CalliopeToNexuse.GenCapacities_CH_canton  = tabledata_elecgens_Scen1_CH;
    % units
    CalliopeToNexuse.Units.GenCapacities = ('MW');
    
    disp(' ')
    disp(['The total processing time for the generator capacities is: ', num2str(toc(strGenCap)), ' (s) '])
    disp('=========================================================================')
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize Investment costs  (cost_per_nameplate_capacity.csv)
    %   -Investment costs by gen type for CH
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are independent of scenario
    % assumes each GenType has same Invstement cost across all CH regions
    % these Investment costs are NOT annualized
    %{
    disp(' ')
    disp('=========================================================================')
    disp('Processing the generator investment costs begins... ')
    % Initilize
    strGenInvCost = tic;
    
    % detect which entry in table_data
    idx_table_GenInvCost = find(strcmp(table_names,'cost_per_nameplate_capacity'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate5 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_GenInvCost}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % separate Investment costs for all CH
    %--------------------------------------------------------------------------
    % assumes each GenType has the same cost across all CH regions
    
    % create identifiers for all CH regions generators that have listed elec
    % capacities for CH
    idx_GenInvCost_Scen1_CHsep   = idx_swiss_AllAsSeparate5 & ismember(table_data{idx_table_GenInvCost}.techs,tabledata_elecgens_Scen1_CHsum.techs);
    
    % get entries for all these CH generators
    tabledata_GenInvCost_Scen1_CHsep = table_data{idx_table_GenInvCost}(idx_GenInvCost_Scen1_CHsep,:);
    
    % get unique techs (will stay in order)
    techs_CH3 = unique(tabledata_GenInvCost_Scen1_CHsep.techs,'stable');
    
    % loop over each tech to find first entry Invstement cost
    for i6 = 1:length(techs_CH3)
        
        % identify all rows with this tech
        idx_tech_GenInvCost_Scen1_CHsep     = find(strcmp(tabledata_GenInvCost_Scen1_CHsep.techs, techs_CH3(i6)),1,'first');
        
        % sum all entries for these timesteps
        data_GenInvCost_Scen1_CHall(i6,1)    = tabledata_GenInvCost_Scen1_CHsep.cost_per_nameplate_capacity(idx_tech_GenInvCost_Scen1_CHsep);
        
    end
    
    % create tabledata for all CH Gen Invstement cost
    tabledata_GenInvCost_Scen1_CHall = tabledata_GenInvCost_Scen1_CHsep(1:length(techs_CH3),:);	% initialize with correct length
    tabledata_GenInvCost_Scen1_CHall.techs = techs_CH3;                                         % replace list of techs
    tabledata_GenInvCost_Scen1_CHall.locs = repmat({'CHE'},length(techs_CH3),1);                % replace locs
    tabledata_GenInvCost_Scen1_CHall.cost_per_nameplate_capacity = data_GenInvCost_Scen1_CHall;	% replace Invstement costs
    
    %--------------------------------------------------------------------------
    % add Investment cost data to capacities data
    %--------------------------------------------------------------------------
    % convert units to EUR/MW (from billion EUR/TW) and round to nearest 0.01
    % (entries not provided are assumed to have InvCost=0)
    
    tabledata_elecgens_Scen1_CHsum = Func_AddToDataByTechType(tabledata_elecgens_Scen1_CHsum,tabledata_GenInvCost_Scen1_CHall,'cost_per_nameplate_capacity',{'unit2','cost_per_nameplate_capacity'},'2015eur_per_mw');
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % save Swiss regions (replace anything entered previously)
    CalliopeToNexuse.GenTypeParams.CH_country = tabledata_elecgens_Scen1_CHsum;
    % units
    CalliopeToNexuse.Units.InvestCost = ('2015EUR_per_MW');
    
    disp(' ')
    disp(['The total processing time for the generator investment costs is: ', num2str(toc(strGenInvCost)), ' (s) '])
    disp('=========================================================================')
    %}
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize VOM costs  (cost_per_flow_out.csv)
    %   -VOM costs by gen type for each country (dont actually need CH since we
    %    use our own existing and candidate capacities)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are independent of scenario
    % assumes each GenType has same VOM cost across all CH regions
    %{
    disp(' ')
    disp('=========================================================================')
    disp('Processing the generator VOM costs begins... ')
    % Initilize
    strGenVOM = tic;
    
    % detect which entry in table_data
    idx_table_GenVOM = find(strcmp(table_names,'cost_per_flow_out'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for neighboring each country
    idx_austria4 = strcmp(table_data{idx_table_GenVOM}.locs,'AUT');
    idx_germany4 = strcmp(table_data{idx_table_GenVOM}.locs,'DEU');
    idx_france4 = strcmp(table_data{idx_table_GenVOM}.locs,'FRA');
    idx_italy4 = strcmp(table_data{idx_table_GenVOM}.locs,'ITA');
    idx_restofEU4 = strcmp(table_data{idx_table_GenVOM}.locs,'rest_of_europe');
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate4 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_GenVOM}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % separate electricity Gen Capacities for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for generators that have listed elec capacities for
    % each country
    idx_GenVOM_Scen1_DE   = idx_germany4  & ismember(table_data{idx_table_GenVOM}.techs,tabledata_elecgens_Scen1_DE.techs);
    idx_GenVOM_Scen1_FR   = idx_france4   & ismember(table_data{idx_table_GenVOM}.techs,tabledata_elecgens_Scen1_FR.techs);
    idx_GenVOM_Scen1_IT   = idx_italy4    & ismember(table_data{idx_table_GenVOM}.techs,tabledata_elecgens_Scen1_IT.techs);
    idx_GenVOM_Scen1_AT   = idx_austria4  & ismember(table_data{idx_table_GenVOM}.techs,tabledata_elecgens_Scen1_AT.techs);
    idx_GenVOM_Scen1_EU   = idx_restofEU4 & ismember(table_data{idx_table_GenVOM}.techs,tabledata_elecgens_Scen1_EU.techs);
    
    % get entries for each country in given scenario
    tabledata_GenVOM_Scen1_DE = table_data{idx_table_GenVOM}(idx_GenVOM_Scen1_DE,:);
    tabledata_GenVOM_Scen1_FR = table_data{idx_table_GenVOM}(idx_GenVOM_Scen1_FR,:);
    tabledata_GenVOM_Scen1_IT = table_data{idx_table_GenVOM}(idx_GenVOM_Scen1_IT,:);
    tabledata_GenVOM_Scen1_AT = table_data{idx_table_GenVOM}(idx_GenVOM_Scen1_AT,:);
    tabledata_GenVOM_Scen1_EU = table_data{idx_table_GenVOM}(idx_GenVOM_Scen1_EU,:);
    
    %--------------------------------------------------------------------------
    % add VOM cost data to capacities data
    %--------------------------------------------------------------------------
    % convert units to EUR/MWh (from billion EUR/TWh) and round to nearest 0.01
    % (entries not provided are assumed to have VOM=0)
    
    % % DE (before I created the function for this
    % % get order of tech types for putting costs into gen capacities
    % [C,order_techs_DE1,ib] = intersect(tabledata_elecgens_Scen1_DE.techs,tabledata_GenVOM_Scen1_DE.techs,'stable');
    % % add columns to DE for VOM costs - initialize cost to 0
    % tabledata_elecgens_Scen1_DE = addvars(tabledata_elecgens_Scen1_DE,repmat('2015eur_per_mwh',size(tabledata_elecgens_Scen1_DE,1),1),zeros(size(tabledata_elecgens_Scen1_DE,1),1),'NewVariableNames',{'unit2','cost_per_flow_out'});
    % % replace VOM costs from VOM data for all entries provided, convert units
    % % to EUR/MWh (from billion EUR/TWh) and round to nearest 0.01
    % % (entries not provided are assumed to have VOM=0)
    % tabledata_elecgens_Scen1_DE.cost_per_flow_out(order_techs_DE1) = round(tabledata_GenVOM_Scen1_DE.cost_per_flow_out*1000,2);
    
    % use FUNCTION
    % DE
    tabledata_elecgens_Scen1_DE = Func_AddToDataByTechType(tabledata_elecgens_Scen1_DE,tabledata_GenVOM_Scen1_DE,'cost_per_flow_out',{'unit2','cost_per_flow_out'},'2015eur_per_mwh');
    % FR
    tabledata_elecgens_Scen1_FR = Func_AddToDataByTechType(tabledata_elecgens_Scen1_FR,tabledata_GenVOM_Scen1_FR,'cost_per_flow_out',{'unit2','cost_per_flow_out'},'2015eur_per_mwh');
    % IT
    tabledata_elecgens_Scen1_IT = Func_AddToDataByTechType(tabledata_elecgens_Scen1_IT,tabledata_GenVOM_Scen1_IT,'cost_per_flow_out',{'unit2','cost_per_flow_out'},'2015eur_per_mwh');
    % AT
    tabledata_elecgens_Scen1_AT = Func_AddToDataByTechType(tabledata_elecgens_Scen1_AT,tabledata_GenVOM_Scen1_AT,'cost_per_flow_out',{'unit2','cost_per_flow_out'},'2015eur_per_mwh');
    % EU
    tabledata_elecgens_Scen1_EU = Func_AddToDataByTechType(tabledata_elecgens_Scen1_EU,tabledata_GenVOM_Scen1_EU,'cost_per_flow_out',{'unit2','cost_per_flow_out'},'2015eur_per_mwh');
    
    %--------------------------------------------------------------------------
    % separate VOM costs for all CH
    %--------------------------------------------------------------------------
    % assumes each GenType has the same cost across all CH regions
    
    % create identifiers for all CH regions generators that have listed elec
    % capacities for CH
    idx_GenVOM_Scen1_CHsep   = idx_swiss_AllAsSeparate4 & ismember(table_data{idx_table_GenVOM}.techs,tabledata_elecgens_Scen1_CHsum.techs);
    
    % get entries for all these CH generators
    tabledata_GenVOM_Scen1_CHsep = table_data{idx_table_GenVOM}(idx_GenVOM_Scen1_CHsep,:);
    
    % get unique techs (will stay in order)
    techs_CH2 = unique(tabledata_GenVOM_Scen1_CHsep.techs,'stable');
    
    % loop over each tech to find first entry VOM cost
    for i6 = 1:length(techs_CH2)
        
        % identify all rows with this tech
        idx_tech_GenVOM_Scen1_CHsep     = find(strcmp(tabledata_GenVOM_Scen1_CHsep.techs, techs_CH2(i6)),1,'first');
        
        % sum all entries for these timesteps
        data_GenVOM_Scen1_CHall(i6,1)    = tabledata_GenVOM_Scen1_CHsep.cost_per_flow_out(idx_tech_GenVOM_Scen1_CHsep);
        
    end
    
    % create tabledata for all CH GenVOM
    tabledata_GenVOM_Scen1_CHall = tabledata_GenVOM_Scen1_CHsep(1:length(techs_CH2),:);     % initialize with correct length
    tabledata_GenVOM_Scen1_CHall.techs = techs_CH2;                                         % replace list of techs
    tabledata_GenVOM_Scen1_CHall.locs = repmat({'CHE'},length(techs_CH2),1);                % replace locs
    tabledata_GenVOM_Scen1_CHall.cost_per_flow_out = data_GenVOM_Scen1_CHall;               % replace VOM costs
    
    %--------------------------------------------------------------------------
    % add VOM cost data to capacities data
    %--------------------------------------------------------------------------
    % convert units to EUR/MWh (from billion EUR/TWh) and round to nearest 0.01
    % (entries not provided are assumed to have VOM=0)
    
    tabledata_elecgens_Scen1_CHsum = Func_AddToDataByTechType(tabledata_elecgens_Scen1_CHsum,tabledata_GenVOM_Scen1_CHall,'cost_per_flow_out',{'unit3','cost_per_flow_out'},'2015eur_per_mwh');
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % all non-Swiss regions (replace anything entered previously)
    CalliopeToNexuse.GenTypeParams.DE = tabledata_elecgens_Scen1_DE;
    CalliopeToNexuse.GenTypeParams.FR = tabledata_elecgens_Scen1_FR;
    CalliopeToNexuse.GenTypeParams.IT = tabledata_elecgens_Scen1_IT;
    CalliopeToNexuse.GenTypeParams.AT = tabledata_elecgens_Scen1_AT;
    CalliopeToNexuse.GenTypeParams.EU = tabledata_elecgens_Scen1_EU;
    % also save Swiss regions (replace anything entered previously)
    CalliopeToNexuse.GenTypeParams.CH_country = tabledata_elecgens_Scen1_CHsum;
    % units
    CalliopeToNexuse.Units.VOMcost = ('2015EUR_per_MWh');
    
    disp(' ')
    disp(['The total processing time for the generator VOM costs is: ', num2str(toc(strGenVOM)), ' (s) '])
    disp('=========================================================================')
    %}
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize FOM costs  (annual_cost_per_nameplate_capacity.csv)
    %   -FOM costs by gen type for CH
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are independent of scenario
    % assumes each GenType has same FOM cost across all CH regions
    % these FOM costs ARE annualized
    %{
    disp(' ')
    disp('=========================================================================')
    disp('Processing the generator FOM costs begins... ')
    % Initilize
    strGenFOMCost = tic;
    
    % detect which entry in table_data
    idx_table_GenFOMCost = find(strcmp(table_names,'annual_cost_per_nameplate_capacity'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate6 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_GenFOMCost}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % separate FOM costs for all CH
    %--------------------------------------------------------------------------
    % assumes each GenType has the same cost across all CH regions
    
    % create identifiers for all CH regions generators that have listed elec
    % capacities for CH
    idx_GenFOMCost_Scen1_CHsep   = idx_swiss_AllAsSeparate6 & ismember(table_data{idx_table_GenFOMCost}.techs,tabledata_elecgens_Scen1_CHsum.techs);
    
    % get entries for all these CH generators
    tabledata_GenFOMCost_Scen1_CHsep = table_data{idx_table_GenFOMCost}(idx_GenFOMCost_Scen1_CHsep,:);
    
    % get unique techs (will stay in order)
    techs_CH4 = unique(tabledata_GenFOMCost_Scen1_CHsep.techs,'stable');
    
    % loop over each tech to find first entry FOM cost
    for i7 = 1:length(techs_CH4)
        
        % identify all rows with this tech
        idx_tech_GenFOMCost_Scen1_CHsep     = find(strcmp(tabledata_GenFOMCost_Scen1_CHsep.techs, techs_CH4(i7)),1,'first');
        
        % sum all entries for these timesteps
        data_GenFOMCost_Scen1_CHall(i7,1)    = tabledata_GenFOMCost_Scen1_CHsep.annual_cost_per_nameplate_capacity(idx_tech_GenFOMCost_Scen1_CHsep);
        
    end
    
    % create tabledata for all CH Gen Invstement cost
    tabledata_GenFOMCost_Scen1_CHall = tabledata_GenFOMCost_Scen1_CHsep(1:length(techs_CH4),:);         % initialize with correct length
    tabledata_GenFOMCost_Scen1_CHall.techs = techs_CH4;                                                 % replace list of techs
    tabledata_GenFOMCost_Scen1_CHall.locs = repmat({'CHE'},length(techs_CH4),1);                        % replace locs
    tabledata_GenFOMCost_Scen1_CHall.annual_cost_per_nameplate_capacity = data_GenFOMCost_Scen1_CHall;	% replace FOM costs
    
    %--------------------------------------------------------------------------
    % add FOM cost data to capacities data
    %--------------------------------------------------------------------------
    % convert units to EUR/MW/yr (from billion EUR/TW) and round to nearest 0.01
    % (entries not provided are assumed to have FOMCost=0)
    
    tabledata_elecgens_Scen1_CHsum = Func_AddToDataByTechType(tabledata_elecgens_Scen1_CHsum,tabledata_GenFOMCost_Scen1_CHall,'annual_cost_per_nameplate_capacity',{'unit4','annual_cost_per_nameplate_capacity'},'2015eur_per_mw_per_yr');
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % save Swiss regions (replace anything entered previously)
    CalliopeToNexuse.GenTypeParams.CH_country = tabledata_elecgens_Scen1_CHsum;
    % units
    CalliopeToNexuse.Units.FOMCost = ('2015EUR_per_MW-yr');
    
    disp(' ')
    disp(['The total processing time for the generator FOM costs is: ', num2str(toc(strGenFOMCost)), ' (s) '])
    disp('=========================================================================')
    %}
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize non-CH storage capacities (storage_capacity.csv)
    %   -installed storage volume capacities by gen type for each country
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values depend on the scenario
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the storage capacities begins... ')
    % Initilize
    strStorCap = tic;
    
    % detect which entry in table_data
    idx_table_StorCapacities = find(strcmp(table_names,'storage_capacity'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario7 = strcmp(table_data{idx_table_StorCapacities}.scenario,scen_opt_1_name);
    %idx_scenario7 = strcmp(table_data{idx_table_StorCapacities}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_StorCapacities}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_StorCapacities}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_StorCapacities}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_StorCapacities}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for electricity production
    idx_elecgen7 = strcmp(table_data{idx_table_StorCapacities}.carriers,'electricity');
    
    % identify data for neighboring each country
    idx_austria7 = strcmp(table_data{idx_table_StorCapacities}.locs,'AUT');
    idx_germany7 = strcmp(table_data{idx_table_StorCapacities}.locs,'DEU');
    idx_france7 = strcmp(table_data{idx_table_StorCapacities}.locs,'FRA');
    idx_italy7 = strcmp(table_data{idx_table_StorCapacities}.locs,'ITA');
    %idx_restofEU7 = strcmp(table_data{idx_table_StorCapacities}.locs,'rest_of_europe');
    
    % identify data for any CH region
    idx_swiss7 = strcmp(table_data{idx_table_StorCapacities}.locs,'CHE');
    %idx_swiss_AllAsSeparate7 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_StorCapacities}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % separate electricity Gen Capacities for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for each country's generators
    idx_elecgens_Scen7_CH   = idx_elecgen7 & idx_scenario7 & idx_swiss7;
    idx_elecgens_Scen7_DE   = idx_elecgen7 & idx_scenario7 & idx_germany7;
    idx_elecgens_Scen7_FR   = idx_elecgen7 & idx_scenario7 & idx_france7;
    idx_elecgens_Scen7_IT   = idx_elecgen7 & idx_scenario7 & idx_italy7;
    idx_elecgens_Scen7_AT   = idx_elecgen7 & idx_scenario7 & idx_austria7;
    %idx_elecgens_Scen7_EU   = idx_elecgen7 & idx_scenario7 & idx_restofEU7;
    
    % get entries for each country in given scenario
    tabledata_elecgens_Scen7_CH = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_CH,2:6);
    tabledata_elecgens_Scen7_DE = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_DE,2:6);
    tabledata_elecgens_Scen7_FR = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_FR,2:6);
    tabledata_elecgens_Scen7_IT = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_IT,2:6);
    tabledata_elecgens_Scen7_AT = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_AT,2:6);
    %tabledata_elecgens_Scen7_EU = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_EU,6:10);
    
    % convert units to MWh (from TWh) and round to nearest 0.1
    tabledata_elecgens_Scen7_CH.storage_capacity = round(tabledata_elecgens_Scen7_CH.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_DE.storage_capacity = round(tabledata_elecgens_Scen7_DE.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_FR.storage_capacity = round(tabledata_elecgens_Scen7_FR.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_IT.storage_capacity = round(tabledata_elecgens_Scen7_IT.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_AT.storage_capacity = round(tabledata_elecgens_Scen7_AT.storage_capacity*1000*1000,1);
    %tabledata_elecgens_Scen7_EU.storage_capacity = round(tabledata_elecgens_Scen7_EU.storage_capacity*1000*1000,1);
    % also modify 'unit' column
    tabledata_elecgens_Scen7_CH.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_CH,1),1);
    tabledata_elecgens_Scen7_DE.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_DE,1),1);
    tabledata_elecgens_Scen7_FR.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_FR,1),1);
    tabledata_elecgens_Scen7_IT.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_IT,1),1);
    tabledata_elecgens_Scen7_AT.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_AT,1),1);
    %tabledata_elecgens_Scen7_EU.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_EU,1),1);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % all non-Swiss regions
    CalliopeToNexuse.StorageCapacities.DE = tabledata_elecgens_Scen7_DE;
    CalliopeToNexuse.StorageCapacities.FR = tabledata_elecgens_Scen7_FR;
    CalliopeToNexuse.StorageCapacities.IT = tabledata_elecgens_Scen7_IT;
    CalliopeToNexuse.StorageCapacities.AT = tabledata_elecgens_Scen7_AT;
    %CalliopeToNexuse.StorageCapacities.EU = tabledata_elecgens_Scen7_EU;
    % also save Swiss regions
    CalliopeToNexuse.StorageCapacities.CH = tabledata_elecgens_Scen7_CH;
    %CalliopeToNexuse.StorageCapacities_CH_canton  = tabledata_elecgens_Scen7_CH;
    % units
    CalliopeToNexuse.Units.StorageCapacities = ('MWh');
    
    disp(' ')
    disp(['The total processing time for the storage capacities is: ', num2str(toc(strStorCap)), ' (s) '])
    disp('=========================================================================')
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize GTCs  (net_transfer_capacity.csv)
    %   -list of country-to-country borders (exporting to importing)
    %   -total grid transfer capacities From/To each country
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values depend on the scenario
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the Cross-Border Xfer capacities begins... ')
    % Initilize
    strXBlims = tic;
    
    % detect which entry in table_data
    idx_table_XBlims = find(strcmp(table_names,'net_transfer_capacity'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario5 = strcmp(table_data{idx_table_XBlims}.scenario,scen_opt_1_name);
    %idx_scenario5 = strcmp(table_data{idx_table_XBlims}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_XBlims}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_XBlims}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_XBlims}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_XBlims}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify all entries not internal to CH
    idx_XBlims_notCHinternal = ~( strcmp(cellfun(@(x) x(1:3),table_data{idx_table_XBlims}.exporting_region,'un',0),'CHE') & strcmp(cellfun(@(x) x(1:3),table_data{idx_table_XBlims}.importing_region,'un',0),'CHE'));
    
    % identify all entries NOT to/from same region
    idx_XBlims_notInternal = ~strcmp(table_data{idx_table_XBlims}.exporting_region, table_data{idx_table_XBlims}.importing_region);
    
    % create identifiers for all non CH-CH borders and non XX-XX borders 
    % for the desired scenario
    idx_XBlims_Scen5_notInternal = idx_scenario5 & idx_XBlims_notCHinternal & idx_XBlims_notInternal;
    
    %--------------------------------------------------------------------------
    % replace all with CHE and separate AC and DC XBlims
    %--------------------------------------------------------------------------
    
    % get XBlims for all non-internal borders for the desired scenario
    tabledata_XBlims_Scen5_notInternal = table_data{idx_table_XBlims}(idx_XBlims_Scen5_notInternal,:);
    
    % reset all CHE_X border to be just CHE so I can sum them up
    %idx_XBlims_exp_Scen5_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_XBlims_Scen5_notCHinternal.exporting_region,'un',0),'CHE');
    %idx_XBlims_imp_Scen5_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_XBlims_Scen5_notCHinternal.importing_region,'un',0),'CHE');
    %tabledata_XBlims_Scen5_notCHinternal.exporting_region(idx_XBlims_exp_Scen5_notCHinternal) = {'CHE'};
    %tabledata_XBlims_Scen5_notCHinternal.importing_region(idx_XBlims_imp_Scen5_notCHinternal) = {'CHE'};
    
    % separate AC and DC XBlims
    idx_XBlims_AC_Scen5_notInternal = strcmp(tabledata_XBlims_Scen5_notInternal.techs,'ac_transmission');
    idx_XBlims_DC_Scen5_notInternal = strcmp(tabledata_XBlims_Scen5_notInternal.techs,'dc_transmission');
    tabledata_XBlims_AC_Scen5_notInternal = tabledata_XBlims_Scen5_notInternal(idx_XBlims_AC_Scen5_notInternal,:);
    tabledata_XBlims_DC_Scen5_notInternal = tabledata_XBlims_Scen5_notInternal(idx_XBlims_DC_Scen5_notInternal,:);
    
    % also get a list of the unique border crossings
    borders_list_ac = unique(tabledata_XBlims_AC_Scen5_notInternal(:,[3 6]));
    borders_list_dc = unique(tabledata_XBlims_DC_Scen5_notInternal(:,[3 6]));
    
    %--------------------------------------------------------------------------
    % reduce list of borders to only those desired
    %--------------------------------------------------------------------------
    
    % define countries to keep
    countries_desired = {'CHE','DEU','FRA','ITA','AUT'};
    
    % FIRST reduce the borders_list_ac
    % identify any entries with exporting_region of desired countries
    idx_borderkeep_ac_exp = ismember(borders_list_ac.exporting_region,countries_desired);
    % identify any entries with importing_region of desired countries
    idx_borderkeep_ac_imp = ismember(borders_list_ac.importing_region,countries_desired);
    % combine index for any entry with BOTH export AND import
    idx_borderkeep_ac = idx_borderkeep_ac_exp & idx_borderkeep_ac_imp;
    % replace any non-desired countries with 'rest_of_europe'
    %borders_list_ac.exporting_region(~idx_borderkeep_ac_exp) = {'rest_of_europe'};
    %borders_list_ac.importing_region(~idx_borderkeep_ac_imp) = {'rest_of_europe'};
    % reduce the borders list, also eliminates the 'rest_of_europe' to 'rest_of_europe' border
    borders_list_ac(~idx_borderkeep_ac,:) = [];
    % only keep the unique entries (no need to list more than once)
    %borders_list_ac = unique(borders_list_ac);
    % save for DBcreation
    %CalliopeToNexuse.ImpExp_Borders_all = borders_list_ac;
    
    % SECOND reduce the borders_list_dc
    % identify any entries with exporting_region of desired countries
    idx_borderkeep_dc_exp = ismember(borders_list_dc.exporting_region,countries_desired);
    % identify any entries with importing_region of desired countries
    idx_borderkeep_dc_imp = ismember(borders_list_dc.importing_region,countries_desired);
    % combine index for any entry with BOTH export AND import
    idx_borderkeep_dc = idx_borderkeep_dc_exp & idx_borderkeep_dc_imp;
    % replace any non-desired countries with 'rest_of_europe'
    %borders_list_dc.exporting_region(~idx_borderkeep_dc_exp) = {'rest_of_europe'};
    %borders_list_dc.importing_region(~idx_borderkeep_dc_imp) = {'rest_of_europe'};
    % reduce the borders list, also eliminates the 'rest_of_europe' to 'rest_of_europe' border
    borders_list_dc(~idx_borderkeep_dc,:) = [];
    % only keep the unique entries (no need to list more than once)
    %borders_list_dc = unique(borders_list_dc);
    % save for DBcreation
    %CalliopeToNexuse.ImpExp_Borders_all = borders_list_dc;
    
    % THIRD reduce the annual AC NTCs data
    % identify any entries with exporting_region of desired countries
    idx_borderdatakeep_ac_exp = ismember(tabledata_XBlims_AC_Scen5_notInternal.exporting_region,countries_desired);
    % identify any entries with importing_region of desired countries
    idx_borderdatakeep_ac_imp = ismember(tabledata_XBlims_AC_Scen5_notInternal.importing_region,countries_desired);
    % combine index for any entry with BOTH export AND import
    idx_borderdatakeep_ac = idx_borderdatakeep_ac_exp & idx_borderdatakeep_ac_imp;
    % replace any non-desired countries with 'rest_of_europe'
    %tabledata_impexp_Scen4_notCHinternal.exporting_region(~idx_borderdatakeep_ac_exp) = {'rest_of_europe'};
    %tabledata_impexp_Scen4_notCHinternal.importing_region(~idx_borderdatakeep_ac_imp) = {'rest_of_europe'};
    % reduce the borders list, also eliminates the 'rest_of_europe' to 'rest_of_europe' border
    tabledata_XBlims_AC_Scen5_notInternal(~idx_borderdatakeep_ac,:) = [];
    
    % FOURTH reduce the annual DC NTCs data
    % identify any entries with exporting_region of desired countries
    idx_borderdatakeep_dc_exp = ismember(tabledata_XBlims_DC_Scen5_notInternal.exporting_region,countries_desired);
    % identify any entries with importing_region of desired countries
    idx_borderdatakeep_dc_imp = ismember(tabledata_XBlims_DC_Scen5_notInternal.importing_region,countries_desired);
    % combine index for any entry with BOTH export AND import
    idx_borderdatakeep_dc = idx_borderdatakeep_dc_exp & idx_borderdatakeep_dc_imp;
    % replace any non-desired countries with 'rest_of_europe'
    %tabledata_impexp_Scen4_notCHinternal.exporting_region(~idx_borderdatakeep_ac_exp) = {'rest_of_europe'};
    %tabledata_impexp_Scen4_notCHinternal.importing_region(~idx_borderdatakeep_ac_imp) = {'rest_of_europe'};
    % reduce the borders list, also eliminates the 'rest_of_europe' to 'rest_of_europe' border
    tabledata_XBlims_DC_Scen5_notInternal(~idx_borderdatakeep_dc,:) = [];
    
    %--------------------------------------------------------------------------
    % AC: sum for all CH, remove duplicates, format final data in struc
    %--------------------------------------------------------------------------
    
    % loop over each border
    for i8a = 1:size(borders_list_ac,1)
        
        % identify all rows with this border
        idx_ac_border = strcmp(tabledata_XBlims_AC_Scen5_notInternal.exporting_region,borders_list_ac.exporting_region(i8a)) & strcmp(tabledata_XBlims_AC_Scen5_notInternal.importing_region,borders_list_ac.importing_region(i8a));
        
        % sum all entries for this timestep and this border
        data_XBlims_AC_Scen5_CHsum(i8a,1) = sum(tabledata_XBlims_AC_Scen5_notInternal.net_transfer_capacity(idx_ac_border));
        
    end
    
    % convert values to MW and round to nearest MW
    data_XBlims_AC_Scen5_CHsum = round(data_XBlims_AC_Scen5_CHsum * 1000 * 1000,0);
    
    % identify and remove duplicates
    i8b = 1;
    while i8b < size(borders_list_ac,1)
        
        % find mirror copy of entry
        idx_ac_border_remove = strcmp(borders_list_ac.importing_region,borders_list_ac.exporting_region(i8b)) & strcmp(borders_list_ac.exporting_region,borders_list_ac.importing_region(i8b));
        
        %remove this entry
        borders_list_ac(idx_ac_border_remove,:) = [];
        data_XBlims_AC_Scen5_CHsum(idx_ac_border_remove,:) = [];
        
        % increment counter for nex row
        i8b = i8b + 1;
        
    end
    
    % manually replace CH transfer limits for NTC=1 or NTC=0.3 scenarios,
    % leave as provided by Calliope for NTC=0 (use their expanded NTCs)
    % DONT DO THIS ANYMORE, because Calliope fixed their NTC handling
    %{
    if scen_opt_1_NTCmult ~= 0
        % need to manually account for the NTC limits for CH borders, since
        % these values are not easily available in the Calliope datafiles
        
        % define expected current NTC limits (use same order of borders)
        NTC_vals = [1200;1200;4400;4400;3700;3700;4440;4440];
        NTC_borders = {'AUT','CHE';'CHE','AUT';'DEU','CHE';'CHE','DEU';'FRA','CHE';'CHE','FRA';'ITA','CHE';'CHE','ITA'};
        
        % loop over each border
        for i12 = 1:size(NTC_vals,1)
            % find index of this border in the data
            idx_border_CH = strcmp(borders_list_ac.exporting_region,NTC_borders(i12,1)) & strcmp(borders_list_ac.importing_region,NTC_borders(i12,2));
            
            % place appropriate value in for this entry, be sure to
            % multiply by the NTC factor
            data_XBlims_AC_Scen5_CHsum(idx_border_CH) = NTC_vals(i12)*scen_opt_1_NTCmult;
        end
        
    end
    %}
    
    % create tabledata for all AC borders for XBlim
    tabledata_XBlims_AC_Scen5_CHsum = tabledata_XBlims_AC_Scen5_notInternal(1:length(data_XBlims_AC_Scen5_CHsum),[2,3,6,4,7]);    % initialize with correct length
    tabledata_XBlims_AC_Scen5_CHsum.exporting_region = borders_list_ac.exporting_region;
    tabledata_XBlims_AC_Scen5_CHsum.importing_region = borders_list_ac.importing_region;
    tabledata_XBlims_AC_Scen5_CHsum.unit = repmat({'mw'},length(data_XBlims_AC_Scen5_CHsum),1);
    tabledata_XBlims_AC_Scen5_CHsum.net_transfer_capacity = data_XBlims_AC_Scen5_CHsum;
    
    %--------------------------------------------------------------------------
    % DC: sum for all CH, remove duplicates, format final data in struc
    %--------------------------------------------------------------------------
    
    % loop over each border
    for i8c = 1:size(borders_list_dc,1)
        
        % identify all rows with this border
        idx_dc_border = strcmp(tabledata_XBlims_DC_Scen5_notInternal.exporting_region,borders_list_dc.exporting_region(i8c)) & strcmp(tabledata_XBlims_DC_Scen5_notInternal.importing_region,borders_list_dc.importing_region(i8c));
        
        % sum all entries for this timestep and this border
        data_XBlims_DC_Scen5_CHsum(i8c,1) = sum(tabledata_XBlims_DC_Scen5_notInternal.net_transfer_capacity(idx_dc_border));
        
    end
    
    % convert values to MW and round to nearest MW
    data_XBlims_DC_Scen5_CHsum = round(data_XBlims_DC_Scen5_CHsum * 1000 * 1000,0);
    
    % identify and remove duplicates
    i8d = 1;
    while i8d < size(borders_list_dc,1)
        
        % find mirror copy of entry
        idx_dc_border_remove = strcmp(borders_list_dc.importing_region,borders_list_dc.exporting_region(i8d)) & strcmp(borders_list_dc.exporting_region,borders_list_dc.importing_region(i8d));
        
        %remove this entry
        borders_list_dc(idx_dc_border_remove,:) = [];
        data_XBlims_DC_Scen5_CHsum(idx_dc_border_remove,:) = [];
        
        % increment counter for nex row
        i8d = i8d + 1;
        
    end
    
    % create tabledata for all AC borders for XBlim
    tabledata_XBlims_DC_Scen5_CHsum = tabledata_XBlims_DC_Scen5_notInternal(1:length(data_XBlims_DC_Scen5_CHsum),[2,3,6,4,7]);    % initialize with correct length
    tabledata_XBlims_DC_Scen5_CHsum.exporting_region = borders_list_dc.exporting_region;
    tabledata_XBlims_DC_Scen5_CHsum.importing_region = borders_list_dc.importing_region;
    tabledata_XBlims_DC_Scen5_CHsum.unit = repmat({'mw'},length(data_XBlims_DC_Scen5_CHsum),1);
    tabledata_XBlims_DC_Scen5_CHsum.net_transfer_capacity = data_XBlims_DC_Scen5_CHsum;
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    CalliopeToNexuse.XBlims.AC = tabledata_XBlims_AC_Scen5_CHsum;
    CalliopeToNexuse.XBlims.DC = tabledata_XBlims_DC_Scen5_CHsum;
    % units
    CalliopeToNexuse.Units.XBlims = ('MW');
    
    
    disp(' ')
    disp(['The total processing time for the Cross-Border Xfer capacities is: ', num2str(toc(strXBlims)), ' (s) '])
    disp('=========================================================================')
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: flow_out.csv
    %   -hourly production from all units (will only need the
    %    non-dispatchable)
    %   -check that a capacity is defined and if not ignore the gen profile
    %   -hourly CO2 captured from all units
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are hourly (8760 entries)
    % the values in these profiles are NOT doubled
    % these profiles depend on the scenario
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the Fixed Injection profiles begins... ')
    % Initilize
    strFixedInj = tic;
    
    % detect which entry in table_data
    idx_table_FixedInj = find(strcmp(table_names,'flow_out'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario6 = strcmp(table_data{idx_table_FixedInj}.scenario,scen_opt_1_name);
    %idx_scenario6 = strcmp(table_data{idx_table_FixedInj}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_FixedInj}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_FixedInj}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_FixedInj}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_FixedInj}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for electricity production
    idx_elecgen6 = strcmp(table_data{idx_table_FixedInj}.carriers,'electricity');
    idx_co2capt6 = strcmp(table_data{idx_table_FixedInj}.carriers,'co2');
    
    % identify data for neighboring each country
    idx_austria5 = strcmp(table_data{idx_table_FixedInj}.locs,'AUT');
    idx_germany5 = strcmp(table_data{idx_table_FixedInj}.locs,'DEU');
    idx_france5 = strcmp(table_data{idx_table_FixedInj}.locs,'FRA');
    idx_italy5 = strcmp(table_data{idx_table_FixedInj}.locs,'ITA');
    
    % identify data for any CH region
    idx_swiss5 = strcmp(table_data{idx_table_FixedInj}.locs,'CHE');
    %idx_swiss_AllAsSeparate5 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_FixedInj}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % get timesteps
    %--------------------------------------------------------------------------
    
    % get unique timesteps (will sort by smallest)
    timesteps_num5 = unique(table_data{idx_table_FixedInj}.timesteps);
    % check for blanks and remove
    timesteps_num5(find(strcmp(timesteps_num5,''))) = [];
    % convert to datenum
    if iscell(timesteps_num5)
        timesteps_num5 = datenum(timesteps_num5);
    end
    timesteps_vec5 = datevec(timesteps_num5);
    % test if number of hours is 8760, edit or stop if needed
    if length(timesteps_num5) > 8760
        % more than expected time steps...just ignore the last one to keep
        % only 8760
        timesteps_num5 = timesteps_num5(1:8760);
        timesteps_vec5 = datevec(timesteps_num5);
    elseif length(timesteps_num5) < 8760
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        %disp('ERROR: number of timesteps in Demand data is less than 8760...')
        error('ERROR: number of timesteps in Generation data is less than 8760...')
    else
        % is ok to proceed
    end
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all CH
    %--------------------------------------------------------------------------
    
    % Fixed Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedInj_CH, data_FixedInj_Scen5_CH_MWh, tabledata_FixedInj_Scen5_CH_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_swiss5,idx_elecgen6,CalliopeToNexuse.GenTypeParams.CH);
    
    % CO2 Captured
    % use function to add hourly profiles for each technology type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_CO2_Capture_CH, data_CO2_Capture_Scen5_CH_MWh, tabledata_CO2_Capture_Scen5_CH_MWh] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_swiss5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.CH);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all DE - Fixed Injections
    %--------------------------------------------------------------------------
    
    % Fixed Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedInj_DE, data_FixedInj_Scen5_DE_MWh, tabledata_FixedInj_Scen5_DE_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_germany5,idx_elecgen6,CalliopeToNexuse.GenTypeParams.DE);
    
    % CO2 Captured
    % use function to add hourly profiles for each technology type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_CO2_Capture_DE, data_CO2_Capture_Scen5_DE_MWh, tabledata_CO2_Capture_Scen5_DE_MWh] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_germany5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.DE);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all FR - Fixed Injections
    %--------------------------------------------------------------------------
    
    % Fixed Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedInj_FR, data_FixedInj_Scen5_FR_MWh, tabledata_FixedInj_Scen5_FR_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_france5,idx_elecgen6,CalliopeToNexuse.GenTypeParams.FR);
    
    % CO2 Captured
    % use function to add hourly profiles for each technology type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_CO2_Capture_FR, data_CO2_Capture_Scen5_FR_MWh, tabledata_CO2_Capture_Scen5_FR_MWh] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_france5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.FR);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all IT - Fixed Injections
    %--------------------------------------------------------------------------
    
    % Fixed Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedInj_IT, data_FixedInj_Scen5_IT_MWh, tabledata_FixedInj_Scen5_IT_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_italy5,idx_elecgen6,CalliopeToNexuse.GenTypeParams.IT);
    
    % CO2 Captured
    % use function to add hourly profiles for each technology type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_CO2_Capture_IT, data_CO2_Capture_Scen5_IT_MWh, tabledata_CO2_Capture_Scen5_IT_MWh] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_italy5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.IT);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all AT - Fixed Injections
    %--------------------------------------------------------------------------
    
    % Fixed Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedInj_AT, data_FixedInj_Scen5_AT_MWh, tabledata_FixedInj_Scen5_AT_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_austria5,idx_elecgen6,CalliopeToNexuse.GenTypeParams.AT);
    
    % CO2 Captured
    % use function to add hourly profiles for each technology type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_CO2_Capture_AT, data_CO2_Capture_Scen5_AT_MWh, tabledata_CO2_Capture_Scen5_AT_MWh] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_austria5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.AT);
    
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % Fixed Injections
    CalliopeToNexuse.FixedInj_hrly.CH = tabledata_FixedInj_Scen5_CH_MWh;
    CalliopeToNexuse.FixedInj_hrly.DE = tabledata_FixedInj_Scen5_DE_MWh;
    CalliopeToNexuse.FixedInj_hrly.FR = tabledata_FixedInj_Scen5_FR_MWh;
    CalliopeToNexuse.FixedInj_hrly.IT = tabledata_FixedInj_Scen5_IT_MWh;
    CalliopeToNexuse.FixedInj_hrly.AT = tabledata_FixedInj_Scen5_AT_MWh;
    % units
    CalliopeToNexuse.Units.FixedInj = ('MWh');
    
    % CO2 Captured
    CalliopeToNexuse.CO2_Capture_hrly.CH = tabledata_CO2_Capture_Scen5_CH_MWh;
    CalliopeToNexuse.CO2_Capture_hrly.DE = tabledata_CO2_Capture_Scen5_DE_MWh;
    CalliopeToNexuse.CO2_Capture_hrly.FR = tabledata_CO2_Capture_Scen5_FR_MWh;
    CalliopeToNexuse.CO2_Capture_hrly.IT = tabledata_CO2_Capture_Scen5_IT_MWh;
    CalliopeToNexuse.CO2_Capture_hrly.AT = tabledata_CO2_Capture_Scen5_AT_MWh;
    
    %--------------------------------------------------------------------------
    % sum profiles over the year
    %--------------------------------------------------------------------------
    % Fixed Injections
    tabledata_FixedInj_Scen5_CH_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_CH_MWh,1),'VariableNames',techs_FixedInj_CH);
    tabledata_FixedInj_Scen5_DE_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_DE_MWh,1),'VariableNames',techs_FixedInj_DE);
    tabledata_FixedInj_Scen5_FR_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_FR_MWh,1),'VariableNames',techs_FixedInj_FR);
    tabledata_FixedInj_Scen5_IT_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_IT_MWh,1),'VariableNames',techs_FixedInj_IT);
    tabledata_FixedInj_Scen5_AT_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_AT_MWh,1),'VariableNames',techs_FixedInj_AT);
    
    % CO2 Captured
    tabledata_CO2_Capture_Scen5_CH_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_CH_MWh,1),'VariableNames',techs_CO2_Capture_CH);
    tabledata_CO2_Capture_Scen5_DE_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_DE_MWh,1),'VariableNames',techs_CO2_Capture_DE);
    tabledata_CO2_Capture_Scen5_FR_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_FR_MWh,1),'VariableNames',techs_CO2_Capture_FR);
    tabledata_CO2_Capture_Scen5_IT_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_IT_MWh,1),'VariableNames',techs_CO2_Capture_IT);
    tabledata_CO2_Capture_Scen5_AT_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_AT_MWh,1),'VariableNames',techs_CO2_Capture_AT);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % Fixed Injections
    CalliopeToNexuse.FixedInj_yrly.CH = tabledata_FixedInj_Scen5_CH_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.DE = tabledata_FixedInj_Scen5_DE_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.FR = tabledata_FixedInj_Scen5_FR_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.IT = tabledata_FixedInj_Scen5_IT_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.AT = tabledata_FixedInj_Scen5_AT_fullyr_sum;
    
    % CO2 Captured
    CalliopeToNexuse.CO2_Capture_yrly.CH = tabledata_CO2_Capture_Scen5_CH_fullyr_sum;
    CalliopeToNexuse.CO2_Capture_yrly.DE = tabledata_CO2_Capture_Scen5_DE_fullyr_sum;
    CalliopeToNexuse.CO2_Capture_yrly.FR = tabledata_CO2_Capture_Scen5_FR_fullyr_sum;
    CalliopeToNexuse.CO2_Capture_yrly.IT = tabledata_CO2_Capture_Scen5_IT_fullyr_sum;
    CalliopeToNexuse.CO2_Capture_yrly.AT = tabledata_CO2_Capture_Scen5_AT_fullyr_sum;
    % units
    CalliopeToNexuse.Units.CO2_Capture = ('MillionTonne');
    
    disp(' ')
    disp(['The total processing time for the Fixed Injection & CO2 Capture profiles is: ', num2str(toc(strFixedInj)), ' (s) '])
    disp('=========================================================================')
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: total_system_emissions.csv
    %   -annual CO2 emissions for each region by tech type
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are annual
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the Total System Emissions begins... ')
    % Initilize
    strTotEmiss = tic;
    
    % detect which entry in table_data
    idx_table_TotSysEmissions = find(strcmp(table_names,'total_system_emissions'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario8 = strcmp(table_data{idx_table_TotSysEmissions}.scenario,scen_opt_1_name);
    
    % identify data for ...
    %idx_elecgen6 = strcmp(table_data{idx_table_TotSysEmissions}.carriers,'electricity');
    %idx_co2capt6 = strcmp(table_data{idx_table_TotSysEmissions}.carriers,'co2');
    
    % identify data for neighboring each country
    idx_austria8 = strcmp(table_data{idx_table_TotSysEmissions}.locs,'AUT');
    idx_germany8 = strcmp(table_data{idx_table_TotSysEmissions}.locs,'DEU');
    idx_france8 = strcmp(table_data{idx_table_TotSysEmissions}.locs,'FRA');
    idx_italy8 = strcmp(table_data{idx_table_TotSysEmissions}.locs,'ITA');
    
    % identify data for any CH region
    idx_swiss8 = strcmp(table_data{idx_table_TotSysEmissions}.locs,'CHE');
    
    %--------------------------------------------------------------------------
    % separate CO2 emissions for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for each country's annual emissions
    idx_systemiss_Scen8_CH   = idx_scenario8 & idx_swiss8;
    idx_systemiss_Scen8_DE   = idx_scenario8 & idx_germany8;
    idx_systemiss_Scen8_FR   = idx_scenario8 & idx_france8;
    idx_systemiss_Scen8_IT   = idx_scenario8 & idx_italy8;
    idx_systemiss_Scen8_AT   = idx_scenario8 & idx_austria8;
    %idx_systemiss_Scen8_EU   = idx_scenario8 & idx_restofEU8;
    
    % get entries for each country in given scenario by tech type
    tabledata_systemiss_Scen8_CH = table_data{idx_table_TotSysEmissions}(idx_systemiss_Scen8_CH,3:5);
    tabledata_systemiss_Scen8_DE = table_data{idx_table_TotSysEmissions}(idx_systemiss_Scen8_DE,3:5);
    tabledata_systemiss_Scen8_FR = table_data{idx_table_TotSysEmissions}(idx_systemiss_Scen8_FR,3:5);
    tabledata_systemiss_Scen8_IT = table_data{idx_table_TotSysEmissions}(idx_systemiss_Scen8_IT,3:5);
    tabledata_systemiss_Scen8_AT = table_data{idx_table_TotSysEmissions}(idx_systemiss_Scen8_AT,3:5);
    %tabledata_systemiss_Scen8_EU = table_data{idx_table_TotSysEmissions}(idx_systemiss_Scen8_EU,6:10);
    
    % units already in Million-tonne, so just round to nearest kg
    tabledata_systemiss_Scen8_CH.total_system_emissions = round(tabledata_systemiss_Scen8_CH.total_system_emissions,6);
    tabledata_systemiss_Scen8_DE.total_system_emissions = round(tabledata_systemiss_Scen8_DE.total_system_emissions,6);
    tabledata_systemiss_Scen8_FR.total_system_emissions = round(tabledata_systemiss_Scen8_FR.total_system_emissions,6);
    tabledata_systemiss_Scen8_IT.total_system_emissions = round(tabledata_systemiss_Scen8_IT.total_system_emissions,6);
    tabledata_systemiss_Scen8_AT.total_system_emissions = round(tabledata_systemiss_Scen8_AT.total_system_emissions,6);
    %tabledata_systemiss_Scen8_EU.total_system_emissions = round(tabledata_systemiss_Scen8_EU.total_system_emissions,6);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % all non-Swiss regions
    CalliopeToNexuse.CO2_Emitted_yrly.DE = tabledata_systemiss_Scen8_DE;
    CalliopeToNexuse.CO2_Emitted_yrly.FR = tabledata_systemiss_Scen8_FR;
    CalliopeToNexuse.CO2_Emitted_yrly.IT = tabledata_systemiss_Scen8_IT;
    CalliopeToNexuse.CO2_Emitted_yrly.AT = tabledata_systemiss_Scen8_AT;
    %CalliopeToNexuse.CO2_Emitted_yrly.EU = tabledata_systemiss_Scen8_EU;
    % also save Swiss regions
    CalliopeToNexuse.CO2_Emitted_yrly.CH = tabledata_systemiss_Scen8_CH;
    % units
    CalliopeToNexuse.Units.CO2_Emitted = ('MillionTonne');

    disp(' ')
    disp(['The total processing time for the Total System Emissions is: ', num2str(toc(strTotEmiss)), ' (s) '])
    disp('=========================================================================')
    %}
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: duals.csv
    %   -hourly prices for CO2, H2, Gas, SynGas
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are hourly (8760 entries)
    % the values in these profiles are NOT doubled
    % these profiles depend on the scenario
    
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the Duals begins... ')
    % Initilize
    strDuals = tic;
    
    % detect which entry in table_data
    idx_table_Duals = find(strcmp(table_names,'duals'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario9 = strcmp(table_data{idx_table_Duals}.scenario,scen_opt_1_name);
    
    % identify data for carriers desired
    idx_elecprice9 = strcmp(table_data{idx_table_Duals}.carriers,'electricity');
    idx_co2price9 = strcmp(table_data{idx_table_Duals}.carriers,'co2');
    idx_biomethaneprice9 = strcmp(table_data{idx_table_Duals}.carriers,'biomethane');
    idx_synmethaneprice9 = strcmp(table_data{idx_table_Duals}.carriers,'syn_methane');
    idx_fossilmethaneprice9 = strcmp(table_data{idx_table_Duals}.carriers,'methane');
    idx_h2price9 = strcmp(table_data{idx_table_Duals}.carriers,'hydrogen');
    idx_coalprice9 = strcmp(table_data{idx_table_Duals}.carriers,'coal');
    
    % identify data for neighboring each country
    idx_austria9 = strcmp(table_data{idx_table_Duals}.locs,'AUT');
    idx_germany9 = strcmp(table_data{idx_table_Duals}.locs,'DEU');
    idx_france9 = strcmp(table_data{idx_table_Duals}.locs,'FRA');
    idx_italy9 = strcmp(table_data{idx_table_Duals}.locs,'ITA');
    % identify data for any CH region
    idx_swiss9 = strcmp(table_data{idx_table_Duals}.locs,'CHE');
    
    %--------------------------------------------------------------------------
    % get timesteps
    %--------------------------------------------------------------------------
    
    % get unique timesteps (will sort by smallest)
    timesteps_num9 = unique(table_data{idx_table_Duals}.timesteps);
    % check for blanks and remove
    timesteps_num9(find(strcmp(timesteps_num9,''))) = [];
    % convert to datenum
    if iscell(timesteps_num9)
        timesteps_num9 = datenum(timesteps_num9);
    end
    timesteps_vec9 = datevec(timesteps_num9);
    % test if number of hours is 8760, edit or stop if needed
    if length(timesteps_num9) > 8760
        % more than expected time steps...just ignore the last one to keep
        % only 8760
        timesteps_num9 = timesteps_num9(1:8760);
        timesteps_vec9 = datevec(timesteps_num9);
    elseif length(timesteps_num9) < 8760
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        %disp('ERROR: number of timesteps in Demand data is less than 8760...')
        error('ERROR: number of timesteps in Price data is less than 8760...')
    else
        % is ok to proceed
    end
    
    %--------------------------------------------------------------------------
    % get hourly price profiles for each country
    %--------------------------------------------------------------------------
    
    % use function to get hourly prices desired
    % CH (also keep track of prices names and units)
    [data_Prices_CH(:,1),name_ElecPrices,units_ElecPrices]                      = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_elecprice9);
    [data_Prices_CH(:,2),name_CO2Prices,units_CO2Prices]                        = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_co2price9);
    [data_Prices_CH(:,3),name_BioMethanePrices,units_BioMethanePrices]          = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_biomethaneprice9);
    [data_Prices_CH(:,4),name_SynMethanePrices,units_SynMethanePrices]          = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_synmethaneprice9);
    [data_Prices_CH(:,5),name_FossilMethanePrices,units_FossilMethanePrices]    = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_fossilmethaneprice9);
    [data_Prices_CH(:,6),name_H2Prices,units_H2Prices]                          = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_h2price9);
    [data_Prices_CH(:,7),name_CoalPrices,units_CoalPrices]                      = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_coalprice9);
    % DE
    [data_Prices_DE(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_elecprice9);
    [data_Prices_DE(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_co2price9);
    [data_Prices_DE(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_biomethaneprice9);
    [data_Prices_DE(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_synmethaneprice9);
    [data_Prices_DE(:,5),z1,z2]	= Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_fossilmethaneprice9);
    [data_Prices_DE(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_h2price9);
    [data_Prices_DE(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_coalprice9);
    % FR
    [data_Prices_FR(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_elecprice9);
    [data_Prices_FR(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_co2price9);
    [data_Prices_FR(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_biomethaneprice9);
    [data_Prices_FR(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_synmethaneprice9);
    [data_Prices_FR(:,5),z1,z2]	= Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_fossilmethaneprice9);
    [data_Prices_FR(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_h2price9);
    [data_Prices_FR(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_coalprice9);
    % IT
    [data_Prices_IT(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_elecprice9);
    [data_Prices_IT(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_co2price9);
    [data_Prices_IT(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_biomethaneprice9);
    [data_Prices_IT(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_synmethaneprice9);
    [data_Prices_IT(:,5),z1,z2]	= Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_fossilmethaneprice9);
    [data_Prices_IT(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_h2price9);
    [data_Prices_IT(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_coalprice9);
    % AT
    [data_Prices_AT(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_elecprice9);
    [data_Prices_AT(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_co2price9);
    [data_Prices_AT(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_biomethaneprice9);
    [data_Prices_AT(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_synmethaneprice9);
    [data_Prices_AT(:,5),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_fossilmethaneprice9);
    [data_Prices_AT(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_h2price9);
    [data_Prices_AT(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_coalprice9);
    
    % organize the price column names
    prices_colnames = {'Electricity','CO2','BioMethane','SynMethane','FossilMethane','Hydrogen','Coal'};  % [name_ElecPrices,name_CO2Prices,name_BioMethanePrices,name_SynMethanePrices,name_FossilMethanePrices,name_H2Prices,name_CoalPrices];
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %-------------------------------------------------------------------------- 
    CalliopeToNexuse.Prices.CH = array2table(data_Prices_CH,'VariableNames',prices_colnames);
    CalliopeToNexuse.Prices.DE = array2table(data_Prices_DE,'VariableNames',prices_colnames);
    CalliopeToNexuse.Prices.FR = array2table(data_Prices_FR,'VariableNames',prices_colnames);
    CalliopeToNexuse.Prices.IT = array2table(data_Prices_IT,'VariableNames',prices_colnames);
    CalliopeToNexuse.Prices.AT = array2table(data_Prices_AT,'VariableNames',prices_colnames);
    
    % Need to get and organize the units of each price, maybe not since
    % they will still be in the tables
    CalliopeToNexuse.Units.ElecPrices           = cell2mat(units_ElecPrices);
    CalliopeToNexuse.Units.CO2Prices            = cell2mat(units_CO2Prices);
    CalliopeToNexuse.Units.BioMethanePrices     = cell2mat(units_BioMethanePrices);
    CalliopeToNexuse.Units.SynMethanePrices     = cell2mat(units_SynMethanePrices);
    CalliopeToNexuse.Units.FossilMethanePrices  = cell2mat(units_FossilMethanePrices);
    CalliopeToNexuse.Units.H2Prices             = cell2mat(units_H2Prices);
    CalliopeToNexuse.Units.CoalPrices           = cell2mat(units_CoalPrices);
    
    disp(' ')
    disp(['The total processing time for the Duals is: ', num2str(toc(strDuals)), ' (s) '])
    disp('=========================================================================')
    %}
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Calculate some demand values in monthly intervals
    % OPTIONAL: to compare the full hourly profiles with the 8-day
    % resolution version of the profiles
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % data are in columns
    
    Hours_month_start = [1,  745, 1417,2161,2881,3625,4345,5089,5833,6553,7297,8017];
    Hours_month_end   = [744,1416,2160,2880,3624,4344,5088,5832,6552,7296,8016,8760];
    
    % --------------------------------------------------------------------
    % for resampled data in Nexus-e
    
    % first resample the demand data for every 8th day
    tpRes = 8;
    days1 = [1:8:365];
    hr_start_full = (days1-1)*24 + 1;
    hr_end_full   = hr_start_full + 23;
    for d1 = 1:length(days1)
        hr_start_pack = (d1-1)*24 +1;
        hr_end_pack = hr_start_pack + 23;
        Dem_pack_Base_tpRes(hr_start_pack:hr_end_pack) = CalliopeToNexuse.BaseElecDemand_hrly.CH(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_Rail_tpRes(hr_start_pack:hr_end_pack) = CalliopeToNexuse.RailElecDemand_hrly.CH(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_H2_tpRes(hr_start_pack:hr_end_pack)   = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_Emob_tpRes(hr_start_pack:hr_end_pack) = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Calliope(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_HP_tpRes(hr_start_pack:hr_end_pack)   = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump(hr_start_full(d1):hr_end_full(d1));
    end
    
    % now unpack back to 8760 hours
    hr_start_unpack = (days1-1)*24 + 1;
    hr_end_unpack = hr_start_unpack + tpRes*24-1;
    for d2 = 1:length(days1)-1
        hr_start_pack2 = (d2-1)*24 + 1;
        hr_end_pack2 = hr_start_pack2 + 23;
        Dem_unpack_Base_tpRes(hr_start_unpack(d2):hr_end_unpack(d2)) = repmat(Dem_pack_Base_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_Rail_tpRes(hr_start_unpack(d2):hr_end_unpack(d2)) = repmat(Dem_pack_Rail_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_H2_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))   = repmat(Dem_pack_H2_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_Emob_tpRes(hr_start_unpack(d2):hr_end_unpack(d2)) = repmat(Dem_pack_Emob_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_HP_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))   = repmat(Dem_pack_HP_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
    end
    % for last day only repeat for the remaining days
    days_remain = 365 - days1(end) + 1;
    Dem_unpack_Base_tpRes(hr_start_unpack(end):8760) = repmat(Dem_pack_Base_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_Rail_tpRes(hr_start_unpack(end):8760) = repmat(Dem_pack_Rail_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_H2_tpRes(hr_start_unpack(end):8760)   = repmat(Dem_pack_H2_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_Emob_tpRes(hr_start_unpack(end):8760) = repmat(Dem_pack_Emob_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_HP_tpRes(hr_start_unpack(end):8760)   = repmat(Dem_pack_HP_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    
    % calc monthly sums
    for m = 1:length(Hours_month_start)
        Dem_unpack_Mthly_Base_TWh(m) = sum(Dem_unpack_Base_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_Rail_TWh(m) = sum(Dem_unpack_Rail_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_H2_TWh(m)   = sum(Dem_unpack_H2_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_Emob_TWh(m) = sum(Dem_unpack_Emob_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_HP_TWh(m)   = sum(Dem_unpack_HP_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
    end
    
    % get annual sums
    Dem_unpack_Yrly_Base_TWh = sum(Dem_unpack_Mthly_Base_TWh);
    Dem_unpack_Yrly_Rail_TWh = sum(Dem_unpack_Mthly_Rail_TWh);
    Dem_unpack_Yrly_H2_TWh   = sum(Dem_unpack_Mthly_H2_TWh);
    Dem_unpack_Yrly_Emob_TWh = sum(Dem_unpack_Mthly_Emob_TWh);
    Dem_unpack_Yrly_HP_TWh   = sum(Dem_unpack_Mthly_HP_TWh);
    
    % --------------------------------------------------------------------
    % for Original dat from Calliope
    for m = 1:length(Hours_month_start)
        
        Dem_Rail_monthly_TWh(m) = sum(CalliopeToNexuse.RailElecDemand_hrly.CH(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_Base_monthly_TWh(m) = sum(CalliopeToNexuse.BaseElecDemand_hrly.CH(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_H2_monthly_TWh(m)   = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_Emob_monthly_TWh(m) = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Calliope(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_HP_monthly_TWh(m)   = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        
    end
    
    Dem_Rail_Tot_TWh = sum(Dem_Rail_monthly_TWh);
    Dem_Base_Tot_TWh = sum(Dem_Base_monthly_TWh);
    Dem_H2_Tot_TWh = sum(Dem_H2_monthly_TWh);
    Dem_Emob_Tot_TWh = sum(Dem_Emob_monthly_TWh);
    Dem_HP_Tot_TWh = sum(Dem_HP_monthly_TWh);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % hourly - original
    CalliopeToNexuse.DemandCompare.Orig_DemBase_CH_Profile_MWh  = CalliopeToNexuse.BaseElecDemand_hrly.CH';
    CalliopeToNexuse.DemandCompare.Orig_DemRail_CH_Profile_MWh  = CalliopeToNexuse.RailElecDemand_hrly.CH';
    CalliopeToNexuse.DemandCompare.Orig_DemH2_CH_Profile_MWh    = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen';
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_CH_Profile_MWh  = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Calliope';
    CalliopeToNexuse.DemandCompare.Orig_DemHP_CH_Profile_MWh    = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump';
    % hourly - 8-day resample
    CalliopeToNexuse.DemandCompare.tpRes8day_DemBase_CH_Profile_MWh  = Dem_unpack_Base_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemRail_CH_Profile_MWh  = Dem_unpack_Rail_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemH2_CH_Profile_MWh    = Dem_unpack_H2_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_CH_Profile_MWh  = Dem_unpack_Emob_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemHP_CH_Profile_MWh    = Dem_unpack_HP_tpRes;
    % monthly - original
    CalliopeToNexuse.DemandCompare.Orig_DemBase_CH_Monthly_TWh  = Dem_Base_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemRail_CH_Monthly_TWh  = Dem_Rail_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemH2_CH_Monthly_TWh    = Dem_H2_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_CH_Monthly_TWh  = Dem_Emob_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemHP_CH_Monthly_TWh    = Dem_HP_monthly_TWh;
    % monthly - 8-day resample
    CalliopeToNexuse.DemandCompare.tpRes8day_DemBase_CH_Monthly_TWh  = Dem_unpack_Mthly_Base_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemRail_CH_Monthly_TWh  = Dem_unpack_Mthly_Rail_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemH2_CH_Monthly_TWh    = Dem_unpack_Mthly_H2_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_CH_Monthly_TWh  = Dem_unpack_Mthly_Emob_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemHP_CH_Monthly_TWh    = Dem_unpack_Mthly_HP_TWh;
    % annual - original
    CalliopeToNexuse.DemandCompare.Orig_DemBase_CH_Annual_TWh  = Dem_Base_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemRail_CH_Annual_TWh  = Dem_Rail_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemH2_CH_Annual_TWh    = Dem_H2_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_CH_Annual_TWh  = Dem_Emob_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemHP_CH_Annual_TWh    = Dem_HP_Tot_TWh;
    % annual - 8-day resample
    CalliopeToNexuse.DemandCompare.tpRes8day_DemBase_CH_Annual_TWh  = Dem_unpack_Yrly_Base_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemRail_CH_Annual_TWh  = Dem_unpack_Yrly_Rail_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemH2_CH_Annual_TWh    = Dem_unpack_Yrly_H2_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_CH_Annual_TWh  = Dem_unpack_Yrly_Emob_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemHP_CH_Annual_TWh    = Dem_unpack_Yrly_HP_TWh;
    %}
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % save other general data for DBcreation
    %   -description of all energy generator types (not just electricity)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    
    % explanation of the tech types
    CalliopeToNexuse.techs_descr_all = table_data{find(strcmp(table_names,'names'))};
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % save processed Calliope results as .mat file for input to Nexus-e
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    
    % save data for this scenario
    save(filename,'CalliopeToNexuse')
    
    disp(' ')
    disp('*************************************************************************')
    disp('*************************************************************************')
    disp(['Save this scenario as: ', filename])
    disp('*************************************************************************')
    disp('*************************************************************************')
    
    
    
    
    %% End Main Loop
    
    disp(' ')
    disp(['The total preprocessing time for this scenario is: ', num2str(toc(strScen)), ' (s) '])
    disp('=========================================================================')
    
end


disp(' ')
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

disp(['The total run time for processing the Calliope data is: ', num2str(toc(strData)), ' (s) '])

disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp(' ')

%%







%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% FUNCTIONS
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^




%% This function aggregates the demand data desired for one country
%  note that

function [data_elecload_base_Scen1_sum_halfyr, data_elecload_hydrogen_Scen1_sum_halfyr, data_elecload_emobility_Scen1_sum_halfyr, data_elecload_heatpump_Scen1_sum_halfyr, data_elecload_rail_Scen1_sum_halfyr] = Func_GetCalliopeDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_country,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail)

%

% create identifiers for given region and each demand
idx_elecload_base_Scen1       = idx_elecload_base         & idx_scenario1     & idx_country;
idx_elecload_hydrogen_Scen1   = idx_elecload_hydrogen     & idx_scenario1     & idx_country;
idx_elecload_emobility_Scen1  = idx_elecload_emobility    & idx_scenario1     & idx_country;
idx_elecload_heatpump_Scen1   = idx_elecload_heatpump     & idx_scenario1     & idx_country;
idx_elecload_rail_Scen1       = idx_elecload_rail         & idx_scenario1     & idx_country;

% get demands for given region in given scenario
tabledata_elecload_base_Scen1      = table_data{idx_table_flowin}(idx_elecload_base_Scen1,:);
tabledata_elecload_hydrogen_Scen1  = table_data{idx_table_flowin}(idx_elecload_hydrogen_Scen1,:);
tabledata_elecload_emobility_Scen1 = table_data{idx_table_flowin}(idx_elecload_emobility_Scen1,:);
tabledata_elecload_heatpump_Scen1  = table_data{idx_table_flowin}(idx_elecload_heatpump_Scen1,:);
tabledata_elecload_rail_Scen1      = table_data{idx_table_flowin}(idx_elecload_rail_Scen1,:);

% loop over each timestep to add for region (assumes same timesteps in each
% type of data_elecload), will add if multiple regions or if multiple
% techs for each demand type or both
for i2 = 1:length(timesteps_num)
    
    % identify all rows with this timestep
    idx_t_elecload_base         = tabledata_elecload_base_Scen1.timesteps == timesteps_num(i2);
    idx_t_elecload_hydrogen     = tabledata_elecload_hydrogen_Scen1.timesteps == timesteps_num(i2);
    idx_t_elecload_emobility    = tabledata_elecload_emobility_Scen1.timesteps == timesteps_num(i2);
    idx_t_elecload_heatpump     = tabledata_elecload_heatpump_Scen1.timesteps == timesteps_num(i2);
    idx_t_elecload_rail         = tabledata_elecload_rail_Scen1.timesteps == timesteps_num(i2);
    
    % sum all entries for these timesteps
    data_elecload_base_Scen1_sum_halfyr(i2,1)     	= sum(tabledata_elecload_base_Scen1.flow_in(idx_t_elecload_base));
    data_elecload_hydrogen_Scen1_sum_halfyr(i2,1)  	= sum(tabledata_elecload_hydrogen_Scen1.flow_in(idx_t_elecload_hydrogen));
    data_elecload_emobility_Scen1_sum_halfyr(i2,1)	= sum(tabledata_elecload_emobility_Scen1.flow_in(idx_t_elecload_emobility));
    data_elecload_heatpump_Scen1_sum_halfyr(i2,1)  	= sum(tabledata_elecload_heatpump_Scen1.flow_in(idx_t_elecload_heatpump));
    data_elecload_rail_Scen1_sum_halfyr(i2,1)       = sum(tabledata_elecload_rail_Scen1.flow_in(idx_t_elecload_rail));
end

end


%% This function organized the generation data desired for one country
%  note that all tech types will be processed and since this loops over the
%  timesteps it will conform to the 8760 desired hours and also any missing
%  timesteps will be set to zero

function [techs_FixedInj, data_FixedInj_Scen_MWh, tabledata_FixedInj_Scen_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num,idx_scenario,idx_country,idx_elecgen,table_capacities)

% create identifiers for given region and all electric gen types
idx_FixedInj_Scen   = idx_scenario     & idx_elecgen &    idx_country;
% get entries for given region in given scenario
tabledata_FixedInj_Scen = table_data{idx_table_FixedInj}(idx_FixedInj_Scen,:);
% get list of technology types for CH
techs_FixedInj = unique(tabledata_FixedInj_Scen.techs);

% check if timesteps are in cell or numeric, switch to numeric if needed
if iscell(tabledata_FixedInj_Scen.timesteps)
    tabledata_FixedInj_Scen.timesteps = datenum(tabledata_FixedInj_Scen.timesteps);
end

% initialize counter for any techs with no capacity defined
counter1 = 0;

% loop over each tech type to organize the hourly injection profile
for i10 = 1:length(techs_FixedInj)
    
    % create identifiers for this tech type
    idx_FixedInj_tech = strcmp(tabledata_FixedInj_Scen.techs,techs_FixedInj{i10});
    
    % loop over each timestep to add up all entries in same region (assumes 
    % same timesteps in each type of data_FixedInj)
    for i11 = 1:length(timesteps_num)
        
        % create identifiers for this tech type
        %idx_FixedInj_tech = strcmp(tabledata_FixedInj_Scen.techs,techs_FixedInj{i10});
        % identify all rows with this timestep
        idx_t_elecgen     = tabledata_FixedInj_Scen.timesteps == timesteps_num(i11);
        % create final identifier for this tech and this timestep
        idx_FixedInj_tech_t = idx_FixedInj_tech & idx_t_elecgen;
        
        % sum all entries for these timesteps
        data_FixedInj_Scen_TWh(i11,i10) = sum(tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech_t));
        
        % get data for this tech type
        %data_FixedInj_Scen_TWh(:,i10) = tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech);
        % convert to MWh and round to nearest MWh
        %data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
        
    end
    
    % convert to MWh and round to nearest MWh
    data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
    
    % check to confirm that a capacity was given for this unit
    if ismember(techs_FixedInj{i10},table_capacities.techs)
        % YES, so proceed as normal
    else
        % NO, no capacity is defined for this production profile
        
        % increment counter
        counter1 = counter1+1;
        % track this tech number so we will remove it at the end
        techs_ToRemove(counter1) = i10;
        
    end
    
end

% check if any techs need to be removed
if counter1 > 0
    % remove any techs with no capacity defined (techs list and data)
    techs_FixedInj(techs_ToRemove) = [];
    data_FixedInj_Scen_MWh(:,techs_ToRemove) = [];
else
    % all techs are ok
end

% create table for saving all the CH profiles
tabledata_FixedInj_Scen_MWh = array2table(data_FixedInj_Scen_MWh,'VariableNames',techs_FixedInj);

end


%% This function organized the CO2 captured data desired for one country
%  note that all tech types will be processed and since this loops over the
%  timesteps it will conform to the 8760 desired hours and also any missing
%  timesteps will be set to zero

function [techs_CO2_Capture, data_CO2_Capture_Scen_MWh, tabledata_CO2_Capture_Scen_MWh] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_CO2_Capture,timesteps_num,idx_scenario,idx_country,idx_co2,table_capacities)

% create identifiers for given region and all technology types
idx_CO2_Capture_Scen   = idx_scenario     & idx_co2 &    idx_country;
% get entries for given region in given scenario
tabledata_CO2_Capture_Scen = table_data{idx_table_CO2_Capture}(idx_CO2_Capture_Scen,:);
% get list of technology types for CH
techs_CO2_Capture = unique(tabledata_CO2_Capture_Scen.techs);

% check if timesteps are in cell or numeric, switch to numeric if needed
if iscell(tabledata_CO2_Capture_Scen.timesteps)
    tabledata_CO2_Capture_Scen.timesteps = datenum(tabledata_CO2_Capture_Scen.timesteps);
end

% initialize counter for any techs with no capacity defined
counter1 = 0;

% loop over each tech type to organize the hourly profile
for i10 = 1:length(techs_CO2_Capture)
    
    % create identifiers for this tech type
    idx_CO2_Capture_tech = strcmp(tabledata_CO2_Capture_Scen.techs,techs_CO2_Capture{i10});
    
    % loop over each timestep to add up all entries in same region (assumes 
    % same timesteps in each type of data_FixedInj)
    for i11 = 1:length(timesteps_num)
        
        % create identifiers for this tech type
        %idx_FixedInj_tech = strcmp(tabledata_FixedInj_Scen.techs,techs_FixedInj{i10});
        % identify all rows with this timestep
        idx_t_CO2_Capture     = tabledata_CO2_Capture_Scen.timesteps == timesteps_num(i11);
        % create final identifier for this tech and this timestep
        idx_CO2_Capture_tech_t = idx_CO2_Capture_tech & idx_t_CO2_Capture;
        
        % sum all entries for these timesteps
        data_CO2_Capture_Scen_TWh(i11,i10) = sum(tabledata_CO2_Capture_Scen.flow_out(idx_CO2_Capture_tech_t));
        
        % get data for this tech type
        %data_FixedInj_Scen_TWh(:,i10) = tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech);
        % convert to MWh and round to nearest MWh
        %data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
        
    end
    
    % convert to million-tonne (in 100k-tonne) and round to nearest kg
    data_CO2_Capture_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_Mtonne(data_CO2_Capture_Scen_TWh(:,i10),9);
    
    %{
    % check to confirm that a capacity was given for this unit
    if ismember(techs_CO2_Capture{i10},table_capacities.techs)
        % YES, so proceed as normal
    else
        % NO, no capacity is defined for this production profile
        
        % increment counter
        counter1 = counter1+1;
        % track this tech number so we will remove it at the end
        techs_ToRemove(counter1) = i10;
        
    end
    %}
    
end

% check if any techs need to be removed
if counter1 > 0
    % remove any techs with no capacity defined (techs list and data)
    techs_CO2_Capture(techs_ToRemove) = [];
    data_CO2_Capture_Scen_MWh(:,techs_ToRemove) = [];
else
    % all techs are ok
end

% create table for saving all the CH profiles
tabledata_CO2_Capture_Scen_MWh = array2table(data_CO2_Capture_Scen_MWh,'VariableNames',techs_CO2_Capture);

end


%% This function organizes the Prices data desired for one country
%  note that since this loops over the
%  timesteps it will conform to the 8760 desired hours and also any missing
%  timesteps will be set to zero

function [data_Prices, data_Prices_name, data_Prices_unit] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario,idx_country,idx_price)

% create identifiers for given region and all desired prices
idx_Prices_Scen_Loc   = idx_scenario & idx_country & idx_price;


% get entries for given region in given scenario
tabledata_Prices = table_data{idx_table_Duals}(idx_Prices_Scen_Loc,:);
% only keep the columns I need
tabledata_Prices = tabledata_Prices(:,[3:6]);

% just get the price values as a numeric vector
data_Prices = tabledata_Prices.dual_value;
% return the name of the price (to use a column name)
data_Prices_name = tabledata_Prices.carriers(1);
% also keep track of the units for this price
data_Prices_unit = tabledata_Prices.unit(1);


% Don't think I need any of this...
%{

% get list of technology types for CH
techs_FixedInj = unique(tabledata_FixedInj_Scen.techs);

% check if timesteps are in cell or numeric, switch to numeric if needed
if iscell(tabledata_FixedInj_Scen.timesteps)
    tabledata_FixedInj_Scen.timesteps = datenum(tabledata_FixedInj_Scen.timesteps);
end

% initialize counter for any techs with no capacity defined
counter1 = 0;

% loop over each tech type to organize the hourly injection profile
for i10 = 1:length(techs_FixedInj)
    
    % create identifiers for this tech type
    idx_FixedInj_tech = strcmp(tabledata_FixedInj_Scen.techs,techs_FixedInj{i10});
    
    % loop over each timestep to add up all entries in same region (assumes 
    % same timesteps in each type of data_FixedInj)
    for i11 = 1:length(timesteps_num)
        
        % create identifiers for this tech type
        %idx_FixedInj_tech = strcmp(tabledata_FixedInj_Scen.techs,techs_FixedInj{i10});
        % identify all rows with this timestep
        idx_t_elecgen     = tabledata_FixedInj_Scen.timesteps == timesteps_num(i11);
        % create final identifier for this tech and this timestep
        idx_FixedInj_tech_t = idx_FixedInj_tech & idx_t_elecgen;
        
        % sum all entries for these timesteps
        data_FixedInj_Scen_TWh(i11,i10) = sum(tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech_t));
        
        % get data for this tech type
        %data_FixedInj_Scen_TWh(:,i10) = tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech);
        % convert to MWh and round to nearest MWh
        %data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
        
    end
    
    % convert to MWh and round to nearest MWh
    data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
    
    % check to confirm that a capacity was given for this unit
    if ismember(techs_FixedInj{i10},table_capacities.techs)
        % YES, so proceed as normal
    else
        % NO, no capacity is defined for this production profile
        
        % increment counter
        counter1 = counter1+1;
        % track this tech number so we will remove it at the end
        techs_ToRemove(counter1) = i10;
        
    end
    
end

% check if any techs need to be removed
if counter1 > 0
    % remove any techs with no capacity defined (techs list and data)
    techs_FixedInj(techs_ToRemove) = [];
    data_FixedInj_Scen_MWh(:,techs_ToRemove) = [];
else
    % all techs are ok
end

% create table for saving all the CH profiles
tabledata_FixedInj_Scen_MWh = array2table(data_FixedInj_Scen_MWh,'VariableNames',techs_FixedInj);
%}

end


%% This function converts a set of profiles of every 2nd hour in TWh into a profile of every hour in MWh
%  note that all Calliope data that are in every 2nd hour format actually
%  double the value to account for both hours at once

function [data_elecload_base_fullyr, data_elecload_hydrogen_fullyr, data_elecload_emobility_fullyr, data_elecload_heatpump_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num,data_elecload_base_halfyr,data_elecload_hydrogen_halfyr,data_elecload_emobility_halfyr,data_elecload_heatpump_halfyr)

% for each profile, interpolate for the missing hours, last hour is
% repeated

% loop over hours in data
for i2 = 1:length(timesteps_num)
    
    % create an identifier for the actual current hour
    t_hr = (i2-1)*2 + 1;
    
    % set entry for current hour
    data_elecload_base_fullyr(t_hr,1)         = data_elecload_base_halfyr(i2);
    data_elecload_hydrogen_fullyr(t_hr,1)     = data_elecload_hydrogen_halfyr(i2);
    data_elecload_emobility_fullyr(t_hr,1)    = data_elecload_emobility_halfyr(i2);
    data_elecload_heatpump_fullyr(t_hr,1)     = data_elecload_heatpump_halfyr(i2);
    
    if i2 ~= length(timesteps_num)
        % interpolate for missing hour
        data_elecload_base_fullyr(t_hr+1,1)         = (data_elecload_base_halfyr(i2)      + data_elecload_base_halfyr(i2+1))      / 2;
        data_elecload_hydrogen_fullyr(t_hr+1,1)     = (data_elecload_hydrogen_halfyr(i2)  + data_elecload_hydrogen_halfyr(i2+1))  / 2;
        data_elecload_emobility_fullyr(t_hr+1,1)    = (data_elecload_emobility_halfyr(i2) + data_elecload_emobility_halfyr(i2+1)) / 2;
        data_elecload_heatpump_fullyr(t_hr+1,1)     = (data_elecload_heatpump_halfyr(i2)  + data_elecload_heatpump_halfyr(i2+1))  / 2;
        
    else
        % set last hour as repeat of previous hour
        data_elecload_base_fullyr(t_hr+1,1)         = data_elecload_base_halfyr(i2);
        data_elecload_hydrogen_fullyr(t_hr+1,1)     = data_elecload_hydrogen_halfyr(i2);
        data_elecload_emobility_fullyr(t_hr+1,1) 	= data_elecload_emobility_halfyr(i2);
        data_elecload_heatpump_fullyr(t_hr+1,1)     = data_elecload_heatpump_halfyr(i2);
        
    end
    
end

% divide each profile in half to account for Calliope's summing the two
% hours
data_elecload_base_fullyr       = data_elecload_base_fullyr / 2;
data_elecload_hydrogen_fullyr   = data_elecload_hydrogen_fullyr / 2;
data_elecload_emobility_fullyr  = data_elecload_emobility_fullyr / 2;
data_elecload_heatpump_fullyr   = data_elecload_heatpump_fullyr / 2;

% convert each profile to MWh and round to nearest MWh
data_elecload_base_fullyr       = round(data_elecload_base_fullyr * 1000 * 1000,0);
data_elecload_hydrogen_fullyr   = round(data_elecload_hydrogen_fullyr * 1000 * 1000,0);
data_elecload_emobility_fullyr  = round(data_elecload_emobility_fullyr * 1000 * 1000,0);
data_elecload_heatpump_fullyr   = round(data_elecload_heatpump_fullyr * 1000 * 1000,0);

end

%% This function converts a set of hourly profiles in TWh into profiles in MWh
%  also rounds the values to the nearest desired decimal

function [data_elecload_base_fullyr_MWh, data_elecload_hydrogen_fullyr_MWh, data_elecload_emobility_fullyr_MWh, data_elecload_heatpump_fullyr_MWh, data_elecload_rail_fullyr_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_fullyr_TWh,data_elecload_hydrogen_fullyr_TWh,data_elecload_emobility_fullyr_TWh,data_elecload_heatpump_fullyr_TWh,data_elecload_rail_fullyr_TWh)

% convert each profile to MWh and round to nearest MWh
data_elecload_base_fullyr_MWh       = round(data_elecload_base_fullyr_TWh * 1000 * 1000,0);
data_elecload_hydrogen_fullyr_MWh   = round(data_elecload_hydrogen_fullyr_TWh * 1000 * 1000,0);
data_elecload_emobility_fullyr_MWh  = round(data_elecload_emobility_fullyr_TWh * 1000 * 1000,0);
data_elecload_heatpump_fullyr_MWh   = round(data_elecload_heatpump_fullyr_TWh * 1000 * 1000,0);
data_elecload_rail_fullyr_MWh       = round(data_elecload_rail_fullyr_TWh * 1000 * 1000,0);

end


%% This function converts a profile of every 2nd hour in TWh into a profile of every hour in MWh
%  note that all Calliope data that are in every 2nd hour format actually
%  double the value to account for both hours at once

function data_fullyr = Func_ConvertCalliopeProfile_HalfYr(timesteps_num,data_halfyr,rnd_to)

% for each profile, interpolate for the missing hours, last hour is
% repeated

% loop over hours in data
for i2 = 1:length(timesteps_num)
    
    % create an identifier for the actual current hour
    t_hr = (i2-1)*2 + 1;
    
    % set entry for current hour
    data_fullyr(t_hr,1)     = data_halfyr(i2);
    
    if i2 ~= length(timesteps_num)
        % interpolate for missing hour
        data_fullyr(t_hr+1,1)     = (data_halfyr(i2)  + data_halfyr(i2+1))  / 2;
        
    else
        % set last hour as repeat of previous hour
        data_fullyr(t_hr+1,1)     = data_halfyr(i2);
        
    end
    
end

% divide each profile in half to account for Calliope's summing the two
% hours
data_fullyr   = data_fullyr / 2;

% convert each profile to MWh and round based on passed rnd_to # decimals
data_fullyr   = round(data_fullyr * 1000 * 1000,rnd_to);

end

%% This function converts an hourly profile in TWh into a profile in MWh
%  also rounds the values to the nearest desired decimal

function data_fullyr_MWh = Func_ConvertCalliopeProfile_TWh(data_fullyr_TWh,rnd_to)

% convert each profile to MWh and round based on passed rnd_to # decimals
data_fullyr_MWh   = round(data_fullyr_TWh * 1000 * 1000,rnd_to);

end

%% This function converts an hourly profile in 100k-tonne into a profile in Million-tonne
%  also rounds the values to the nearest desired decimal

function data_fullyr_Mtonne = Func_ConvertCalliopeProfile_Mtonne(data_fullyr_100ktonne,rnd_to)

% convert each profile to MWh and round based on passed rnd_to # decimals
data_fullyr_Mtonne   = round(data_fullyr_100ktonne / 10,rnd_to);

end


%% This function adds to the existing tabledata_byTechType
%  data are added to match original TechTypes (missing entries are assume
%  to be zero), first column will be for units with entries as
%  'units_entries', second column will be the data with column name
%  'col_copy', 'col_head' defines the column names of the two added
%  columns,

function data_byTechType = Func_AddToDataByTechType(data_byTechType,tabledata_ToAdd,col_copy,col_heads,units_entries)
    
% get order of tech types for putting costs into gen capacities
[C,order_techs,ib] = intersect(data_byTechType.techs,tabledata_ToAdd.techs,'stable');
% add columns to DE for VOM costs - initialize cost to 0
data_byTechType = addvars(data_byTechType,repmat(units_entries,size(data_byTechType,1),1),zeros(size(data_byTechType,1),1),'NewVariableNames',col_heads);
% replace VOM costs from VOM data for all entries provided, convert units 
% to EUR/MWh (from billion EUR/TWh) and round to nearest 0.01 
% (entries not provided are assumed to have VOM=0)
data_byTechType.(col_copy)(order_techs) = round(tabledata_ToAdd.(col_copy)*1000,2);

end






