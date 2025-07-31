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
% Data import
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('The pulling of the Calliope data files begins... ')
% Initilize
strPullData = tic;

% add path to datapackage function for reading 'friendly_data' type
addpath('/Users/jared/Documents/MATLAB/datapackage/')

% set path to Calliope results folder
%datapath = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/CalliopeData_2022.01.31/friendly_data_nexus/';   	%FEN network drive from Jared's Mac
%datapath = '~/Documents/Research/ETH_FEN/GitLab/CalliopeData/';   	%local drive on Jared's iMac
%datapath = '/Users/jaredg/Documents/Research/ETH_FEN/GitLab/CalliopeData/';         % path on jared's iMac
datapath = '/Users/jared/Documents/Research/ETH_FEN/GitLab/EuroCalliope_Data/';     % path on jared's Macbook Pro
%datapath = '/Users/jaredg/Documents/Research/ETH_FEN/GitLab/CalliopeData/';
% define which set of results to import (which folder)
%folder = 'friendly_storylines_2016_1H_2030';
%folder = 'CalliopeData_2022.01.31/friendly_data_nexus';
folder = 'ScenarioData_01/2050-comp-abroad';
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
%path2 = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/';   	%FEN network drive from Jared's Mac
ReadFile3 = strcat(datapath,'Manually_BaseDemand/base_electricity_profile.csv');
% import EXPANSE normalized profile for e-mobility (normailzed by the
% annual total, unitless)
[base_demand_profile] = readtable(ReadFile3);

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


%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Data reduction
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% tables I need
%   - flow_in.csv                               -> electrified demand profiles          -> (by scenario)
%   - base_electricity_demand.csv               -> base electricity demand profiles     -> (independent of scenario)
%   - rail_electricity_consumption.csv       	-> CH rail electricity demand profile   -> (independent of scenario)
%   - net_import.csv                            -> nonCH-EU XB flow profiles            -> (by scenario)
%   - nameplate_capacity.csv                    -> generator capacities                 -> (by scenario)
%   - cost_per_nameplate_capacity.csv           -> investment costs (lifetime)          -> (independent of scenario)
%   - cost_per_flow_out.csv                     -> VOM costs                            -> (independent of scenario)
%   - annual_cost_per_nameplate_capacity.csv    -> FOM costs (annualized)               -> (independent of scenario)
%   - cost_per_flow_in.csv                      -> Fuel cost                            -> (independent of scenario)
%   - net_transfer_capacity.csv                 -> GTC limits for each XB               -> (by scenario)
%   - total_system_emissions.csv                -> just to get list of scenarios        -> (by scenario)
%   - names.csv                                 -> descriptions of techs                -> (independent of scenario)
%   - flow_out.csv                              -> fixed injection profiles             -> (by scenario)
%   - storage_capacity.csv                      -> energy storage capacities            -> (by scenario)

% eliminate tables I don't need
tables_need_list = {'flow_in','base_electricity_demand','rail_electricity_consumption','net_import','nameplate_capacity','cost_per_nameplate_capacity','cost_per_flow_out','annual_cost_per_nameplate_capacity','cost_per_flow_in','net_transfer_capacity','total_system_emissions','names','flow_out','storage_capacity'};
table_data(~ismember(table_names,tables_need_list)) = [];
table_names(~ismember(table_names,tables_need_list)) = [];

disp(' ')
disp(['The total processing time for the data reduction is: ', num2str(toc(strInit)), ' (s) '])
disp('=========================================================================')


%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% List of scenarios (total_system_emissions.csv)
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('Processing the list of scenarios begins... ')
% Initilize
strScenarios = tic;

% detect which entry in table_data
idx_table_scenarios = find(strcmp(table_names,'total_system_emissions'));

%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% create a list of the unique scenarios in these Calliope datafiles
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% create a temp table with the list of scenarios (have repeats)
tabledata_scenarios_temp = table_data{idx_table_scenarios}(:,1:5);

% replace NaNs in NTC_multiplier with 0
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


%%
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


%%
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


%%
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
    
    disp(' ')
    disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    disp(['Begin processing for Scenario ', num2str(ScenarioNum),'...'])
    % Initilize
    strScen = tic;
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Get scenario options
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    
    % first save details of the desired scenario
    CalliopeToNexuse.Scenario_current = tabledata_scenarios(ScenarioNum,:);
    
    % get scenario params
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
    %   for electrified loads (flow_in.csv) there are 3 techs:
    %      -electrolysis, heat, transport
    %   for producing electricity, there are 15 techs:
    %      -battery, biofuel_to_liquids, ccgt, chp_biofuel_extraction,
    %       chp_hydrogen, chp_methane_extraction, chp_wte_back_pressure,
    %       hydro_reservoir, hydro_run_of_river, nuclear, open_field_pv,
    %       pumped_hydro, roof_mounted_pv, wind_offshore, wind_onshore
    %
    % Other #2
    % locs ('AUT', 'DEU', 'FRA', 'ITA', 'CHE_1' -to- 'CHE_20', 'rest_of_europe')
    %
    % Other #3
    % carriers ('electricity', don't need others)
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: flow_in.csv
    %   -electrified hydrogen demands for each country (hourly)
    %   -electrified mobility demands for each country (hourly)
    %   -electrified heatpump demands for each country (hourly)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are every 2nd hour (4380 entries)
    % Calliope doubles the values to account for the full year
    % these profiles depend on the scenario
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the electrified demand profiles begins... ')
    % Initilize
    strElecLoad = tic;
    
    % detect which entry in table_data
    idx_table_flowin = find(strcmp(table_names,'flow_in'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    % identify data for desired scenario
    idx_scenario1 = strcmp(table_data{idx_table_flowin}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_flowin}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_flowin}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_flowin}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_flowin}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for hydrogen demand
    idx_elecload_hydrogen = strcmp(table_data{idx_table_flowin}.techs,'electrolysis');
    % identify data for e-mobility demand
    idx_elecload_emobility = strcmp(table_data{idx_table_flowin}.techs,'transport');
    % identify data for heatpump demand
    idx_elecload_heatpump = strcmp(table_data{idx_table_flowin}.techs,'heat');
    
    % identify data for neighboring each country
    idx_austria1 = strcmp(table_data{idx_table_flowin}.locs,'AUT');
    idx_germany1 = strcmp(table_data{idx_table_flowin}.locs,'DEU');
    idx_france1 = strcmp(table_data{idx_table_flowin}.locs,'FRA');
    idx_italy1 = strcmp(table_data{idx_table_flowin}.locs,'ITA');
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate1 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_flowin}.locs,'un',0),'CHE');
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all CH
    %--------------------------------------------------------------------------
    
    % create identifiers for all CH regions and each demand
    idx_elecload_hydrogen_Scen1_CHsep   = idx_elecload_hydrogen     & idx_scenario1     & idx_swiss_AllAsSeparate1;
    idx_elecload_emobility_Scen1_CHsep  = idx_elecload_emobility    & idx_scenario1     & idx_swiss_AllAsSeparate1;
    idx_elecload_heatpump_Scen1_CHsep   = idx_elecload_heatpump     & idx_scenario1     & idx_swiss_AllAsSeparate1;
    
    % get demands for all CH regions in given scenario
    tabledata_elecload_hydrogen_Scen1_CHsep = table_data{idx_table_flowin}(idx_elecload_hydrogen_Scen1_CHsep,:);
    tabledata_elecload_emobility_Scen1_CHsep = table_data{idx_table_flowin}(idx_elecload_emobility_Scen1_CHsep,:);
    tabledata_elecload_heatpump_Scen1_CHsep = table_data{idx_table_flowin}(idx_elecload_heatpump_Scen1_CHsep,:);
    
    % get unique timesteps (will sort by smallest)
    timesteps_num = unique(table_data{idx_table_flowin}.timesteps);
    timesteps_vec = datevec(timesteps_num);
    
    % loop over each timestep to add up all CH (assumes same timesteps in each
    % type of data_elecload)
    for i2 = 1:length(timesteps_num)
        
        % identify all rows with this timestep
        idx_t_elecload_hydrogen     = tabledata_elecload_hydrogen_Scen1_CHsep.timesteps == timesteps_num(i2);
        idx_t_elecload_emobility    = tabledata_elecload_emobility_Scen1_CHsep.timesteps == timesteps_num(i2);
        idx_t_elecload_heatpump     = tabledata_elecload_heatpump_Scen1_CHsep.timesteps == timesteps_num(i2);
        
        % sum all entries for these timesteps
        data_elecload_hydrogen_Scen1_CHsum_halfyr(i2,1)    = sum(tabledata_elecload_hydrogen_Scen1_CHsep.flow_in(idx_t_elecload_hydrogen));
        data_elecload_emobility_Scen1_CHsum_halfyr(i2,1)   = sum(tabledata_elecload_emobility_Scen1_CHsep.flow_in(idx_t_elecload_emobility));
        data_elecload_heatpump_Scen1_CHsum_halfyr(i2,1)    = sum(tabledata_elecload_heatpump_Scen1_CHsep.flow_in(idx_t_elecload_heatpump));
    end
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all DE
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_elecload_hydrogen_Scen1_DE   = idx_elecload_hydrogen     & idx_scenario1     & idx_germany1;
    idx_elecload_emobility_Scen1_DE  = idx_elecload_emobility    & idx_scenario1     & idx_germany1;
    idx_elecload_heatpump_Scen1_DE   = idx_elecload_heatpump     & idx_scenario1     & idx_germany1;
    
    % get entries for DE in given scenario
    tabledata_elecload_hydrogen_Scen1_DE_halfyr = table_data{idx_table_flowin}(idx_elecload_hydrogen_Scen1_DE,:);
    tabledata_elecload_emobility_Scen1_DE_halfyr = table_data{idx_table_flowin}(idx_elecload_emobility_Scen1_DE,:);
    tabledata_elecload_heatpump_Scen1_DE_halfyr = table_data{idx_table_flowin}(idx_elecload_heatpump_Scen1_DE,:);
    
    % get demands for DE in given scenario
    data_elecload_hydrogen_Scen1_DE_halfyr = tabledata_elecload_hydrogen_Scen1_DE_halfyr.flow_in;
    data_elecload_emobility_Scen1_DE_halfyr = tabledata_elecload_emobility_Scen1_DE_halfyr.flow_in;
    data_elecload_heatpump_Scen1_DE_halfyr = tabledata_elecload_heatpump_Scen1_DE_halfyr.flow_in;
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all FR
    %--------------------------------------------------------------------------
    
    % create identifiers for FR and each demand
    idx_elecload_hydrogen_Scen1_FR   = idx_elecload_hydrogen     & idx_scenario1     & idx_france1;
    idx_elecload_emobility_Scen1_FR  = idx_elecload_emobility    & idx_scenario1     & idx_france1;
    idx_elecload_heatpump_Scen1_FR   = idx_elecload_heatpump     & idx_scenario1     & idx_france1;
    
    % get entries for FR in given scenario
    tabledata_elecload_hydrogen_Scen1_FR_halfyr = table_data{idx_table_flowin}(idx_elecload_hydrogen_Scen1_FR,:);
    tabledata_elecload_emobility_Scen1_FR_halfyr = table_data{idx_table_flowin}(idx_elecload_emobility_Scen1_FR,:);
    tabledata_elecload_heatpump_Scen1_FR_halfyr = table_data{idx_table_flowin}(idx_elecload_heatpump_Scen1_FR,:);
    
    % get demands for FR in given scenario
    data_elecload_hydrogen_Scen1_FR_halfyr = tabledata_elecload_hydrogen_Scen1_FR_halfyr.flow_in;
    data_elecload_emobility_Scen1_FR_halfyr = tabledata_elecload_emobility_Scen1_FR_halfyr.flow_in;
    data_elecload_heatpump_Scen1_FR_halfyr = tabledata_elecload_heatpump_Scen1_FR_halfyr.flow_in;
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all IT
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_elecload_hydrogen_Scen1_IT   = idx_elecload_hydrogen     & idx_scenario1     & idx_italy1;
    idx_elecload_emobility_Scen1_IT  = idx_elecload_emobility    & idx_scenario1     & idx_italy1;
    idx_elecload_heatpump_Scen1_IT   = idx_elecload_heatpump     & idx_scenario1     & idx_italy1;
    
    % get entries for DE in given scenario
    tabledata_elecload_hydrogen_Scen1_IT_halfyr = table_data{idx_table_flowin}(idx_elecload_hydrogen_Scen1_IT,:);
    tabledata_elecload_emobility_Scen1_IT_halfyr = table_data{idx_table_flowin}(idx_elecload_emobility_Scen1_IT,:);
    tabledata_elecload_heatpump_Scen1_IT_halfyr = table_data{idx_table_flowin}(idx_elecload_heatpump_Scen1_IT,:);
    
    % get demands for DE in given scenario
    data_elecload_hydrogen_Scen1_IT_halfyr = tabledata_elecload_hydrogen_Scen1_IT_halfyr.flow_in;
    data_elecload_emobility_Scen1_IT_halfyr = tabledata_elecload_emobility_Scen1_IT_halfyr.flow_in;
    data_elecload_heatpump_Scen1_IT_halfyr = tabledata_elecload_heatpump_Scen1_IT_halfyr.flow_in;
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all AT
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_elecload_hydrogen_Scen1_AT   = idx_elecload_hydrogen     & idx_scenario1     & idx_austria1;
    idx_elecload_emobility_Scen1_AT  = idx_elecload_emobility    & idx_scenario1     & idx_austria1;
    idx_elecload_heatpump_Scen1_AT   = idx_elecload_heatpump     & idx_scenario1     & idx_austria1;
    
    % get entries for DE in given scenario
    tabledata_elecload_hydrogen_Scen1_AT_halfyr = table_data{idx_table_flowin}(idx_elecload_hydrogen_Scen1_AT,:);
    tabledata_elecload_emobility_Scen1_AT_halfyr = table_data{idx_table_flowin}(idx_elecload_emobility_Scen1_AT,:);
    tabledata_elecload_heatpump_Scen1_AT_halfyr = table_data{idx_table_flowin}(idx_elecload_heatpump_Scen1_AT,:);
    
    % get demands for DE in given scenario
    data_elecload_hydrogen_Scen1_AT_halfyr = tabledata_elecload_hydrogen_Scen1_AT_halfyr.flow_in;
    data_elecload_emobility_Scen1_AT_halfyr = tabledata_elecload_emobility_Scen1_AT_halfyr.flow_in;
    data_elecload_heatpump_Scen1_AT_halfyr = tabledata_elecload_heatpump_Scen1_AT_halfyr.flow_in;
    
    
    %--------------------------------------------------------------------------
    % CH,DE,FR,IT,AT interpolate to create 8760 hr profile and convert units to
    % MWh and round to nearest MWh
    %--------------------------------------------------------------------------
    
    % CH
    [data_elecload_hydrogen_Scen1_CHsum_fullyr, data_elecload_emobility_Scen1_CHsum_fullyr, data_elecload_heatpump_Scen1_CHsum_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num, data_elecload_hydrogen_Scen1_CHsum_halfyr, data_elecload_emobility_Scen1_CHsum_halfyr, data_elecload_heatpump_Scen1_CHsum_halfyr);
    % DE
    [data_elecload_hydrogen_Scen1_DE_fullyr, data_elecload_emobility_Scen1_DE_fullyr, data_elecload_heatpump_Scen1_DE_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num, data_elecload_hydrogen_Scen1_DE_halfyr, data_elecload_emobility_Scen1_DE_halfyr, data_elecload_heatpump_Scen1_DE_halfyr);
    % FR
    [data_elecload_hydrogen_Scen1_FR_fullyr, data_elecload_emobility_Scen1_FR_fullyr, data_elecload_heatpump_Scen1_FR_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num, data_elecload_hydrogen_Scen1_FR_halfyr, data_elecload_emobility_Scen1_FR_halfyr, data_elecload_heatpump_Scen1_FR_halfyr);
    % IT
    [data_elecload_hydrogen_Scen1_IT_fullyr, data_elecload_emobility_Scen1_IT_fullyr, data_elecload_heatpump_Scen1_IT_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num, data_elecload_hydrogen_Scen1_IT_halfyr, data_elecload_emobility_Scen1_IT_halfyr, data_elecload_heatpump_Scen1_IT_halfyr);
    % AT
    [data_elecload_hydrogen_Scen1_AT_fullyr, data_elecload_emobility_Scen1_AT_fullyr, data_elecload_heatpump_Scen1_AT_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num, data_elecload_hydrogen_Scen1_AT_halfyr, data_elecload_emobility_Scen1_AT_halfyr, data_elecload_heatpump_Scen1_AT_halfyr);
    
    
    %--------------------------------------------------------------------------
    % create different e-mobility profile using EXPANSE normalized profile
    %--------------------------------------------------------------------------
    
    % use the annual total for each country and the Expanse normalized profile
    % create a new version of the e-mobility demand profile
    % also round to the nearest MWh
    data_elecload_emobility_Scen1_CHsum_fullyr_EXP = round(sum(data_elecload_emobility_Scen1_CHsum_fullyr)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_DE_fullyr_EXP = round(sum(data_elecload_emobility_Scen1_DE_fullyr)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_FR_fullyr_EXP = round(sum(data_elecload_emobility_Scen1_FR_fullyr)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_IT_fullyr_EXP = round(sum(data_elecload_emobility_Scen1_IT_fullyr)*Expanse_emobility_normalized,0);
    data_elecload_emobility_Scen1_AT_fullyr_EXP = round(sum(data_elecload_emobility_Scen1_AT_fullyr)*Expanse_emobility_normalized,0);
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % units
    CalliopeToNexuse.Units.ElectrifiedDemands = ('MWh');
    
    % next save the electrified demand profiles (hourly)
    % CH
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen             = data_elecload_hydrogen_Scen1_CHsum_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Calliope 	= data_elecload_emobility_Scen1_CHsum_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_Expanse	= data_elecload_emobility_Scen1_CHsum_fullyr_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump             = data_elecload_heatpump_Scen1_CHsum_fullyr;
    % DE
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_hydrogen             = data_elecload_hydrogen_Scen1_DE_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_Calliope 	= data_elecload_emobility_Scen1_DE_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_Expanse    = data_elecload_emobility_Scen1_DE_fullyr_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_heatpump             = data_elecload_heatpump_Scen1_DE_fullyr;
    % FR
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_hydrogen             = data_elecload_hydrogen_Scen1_FR_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_Calliope  	= data_elecload_emobility_Scen1_FR_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_Expanse    = data_elecload_emobility_Scen1_FR_fullyr_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_heatpump             = data_elecload_heatpump_Scen1_FR_fullyr;
    % IT
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_hydrogen             = data_elecload_hydrogen_Scen1_IT_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_Calliope 	= data_elecload_emobility_Scen1_IT_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_Expanse    = data_elecload_emobility_Scen1_IT_fullyr_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_heatpump             = data_elecload_heatpump_Scen1_IT_fullyr;
    % AT
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_hydrogen             = data_elecload_hydrogen_Scen1_AT_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_Calliope 	= data_elecload_emobility_Scen1_AT_fullyr;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_Expanse    = data_elecload_emobility_Scen1_AT_fullyr_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_heatpump             = data_elecload_heatpump_Scen1_AT_fullyr;
    
    % next save the electrified demand profiles (annual)
    % CH
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_hydrogen          	= sum(data_elecload_hydrogen_Scen1_CHsum_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_Calliope	= sum(data_elecload_emobility_Scen1_CHsum_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_Expanse	= sum(data_elecload_emobility_Scen1_CHsum_fullyr_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_heatpump           	= sum(data_elecload_heatpump_Scen1_CHsum_fullyr);
    % DE
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_hydrogen          	= sum(data_elecload_hydrogen_Scen1_DE_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_Calliope 	= sum(data_elecload_emobility_Scen1_DE_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_Expanse  	= sum(data_elecload_emobility_Scen1_DE_fullyr_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_heatpump        	= sum(data_elecload_heatpump_Scen1_DE_fullyr);
    % FR
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_hydrogen          	= sum(data_elecload_hydrogen_Scen1_FR_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_Calliope	= sum(data_elecload_emobility_Scen1_FR_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_Expanse 	= sum(data_elecload_emobility_Scen1_FR_fullyr_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_heatpump        	= sum(data_elecload_heatpump_Scen1_FR_fullyr);
    % IT
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_hydrogen         	= sum(data_elecload_hydrogen_Scen1_IT_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_Calliope 	= sum(data_elecload_emobility_Scen1_IT_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_Expanse	= sum(data_elecload_emobility_Scen1_IT_fullyr_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_heatpump        	= sum(data_elecload_heatpump_Scen1_IT_fullyr);
    % AT
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_hydrogen            = sum(data_elecload_hydrogen_Scen1_AT_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_Calliope 	= sum(data_elecload_emobility_Scen1_AT_fullyr);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_Expanse	= sum(data_elecload_emobility_Scen1_AT_fullyr_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_heatpump         	= sum(data_elecload_heatpump_Scen1_AT_fullyr);
    
    disp(' ')
    disp(['The total processing time for the electrified demand profiles is: ', num2str(toc(strElecLoad)), ' (s) '])
    disp('=========================================================================')
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: net_import.csv
    %   -list of country-to-country borders (exporting to importing)
    %   -power flows From/To each country (hourly)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are every 2nd hour (4380 entries)
    % Calliope doubles the values to account for the full year
    % these profiles depend on the scenario
    
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
    
    % identify data for desired scenario
    idx_scenario4 = strcmp(table_data{idx_table_impexp}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_impexp}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_impexp}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_impexp}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_impexp}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify all entries not internal to CH
    idx_impexp_notCHinternal = ~( strcmp(cellfun(@(x) x(1:3),table_data{idx_table_impexp}.exporting_region,'un',0),'CHE') & strcmp(cellfun(@(x) x(1:3),table_data{idx_table_impexp}.importing_region,'un',0),'CHE'));
    
    % create identifiers for all nonCH borders for the desired scenario
    idx_impexp_Scen4_notCHinternal = idx_scenario4 & idx_impexp_notCHinternal;
    
    % create a list of all border crossings
    %borders_list = unique(table_data{idx_table_impexp}(idx_impexp_Scen4_notCHinternal,6:7));
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all CH
    %--------------------------------------------------------------------------
    
    % get XB flows for all nonCH borders for the desired scenario
    tabledata_impexp_Scen4_notCHinternal = table_data{idx_table_impexp}(idx_impexp_Scen4_notCHinternal,:);
    
    % reset all CHE_X border to be just CHE so I can sum them up
    idx_exp_Scen4_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_impexp_Scen4_notCHinternal.exporting_region,'un',0),'CHE');
    idx_imp_Scen4_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_impexp_Scen4_notCHinternal.importing_region,'un',0),'CHE');
    tabledata_impexp_Scen4_notCHinternal.exporting_region(idx_exp_Scen4_notCHinternal) = {'CHE'};
    tabledata_impexp_Scen4_notCHinternal.importing_region(idx_imp_Scen4_notCHinternal) = {'CHE'};
    
    % also get a list of the unique border crossings
    borders_list = unique(tabledata_impexp_Scen4_notCHinternal(:,6:7));
    % save for DBcreation
    CalliopeToNexuse.ImpExp_Borders_all = borders_list;
    
    % get unique timesteps (will sort by smallest)
    timesteps_num4 = unique(tabledata_impexp_Scen4_notCHinternal.timesteps);
    timesteps_vec4 = datevec(timesteps_num4);
    
    % loop over each border
    for i4a = 1:size(borders_list,1)
        
        % identify all rows with this border
        idx_t_border = strcmp(tabledata_impexp_Scen4_notCHinternal.exporting_region,borders_list.exporting_region(i4a)) & strcmp(tabledata_impexp_Scen4_notCHinternal.importing_region,borders_list.importing_region(i4a));
        
        % loop over each timestep to add up all CH
        for i4b = 1:length(timesteps_num4)
            
            % identify all rows with this timestep and this border
            idx_t_border_time = tabledata_impexp_Scen4_notCHinternal.timesteps == timesteps_num4(i4b) & idx_t_border;
            
            % sum all entries for this timestep and this border
            data_impexp_Scen4_CHsum_halfyr(i4b,i4a)    = sum(tabledata_impexp_Scen4_notCHinternal.net_import(idx_t_border_time));
            
        end
        
        % convert profiles to 8760 hours (interpolate and divide by 2), also
        % convert to MWh and round to nearest MWh
        data_impexp_Scen4_CHsum_fullyr(:,i4a) = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num4,data_impexp_Scen4_CHsum_halfyr(:,i4a),0);
        
    end
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    
    % note that I make these profiles as rows so I can identify which From/To
    % country they are with the corresponding CalliopeToNexuse.ImpExp_Borders
    CalliopeToNexuse.ImpExp_profiles_all = data_impexp_Scen4_CHsum_fullyr';
    
    %--------------------------------------------------------------------------
    % separate the Exp from DE,FR,IT,AT that is Imp to EU
    %--------------------------------------------------------------------------
    
    % separate only the exports from neighbors going as imports to rest_of_EU
    % (also don't need the EU -> EU entry
    idx_borders_ImpEU = strcmp(borders_list.importing_region,'rest_of_europe') & ~strcmp(borders_list.exporting_region,'rest_of_europe');
    
    % save list of these borders and profiles for DBcreation
    CalliopeToNexuse.ImpExp_Borders_need  = borders_list(idx_borders_ImpEU,:);
    CalliopeToNexuse.ImpExp_profiles_need = data_impexp_Scen4_CHsum_fullyr(:,idx_borders_ImpEU)';
    
    %--------------------------------------------------------------------------
    % create annual sum of import and export for each border
    %--------------------------------------------------------------------------
    
    % create temporary copy of all Imp/Exp data
    temp1_data_impexp_Scen4_CHsum_fullyr = data_impexp_Scen4_CHsum_fullyr;
    temp2_data_impexp_Scen4_CHsum_fullyr = data_impexp_Scen4_CHsum_fullyr;
    % replace negitives/positives with 0
    temp1_data_impexp_Scen4_CHsum_fullyr(temp1_data_impexp_Scen4_CHsum_fullyr < 0) = 0;
    temp2_data_impexp_Scen4_CHsum_fullyr(temp2_data_impexp_Scen4_CHsum_fullyr > 0) = 0;
    % sum to get the total imports and total exports for each border
    data_impexp_Scen4_ImpTot_all = sum(temp1_data_impexp_Scen4_CHsum_fullyr,1)';    % only (+)
    data_impexp_Scen4_ExpTot_all = sum(temp2_data_impexp_Scen4_CHsum_fullyr,1)';    % only (-)
    % also get totals for the borders 'needed'
    data_impexp_Scen4_ImpTot_need = data_impexp_Scen4_ImpTot_all(idx_borders_ImpEU,:);
    data_impexp_Scen4_ExpTot_need = data_impexp_Scen4_ExpTot_all(idx_borders_ImpEU,:);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    CalliopeToNexuse.ImpExp_YrlyTot_all.Imp = data_impexp_Scen4_ImpTot_all;
    CalliopeToNexuse.ImpExp_YrlyTot_all.Exp = data_impexp_Scen4_ExpTot_all;
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
    % any capacity value < 10 is ignored (set to 0)
    
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
    idx_scenario3 = strcmp(table_data{idx_table_GenCapacities}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_GenCapacities}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_GenCapacities}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_GenCapacities}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_GenCapacities}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for electricity production
    idx_elecgen = strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity');
    % ignore the tech type 'biofuel_to_liquids'
    idx_elecgen(strcmp(table_data{idx_table_GenCapacities}.techs,'biofuel_to_liquids')) = false;
    
    % identify data for neighboring each country
    idx_austria3 = strcmp(table_data{idx_table_GenCapacities}.locs,'AUT');
    idx_germany3 = strcmp(table_data{idx_table_GenCapacities}.locs,'DEU');
    idx_france3 = strcmp(table_data{idx_table_GenCapacities}.locs,'FRA');
    idx_italy3 = strcmp(table_data{idx_table_GenCapacities}.locs,'ITA');
    idx_restofEU3 = strcmp(table_data{idx_table_GenCapacities}.locs,'rest_of_europe');
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate3 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_GenCapacities}.locs,'un',0),'CHE');
    
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
    idx_elecgens_Scen1_DE   = idx_elecgen & idx_scenario3 & idx_germany3;
    idx_elecgens_Scen1_FR   = idx_elecgen & idx_scenario3 & idx_france3;
    idx_elecgens_Scen1_IT   = idx_elecgen & idx_scenario3 & idx_italy3;
    idx_elecgens_Scen1_AT   = idx_elecgen & idx_scenario3 & idx_austria3;
    idx_elecgens_Scen1_EU   = idx_elecgen & idx_scenario3 & idx_restofEU3;
    
    % get entries for each country in given scenario
    tabledata_elecgens_Scen1_DE = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_DE,6:10);
    tabledata_elecgens_Scen1_FR = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_FR,6:10);
    tabledata_elecgens_Scen1_IT = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_IT,6:10);
    tabledata_elecgens_Scen1_AT = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_AT,6:10);
    tabledata_elecgens_Scen1_EU = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_EU,6:10);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_elecgens_Scen1_DE.nameplate_capacity = round(tabledata_elecgens_Scen1_DE.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_FR.nameplate_capacity = round(tabledata_elecgens_Scen1_FR.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_IT.nameplate_capacity = round(tabledata_elecgens_Scen1_IT.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_AT.nameplate_capacity = round(tabledata_elecgens_Scen1_AT.nameplate_capacity*1000*1000,1);
    tabledata_elecgens_Scen1_EU.nameplate_capacity = round(tabledata_elecgens_Scen1_EU.nameplate_capacity*1000*1000,1);
    % also modify 'unit' column
    tabledata_elecgens_Scen1_DE.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_DE,1),1);
    tabledata_elecgens_Scen1_FR.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_FR,1),1);
    tabledata_elecgens_Scen1_IT.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_IT,1),1);
    tabledata_elecgens_Scen1_AT.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_AT,1),1);
    tabledata_elecgens_Scen1_EU.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_EU,1),1);
    
    %--------------------------------------------------------------------------
    % separate electricity gens for all CH
    %--------------------------------------------------------------------------
    
    % create identifiers for all CH regions generators
    idx_elecgens_Scen1_CHsep   = idx_elecgen & idx_scenario3 & idx_swiss_AllAsSeparate3;
    
    % get entries for each country in given scenario
    tabledata_elecgens_Scen1_CHsep = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_CHsep,6:10);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_elecgens_Scen1_CHsep.nameplate_capacity = round(tabledata_elecgens_Scen1_CHsep.nameplate_capacity*1000*1000,1);
    % also modify 'unit' column
    tabledata_elecgens_Scen1_CHsep.unit = repmat({'mw'},size(tabledata_elecgens_Scen1_CHsep,1),1);
    
    % get unique techs (will stay in order)
    techs_CH = unique(tabledata_elecgens_Scen1_CHsep.techs,'stable');
    
    % loop over each tech to add up all CH
    for i3 = 1:length(techs_CH)
        
        % identify all rows with this tech
        idx_tech_elecgens_Scen1_CHsep     = strcmp(tabledata_elecgens_Scen1_CHsep.techs, techs_CH(i3));
        
        % sum all entries for these timesteps
        data_elecgens_Scen1_CHsum(i3,1)    = sum(tabledata_elecgens_Scen1_CHsep.nameplate_capacity(idx_tech_elecgens_Scen1_CHsep));
        
    end
    
    % create tabledata for all CH elecgens
    tabledata_elecgens_Scen1_CHsum = tabledata_elecgens_Scen1_CHsep(1:length(techs_CH),:);  % initialize with correct length
    tabledata_elecgens_Scen1_CHsum.techs = techs_CH;                                        % replace list of techs
    tabledata_elecgens_Scen1_CHsum.locs = repmat({'CHE'},length(techs_CH),1);               % replace locs
    tabledata_elecgens_Scen1_CHsum.nameplate_capacity = data_elecgens_Scen1_CHsum;          % replace capacities
    
    %--------------------------------------------------------------------------
    % set any capacities < 10 MW to = 0 (eliminate really small capacities)
    %--------------------------------------------------------------------------
    tabledata_elecgens_Scen1_DE.nameplate_capacity( tabledata_elecgens_Scen1_DE.nameplate_capacity < 10 ) = 0;
    tabledata_elecgens_Scen1_FR.nameplate_capacity( tabledata_elecgens_Scen1_FR.nameplate_capacity < 10 ) = 0;
    tabledata_elecgens_Scen1_IT.nameplate_capacity( tabledata_elecgens_Scen1_IT.nameplate_capacity < 10 ) = 0;
    tabledata_elecgens_Scen1_AT.nameplate_capacity( tabledata_elecgens_Scen1_AT.nameplate_capacity < 10 ) = 0;
    tabledata_elecgens_Scen1_CHsum.nameplate_capacity( tabledata_elecgens_Scen1_CHsum.nameplate_capacity < 10 ) = 0;
    
    %--------------------------------------------------------------------------
    % separate each country by tech type - quick check for if capacities change
    %--------------------------------------------------------------------------
    
    tabledata_elecgens_ScenAll_DE = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'DEU') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    tabledata_elecgens_ScenAll_DE_sort = sortrows(tabledata_elecgens_ScenAll_DE,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_FR = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'FRA') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    tabledata_elecgens_ScenAll_FR_sort = sortrows(tabledata_elecgens_ScenAll_FR,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_IT = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'ITA') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    tabledata_elecgens_ScenAll_IT_sort = sortrows(tabledata_elecgens_ScenAll_IT,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_AU = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'AUT') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    tabledata_elecgens_ScenAll_AU_sort = sortrows(tabledata_elecgens_ScenAll_AU,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    
    % all non-Swiss regions
    CalliopeToNexuse.GenTypeParams.DE = tabledata_elecgens_Scen1_DE;
    CalliopeToNexuse.GenTypeParams.FR = tabledata_elecgens_Scen1_FR;
    CalliopeToNexuse.GenTypeParams.IT = tabledata_elecgens_Scen1_IT;
    CalliopeToNexuse.GenTypeParams.AT = tabledata_elecgens_Scen1_AT;
    CalliopeToNexuse.GenTypeParams.EU = tabledata_elecgens_Scen1_EU;
    % also save Swiss regions
    CalliopeToNexuse.GenTypeParams.CH = tabledata_elecgens_Scen1_CHsum;
    CalliopeToNexuse.GenCapacities_CH_canton  = tabledata_elecgens_Scen1_CHsep;
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
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize VOM costs  (cost_per_flow_out.csv)
    %   -VOM costs by gen type for each country (dont actually need CH since we
    %    use our own existing and candidate capacities)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are independent of scenario
    % assumes each GenType has same VOM cost across all CH regions
    
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
    
    
    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % organize FOM costs  (annual_cost_per_nameplate_capacity.csv)
    %   -FOM costs by gen type for CH
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are independent of scenario
    % assumes each GenType has same FOM cost across all CH regions
    % these FOM costs ARE annualized
    
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
    idx_scenario7 = strcmp(table_data{idx_table_StorCapacities}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_StorCapacities}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_StorCapacities}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_StorCapacities}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_StorCapacities}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for electricity production
    idx_elecgen7 = strcmp(table_data{idx_table_StorCapacities}.carriers,'electricity');
    
    % identify data for neighboring each country
    idx_austria7 = strcmp(table_data{idx_table_StorCapacities}.locs,'AUT');
    idx_germany7 = strcmp(table_data{idx_table_StorCapacities}.locs,'DEU');
    idx_france7 = strcmp(table_data{idx_table_StorCapacities}.locs,'FRA');
    idx_italy7 = strcmp(table_data{idx_table_StorCapacities}.locs,'ITA');
    idx_restofEU7 = strcmp(table_data{idx_table_StorCapacities}.locs,'rest_of_europe');
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate7 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_StorCapacities}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % separate electricity Gen Capacities for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for each country's generators
    idx_elecgens_Scen7_DE   = idx_elecgen7 & idx_scenario7 & idx_germany7;
    idx_elecgens_Scen7_FR   = idx_elecgen7 & idx_scenario7 & idx_france7;
    idx_elecgens_Scen7_IT   = idx_elecgen7 & idx_scenario7 & idx_italy7;
    idx_elecgens_Scen7_AT   = idx_elecgen7 & idx_scenario7 & idx_austria7;
    idx_elecgens_Scen7_EU   = idx_elecgen7 & idx_scenario7 & idx_restofEU7;
    
    % get entries for each country in given scenario
    tabledata_elecgens_Scen7_DE = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_DE,6:10);
    tabledata_elecgens_Scen7_FR = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_FR,6:10);
    tabledata_elecgens_Scen7_IT = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_IT,6:10);
    tabledata_elecgens_Scen7_AT = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_AT,6:10);
    tabledata_elecgens_Scen7_EU = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_EU,6:10);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_elecgens_Scen7_DE.storage_capacity = round(tabledata_elecgens_Scen7_DE.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_FR.storage_capacity = round(tabledata_elecgens_Scen7_FR.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_IT.storage_capacity = round(tabledata_elecgens_Scen7_IT.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_AT.storage_capacity = round(tabledata_elecgens_Scen7_AT.storage_capacity*1000*1000,1);
    tabledata_elecgens_Scen7_EU.storage_capacity = round(tabledata_elecgens_Scen7_EU.storage_capacity*1000*1000,1);
    % also modify 'unit' column
    tabledata_elecgens_Scen7_DE.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_DE,1),1);
    tabledata_elecgens_Scen7_FR.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_FR,1),1);
    tabledata_elecgens_Scen7_IT.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_IT,1),1);
    tabledata_elecgens_Scen7_AT.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_AT,1),1);
    tabledata_elecgens_Scen7_EU.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_EU,1),1);
    
    %--------------------------------------------------------------------------
    % separate electricity gens for all CH
    %--------------------------------------------------------------------------
    
    % create identifiers for all CH regions generators
    idx_elecgens_Scen7_CHsep   = idx_elecgen7 & idx_scenario7 & idx_swiss_AllAsSeparate7;
    
    % get entries for each country in given scenario
    tabledata_elecgens_Scen7_CHsep = table_data{idx_table_StorCapacities}(idx_elecgens_Scen7_CHsep,6:10);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_elecgens_Scen7_CHsep.storage_capacity = round(tabledata_elecgens_Scen7_CHsep.storage_capacity*1000*1000,1);
    % also modify 'unit' column
    tabledata_elecgens_Scen7_CHsep.unit = repmat({'mwh'},size(tabledata_elecgens_Scen7_CHsep,1),1);
    
    % get unique techs (will stay in order)
    techs_CH = unique(tabledata_elecgens_Scen7_CHsep.techs,'stable');
    
    % loop over each tech to add up all CH
    for i11 = 1:length(techs_CH)
        
        % identify all rows with this tech
        idx_tech_elecgens_Scen7_CHsep     = strcmp(tabledata_elecgens_Scen7_CHsep.techs, techs_CH(i11));
        
        % sum all entries for these timesteps
        data_elecgens_Scen7_CHsum(i11,1)    = sum(tabledata_elecgens_Scen7_CHsep.storage_capacity(idx_tech_elecgens_Scen7_CHsep));
        
    end
    
    % create tabledata for all CH elecgens
    tabledata_elecgens_Scen7_CHsum = tabledata_elecgens_Scen7_CHsep(1:length(techs_CH),:);  % initialize with correct length
    tabledata_elecgens_Scen7_CHsum.techs = techs_CH;                                        % replace list of techs
    tabledata_elecgens_Scen7_CHsum.locs = repmat({'CHE'},length(techs_CH),1);               % replace locs
    tabledata_elecgens_Scen7_CHsum.storage_capacity = data_elecgens_Scen7_CHsum;            % replace capacities
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % all non-Swiss regions
    CalliopeToNexuse.StorageCapacities.DE = tabledata_elecgens_Scen7_DE;
    CalliopeToNexuse.StorageCapacities.FR = tabledata_elecgens_Scen7_FR;
    CalliopeToNexuse.StorageCapacities.IT = tabledata_elecgens_Scen7_IT;
    CalliopeToNexuse.StorageCapacities.AT = tabledata_elecgens_Scen7_AT;
    CalliopeToNexuse.StorageCapacities.EU = tabledata_elecgens_Scen7_EU;
    % also save Swiss regions
    CalliopeToNexuse.StorageCapacities.CH = tabledata_elecgens_Scen7_CHsum;
    CalliopeToNexuse.StorageCapacities_CH_canton  = tabledata_elecgens_Scen7_CHsep;
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
    idx_scenario5 = strcmp(table_data{idx_table_XBlims}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_XBlims}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_XBlims}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_XBlims}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_XBlims}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify all entries not internal to CH
    idx_XBlims_notCHinternal = ~( strcmp(cellfun(@(x) x(1:3),table_data{idx_table_XBlims}.exporting_region,'un',0),'CHE') & strcmp(cellfun(@(x) x(1:3),table_data{idx_table_XBlims}.importing_region,'un',0),'CHE'));
    
    % identify all entries NOT to/from same region
    idx_XBlims_notInternal = ~strcmp(table_data{idx_table_XBlims}.exporting_region, table_data{idx_table_XBlims}.importing_region);
    
    % create identifiers for all CH-CH borders and non XX-XX borders for the
    % desired scenario
    idx_XBlims_Scen5_notCHinternal = idx_scenario5 & idx_XBlims_notCHinternal & idx_XBlims_notInternal;
    
    %--------------------------------------------------------------------------
    % replace all with CHE and separate AC and DC XBlims
    %--------------------------------------------------------------------------
    
    % get XBlims for all nonCH borders for the desired scenario
    tabledata_XBlims_Scen5_notCHinternal = table_data{idx_table_XBlims}(idx_XBlims_Scen5_notCHinternal,:);
    
    % reset all CHE_X border to be just CHE so I can sum them up
    idx_XBlims_exp_Scen5_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_XBlims_Scen5_notCHinternal.exporting_region,'un',0),'CHE');
    idx_XBlims_imp_Scen5_notCHinternal = strcmp(cellfun(@(x) x(1:3),tabledata_XBlims_Scen5_notCHinternal.importing_region,'un',0),'CHE');
    tabledata_XBlims_Scen5_notCHinternal.exporting_region(idx_XBlims_exp_Scen5_notCHinternal) = {'CHE'};
    tabledata_XBlims_Scen5_notCHinternal.importing_region(idx_XBlims_imp_Scen5_notCHinternal) = {'CHE'};
    
    % separate AC and DC XBlims
    idx_XBlims_AC_Scen5_notCHinternal = strcmp(tabledata_XBlims_Scen5_notCHinternal.techs,'ac_transmission');
    idx_XBlims_DC_Scen5_notCHinternal = strcmp(tabledata_XBlims_Scen5_notCHinternal.techs,'dc_transmission');
    tabledata_XBlims_AC_Scen5_notCHinternal = tabledata_XBlims_Scen5_notCHinternal(idx_XBlims_AC_Scen5_notCHinternal,:);
    tabledata_XBlims_DC_Scen5_notCHinternal = tabledata_XBlims_Scen5_notCHinternal(idx_XBlims_DC_Scen5_notCHinternal,:);
    
    % also get a list of the unique border crossings
    borders_list_ac = unique(tabledata_XBlims_AC_Scen5_notCHinternal(:,[7 10]));
    borders_list_dc = unique(tabledata_XBlims_DC_Scen5_notCHinternal(:,[7 10]));
    
    %--------------------------------------------------------------------------
    % AC: sum for all CH, remove duplicates, format final data in struc
    %--------------------------------------------------------------------------
    
    % loop over each border
    for i8a = 1:size(borders_list_ac,1)
        
        % identify all rows with this border
        idx_ac_border = strcmp(tabledata_XBlims_AC_Scen5_notCHinternal.exporting_region,borders_list_ac.exporting_region(i8a)) & strcmp(tabledata_XBlims_AC_Scen5_notCHinternal.importing_region,borders_list_ac.importing_region(i8a));
        
        % sum all entries for this timestep and this border
        data_XBlims_AC_Scen5_CHsum(i8a,1) = sum(tabledata_XBlims_AC_Scen5_notCHinternal.net_transfer_capacity(idx_ac_border));
        
    end
    
    % convert values to MW and round to nearest MW
    data_XBlims_AC_Scen5_CHsum = data_XBlims_AC_Scen5_CHsum * 1000 * 1000;
    
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
    
    % manually replace CH transfer limits for NTC=1 or NTC=0.3 scenarios
    if scen_opt_2_NTCmult ~= 0
        % need to manually account for the NTC limits for CH borders, since
        % these values are not easily available in the Calliope datafiles
        
        % define expected current NTC limits (use same order of borders)
        NTC_vals = [1200;1200;4000;4000;3000;3000;4240;4240];
        NTC_borders = {'AUT','CHE';'CHE','AUT';'DEU','CHE';'CHE','DEU';'FRA','CHE';'CHE','FRA';'ITA','CHE';'CHE','ITA'};
        
        % loop over each border
        for i12 = 1:size(NTC_vals,1)
            % find index of this border in the data
            idx_border_CH = strcmp(borders_list_ac.exporting_region,NTC_borders(i12,1)) & strcmp(borders_list_ac.importing_region,NTC_borders(i12,2));
            
            % place appropriate value in for this entry, be sure to
            % multiply by the NTC factor
            data_XBlims_AC_Scen5_CHsum(idx_border_CH) = NTC_vals(i12)*scen_opt_2_NTCmult;
        end
        
    end
    
    
    
    % create tabledata for all AC borders for XBlim
    tabledata_XBlims_AC_Scen5_CHsum = tabledata_XBlims_AC_Scen5_notCHinternal(1:length(data_XBlims_AC_Scen5_CHsum),[6,7,10,8,11]);    % initialize with correct length
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
        idx_dc_border = strcmp(tabledata_XBlims_DC_Scen5_notCHinternal.exporting_region,borders_list_dc.exporting_region(i8c)) & strcmp(tabledata_XBlims_DC_Scen5_notCHinternal.importing_region,borders_list_dc.importing_region(i8c));
        
        % sum all entries for this timestep and this border
        data_XBlims_DC_Scen5_CHsum(i8c,1) = sum(tabledata_XBlims_DC_Scen5_notCHinternal.net_transfer_capacity(idx_dc_border));
        
    end
    
    % convert values to MW and round to nearest MW
    data_XBlims_DC_Scen5_CHsum = data_XBlims_DC_Scen5_CHsum * 1000 * 1000;
    
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
    tabledata_XBlims_DC_Scen5_CHsum = tabledata_XBlims_DC_Scen5_notCHinternal(1:length(data_XBlims_DC_Scen5_CHsum),[6,7,10,8,11]);    % initialize with correct length
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
    %   -hourly wind production
    %   -
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are every 2nd hour (4380 entries)
    % Calliope doubles the values to account for the full year
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
    idx_scenario6 = strcmp(table_data{idx_table_FixedInj}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_FixedInj}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_FixedInj}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_FixedInj}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_FixedInj}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for neighboring each country
    idx_austria5 = strcmp(table_data{idx_table_FixedInj}.locs,'AUT');
    idx_germany5 = strcmp(table_data{idx_table_FixedInj}.locs,'DEU');
    idx_france5 = strcmp(table_data{idx_table_FixedInj}.locs,'FRA');
    idx_italy5 = strcmp(table_data{idx_table_FixedInj}.locs,'ITA');
    
    % identify data for any CH region
    idx_swiss_AllAsSeparate5 = strcmp(cellfun(@(x) x(1:3),table_data{idx_table_FixedInj}.locs,'un',0),'CHE');
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all CH
    %--------------------------------------------------------------------------
    
    % create identifiers for all CH regions and each demand
    idx_FixedInj_Scen5_CHsep   = idx_scenario6     & idx_swiss_AllAsSeparate5;
    % get demands for all CH regions in given scenario
    tabledata_FixedInj_Scen5_CHsep = table_data{idx_table_FixedInj}(idx_FixedInj_Scen5_CHsep,:);
    % get list of technology types for CH
    techs_FixedInj_CH = unique(tabledata_FixedInj_Scen5_CHsep.techs);
    
    % get unique timesteps (will sort by smallest)
    timesteps_num5 = unique(table_data{idx_table_FixedInj}.timesteps);
    timesteps_vec5 = datevec(timesteps_num5);
    
    % loop over each tech type to organize the hourly injection profile
    for i10 = 1:length(techs_FixedInj_CH)
        
        % create identifiers for this tech type
        idx_FixedInj_tech_CH = strcmp(tabledata_FixedInj_Scen5_CHsep.techs,techs_FixedInj_CH{i10});
        
        % loop over each timestep to add up all CH (assumes same timesteps in each
        % type of data)
        for i9 = 1:length(timesteps_num5)
            
            % identify all rows with this timestep
            idx_t_FixedInj     = tabledata_FixedInj_Scen5_CHsep.timesteps == timesteps_num5(i9);
            
            % sum all entries for these timesteps and this tech type
            data_FixedInj_Scen5_CHsum_halfyr(i9,i10)    = sum(tabledata_FixedInj_Scen5_CHsep.flow_out(idx_t_FixedInj & idx_FixedInj_tech_CH));
            
        end
        
        % convert profiles to 8760 hours (interpolate and divide by 2), also
        % convert to MWh and round to nearest MWh
        data_FixedInj_Scen5_CHsum_fullyr(:,i10) = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num5,data_FixedInj_Scen5_CHsum_halfyr(:,i10),1);
        
    end
    
    % create table for saving all the DE profiles
    tabledata_FixedInj_Scen5_CHsum_fullyr = array2table(data_FixedInj_Scen5_CHsum_fullyr,'VariableNames',techs_FixedInj_CH);
    
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all DE
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_FixedInj_Scen5_DE   = idx_scenario6     & idx_germany5;
    % get entries for DE in given scenario
    tabledata_FixedInj_Scen5_DE_halfyr = table_data{idx_table_FixedInj}(idx_FixedInj_Scen5_DE,:);
    % get list of technology types for DE
    techs_FixedInj_DE = unique(tabledata_FixedInj_Scen5_DE_halfyr.techs);
    
    % loop over each tech type to organize the hourly injection profile
    for i10 = 1:length(techs_FixedInj_DE)
        
        % create identifiers for this tech type
        idx_FixedInj_tech_DE = strcmp(tabledata_FixedInj_Scen5_DE_halfyr.techs,techs_FixedInj_DE{i10});
        % get data for this tech type
        data_FixedInj_Scen5_DE_halfyr(:,i10) = tabledata_FixedInj_Scen5_DE_halfyr.flow_out(idx_FixedInj_tech_DE);
        % convert profiles to 8760 hours (interpolate and divide by 2), also
        % convert to MWh and round to nearest MWh
        data_FixedInj_Scen5_DE_fullyr(:,i10) = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num5,data_FixedInj_Scen5_DE_halfyr(:,i10),1);
        
    end
    
    % create table for saving all the DE profiles
    tabledata_FixedInj_Scen5_DE_fullyr = array2table(data_FixedInj_Scen5_DE_fullyr,'VariableNames',techs_FixedInj_DE);
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all FR
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_FixedInj_Scen5_FR   = idx_scenario6     & idx_france5;
    % get entries for DE in given scenario
    tabledata_FixedInj_Scen5_FR_halfyr = table_data{idx_table_FixedInj}(idx_FixedInj_Scen5_FR,:);
    % get list of technology types for DE
    techs_FixedInj_FR = unique(tabledata_FixedInj_Scen5_FR_halfyr.techs);
    
    % loop over each tech type to organize the hourly injection profile
    for i10 = 1:length(techs_FixedInj_FR)
        
        % create identifiers for this tech type
        idx_FixedInj_tech_FR = strcmp(tabledata_FixedInj_Scen5_FR_halfyr.techs,techs_FixedInj_FR{i10});
        % get data for this tech type
        data_FixedInj_Scen5_FR_halfyr(:,i10) = tabledata_FixedInj_Scen5_FR_halfyr.flow_out(idx_FixedInj_tech_FR);
        % convert profiles to 8760 hours (interpolate and divide by 2), also
        % convert to MWh and round to nearest MWh
        data_FixedInj_Scen5_FR_fullyr(:,i10) = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num5,data_FixedInj_Scen5_FR_halfyr(:,i10),1);
        
    end
    
    % create table for saving all the DE profiles
    tabledata_FixedInj_Scen5_FR_fullyr = array2table(data_FixedInj_Scen5_FR_fullyr,'VariableNames',techs_FixedInj_FR);
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all IT
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_FixedInj_Scen5_IT   = idx_scenario6     & idx_italy5;
    % get entries for DE in given scenario
    tabledata_FixedInj_Scen5_IT_halfyr = table_data{idx_table_FixedInj}(idx_FixedInj_Scen5_IT,:);
    % get list of technology types for DE
    techs_FixedInj_IT = unique(tabledata_FixedInj_Scen5_IT_halfyr.techs);
    
    % loop over each tech type to organize the hourly injection profile
    for i10 = 1:length(techs_FixedInj_IT)
        
        % create identifiers for this tech type
        idx_FixedInj_tech_IT = strcmp(tabledata_FixedInj_Scen5_IT_halfyr.techs,techs_FixedInj_IT{i10});
        % get data for this tech type
        data_FixedInj_Scen5_IT_halfyr(:,i10) = tabledata_FixedInj_Scen5_IT_halfyr.flow_out(idx_FixedInj_tech_IT);
        % convert profiles to 8760 hours (interpolate and divide by 2), also
        % convert to MWh and round to nearest MWh
        data_FixedInj_Scen5_IT_fullyr(:,i10) = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num5,data_FixedInj_Scen5_IT_halfyr(:,i10),1);
        
    end
    
    % create table for saving all the DE profiles
    tabledata_FixedInj_Scen5_IT_fullyr = array2table(data_FixedInj_Scen5_IT_fullyr,'VariableNames',techs_FixedInj_IT);
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all AT
    %--------------------------------------------------------------------------
    
    % create identifiers for DE and each demand
    idx_FixedInj_Scen5_AT   = idx_scenario6     & idx_austria5;
    % get entries for DE in given scenario
    tabledata_FixedInj_Scen5_AT_halfyr = table_data{idx_table_FixedInj}(idx_FixedInj_Scen5_AT,:);
    % get list of technology types for DE
    techs_FixedInj_AT = unique(tabledata_FixedInj_Scen5_AT_halfyr.techs);
    
    % loop over each tech type to organize the hourly injection profile
    for i10 = 1:length(techs_FixedInj_AT)
        
        % create identifiers for this tech type
        idx_FixedInj_tech_AT = strcmp(tabledata_FixedInj_Scen5_AT_halfyr.techs,techs_FixedInj_AT{i10});
        % get data for this tech type
        data_FixedInj_Scen5_AT_halfyr(:,i10) = tabledata_FixedInj_Scen5_AT_halfyr.flow_out(idx_FixedInj_tech_AT);
        % convert profiles to 8760 hours (interpolate and divide by 2), also
        % convert to MWh and round to nearest MWh
        data_FixedInj_Scen5_AT_fullyr(:,i10) = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num5,data_FixedInj_Scen5_AT_halfyr(:,i10),1);
        
    end
    
    % create table for saving all the DE profiles
    tabledata_FixedInj_Scen5_AT_fullyr = array2table(data_FixedInj_Scen5_AT_fullyr,'VariableNames',techs_FixedInj_AT);
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    CalliopeToNexuse.FixedInj_hrly.CH = tabledata_FixedInj_Scen5_CHsum_fullyr;
    CalliopeToNexuse.FixedInj_hrly.DE = tabledata_FixedInj_Scen5_DE_fullyr;
    CalliopeToNexuse.FixedInj_hrly.FR = tabledata_FixedInj_Scen5_FR_fullyr;
    CalliopeToNexuse.FixedInj_hrly.IT = tabledata_FixedInj_Scen5_IT_fullyr;
    CalliopeToNexuse.FixedInj_hrly.AT = tabledata_FixedInj_Scen5_AT_fullyr;
    % units
    CalliopeToNexuse.Units.FixedInj = ('MWh');
    
    
    %--------------------------------------------------------------------------
    % sum profiles over the year
    %--------------------------------------------------------------------------
    
    tabledata_FixedInj_Scen5_CHsum_fullyr_sum = array2table(sum(data_FixedInj_Scen5_CHsum_fullyr,1),'VariableNames',techs_FixedInj_CH);
    tabledata_FixedInj_Scen5_DE_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_DE_fullyr,1),'VariableNames',techs_FixedInj_DE);
    tabledata_FixedInj_Scen5_FR_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_FR_fullyr,1),'VariableNames',techs_FixedInj_FR);
    tabledata_FixedInj_Scen5_IT_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_IT_fullyr,1),'VariableNames',techs_FixedInj_IT);
    tabledata_FixedInj_Scen5_AT_fullyr_sum    = array2table(sum(data_FixedInj_Scen5_AT_fullyr,1),'VariableNames',techs_FixedInj_AT);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    CalliopeToNexuse.FixedInj_yrly.CH = tabledata_FixedInj_Scen5_CHsum_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.DE = tabledata_FixedInj_Scen5_DE_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.FR = tabledata_FixedInj_Scen5_FR_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.IT = tabledata_FixedInj_Scen5_IT_fullyr_sum;
    CalliopeToNexuse.FixedInj_yrly.AT = tabledata_FixedInj_Scen5_AT_fullyr_sum;
    
    
    disp(' ')
    disp(['The total processing time for the Fixed Injection profiles is: ', num2str(toc(strFixedInj)), ' (s) '])
    disp('=========================================================================')
    
    
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








%% This function converts a profile of every 2nd hour in TWh into a profile of every hour in MWh
%  note that all Calliope data that are in every 2nd hour format actually
%  double the value to account for both hours at once

function [data_elecload_hydrogen_fullyr, data_elecload_emobility_fullyr, data_elecload_heatpump_fullyr] = Func_ConvertCalliopeProfiles_electrification(timesteps_num,data_elecload_hydrogen_halfyr,data_elecload_emobility_halfyr,data_elecload_heatpump_halfyr)

% for each profile, interpolate for the missing hours, last hour is
% repeated

% loop over hours in data
for i2 = 1:length(timesteps_num)
    
    % create an identifier for the actual current hour
    t_hr = (i2-1)*2 + 1;
    
    % set entry for current hour
    data_elecload_hydrogen_fullyr(t_hr,1)     = data_elecload_hydrogen_halfyr(i2);
    data_elecload_emobility_fullyr(t_hr,1)    = data_elecload_emobility_halfyr(i2);
    data_elecload_heatpump_fullyr(t_hr,1)     = data_elecload_heatpump_halfyr(i2);
    
    if i2 ~= length(timesteps_num)
        % interpolate for missing hour
        data_elecload_hydrogen_fullyr(t_hr+1,1)     = (data_elecload_hydrogen_halfyr(i2)  + data_elecload_hydrogen_halfyr(i2+1))  / 2;
        data_elecload_emobility_fullyr(t_hr+1,1)    = (data_elecload_emobility_halfyr(i2) + data_elecload_emobility_halfyr(i2+1)) / 2;
        data_elecload_heatpump_fullyr(t_hr+1,1)     = (data_elecload_heatpump_halfyr(i2)  + data_elecload_heatpump_halfyr(i2+1))  / 2;
        
    else
        % set last hour as repeat of previous hour
        data_elecload_hydrogen_fullyr(t_hr+1,1)     = data_elecload_hydrogen_halfyr(i2);
        data_elecload_emobility_fullyr(t_hr+1,1) 	= data_elecload_emobility_halfyr(i2);
        data_elecload_heatpump_fullyr(t_hr+1,1)     = data_elecload_heatpump_halfyr(i2);
        
    end
    
end

% divide each profile in half to account for Calliope's summing the two
% hours
data_elecload_hydrogen_fullyr   = data_elecload_hydrogen_fullyr / 2;
data_elecload_emobility_fullyr  = data_elecload_emobility_fullyr / 2;
data_elecload_heatpump_fullyr   = data_elecload_heatpump_fullyr / 2;

% convert each profile to MWh and round to nearest MWh
data_elecload_hydrogen_fullyr   = round(data_elecload_hydrogen_fullyr * 1000 * 1000,0);
data_elecload_emobility_fullyr  = round(data_elecload_emobility_fullyr * 1000 * 1000,0);
data_elecload_heatpump_fullyr   = round(data_elecload_heatpump_fullyr * 1000 * 1000,0);

end

%% This function converts a profile of every 2nd hour in TWh into a profile of every hour in MWh
%  note that all Calliope data that are in every 2nd hour format actually
%  double the value to account for both hours at once

function data_fullyr = Func_ConvertCalliopeProfiles_HalfYr(timesteps_num,data_halfyr,rnd_to)

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






