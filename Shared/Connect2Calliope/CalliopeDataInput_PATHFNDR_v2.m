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
%> edit by: Christina Graf
%> email  : chrgraf@student.ethz.ch
%> project: Using the Calliope results as Inputdata for CentIv & GasNet (Master Thesis) 
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
%>       : adding not only data used for Nexus-e but also used for GasNet (April 24, chrgraf)
%>
%> ========================================================================

%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Preparation before running the script:
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% open this script locally (The old versions can't be run on euler)
% "Install" the datapackage: function for reading 'friendly_data' typen (enought to have it in a folder)
% on line ~100 add path to datapackage function for reading 'friendly_data' type
% chose the location, from where the script is run (line 49)

% chose input 'folder' (regarding the wished scenario) on line 130
% chose 'filename' to save the resulting CalliopeToNexus.mat file on line 432


%% Clear / Close
clear all;
close all;
clc;

location = 'laptop'; % 'laptop' or 'euler'

%% Begin
% begin timer
strData = tic; % saves the current time in seconds to determine the elapsed time in the end.
disp(' ')
disp(['Skript ', mfilename, ' is running'])
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
disp('Begin processing the Calliope data...')


%%
% INFO: The InputParameters are going to be defined automatically acording 
% to the chosen folder.
% -> Choose the folder to be imported in line ~ 150

%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Define Input Parameters
% ToDo: set the names corresponding to the input folder on line 128 to create a correct name of the result file
% #1 - #3 correspond to the 3 scenario dimensions defined by PATHFNDR
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% #1
% set the value for the CO2 compensation option = 'abroad' or 'domestic'
% 'abroad' means the Swiss CO2 target can be compensated outside CH
% 'domestic' means the Swiss CO2 targer can only be compensated inside CH
scen_opt_1_Co2Comp = 'abroad';   	% value = 'abroad' or 'domestic'

% #2
% set the value for the energy market integration option = 'high' or 'low' 
% 'high' means current NTCs and ability to import fuels
% 'low' means reduced NTCs and no importing of fuels
scen_opt_2_MarketIntegr = 'low';

% #3
% set the value for the technology development option = 'progressive' or 'conservative'
% 'progressive' means ability to use new inovative technology for: generation, storage, CC process
%                     & demand flexibility in all sectors
% 'conservative' means only legacy technologies & demand flexibility inactive
scen_opt_3_TechDev = 'progressive';	% value = 'progressive' or 'conservative'

% #4
% set if EV Flexibility is allowed or disabled
% 'EVflex' means EV flexibility is allowed within limits
% 'noEVflex' means no EV flexibility
scen_opt_4_EVHPflex = 'noEVflex-noHPflex'; % value = 'EVflex-HPflex' or 'noEVflex-HPflex' or 'EVflex-noHPflex' or 'noEVflex-noHPflex'

%%
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Data import
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ')
disp('=========================================================================')
disp('The pulling of the Calliope data files begins... ')
% Initilize
strPullData = tic;

% if strcmp(location, 'laptop') % somehow location == 'laptop' did not work on euler (it did work however on my laptop R2022b)
%     addpath('C:\Users\Christina\Documents\Nexus-e_Code\nexus-e-framework-chrgraf\Shared\Connect2Calliope\datapackage\')
% elseif strcmp(location, 'euler')
%     addpath('/cluster/scratch/chrgraf/CalliopeConverions/datapackage/') % on Euler Scratch
% else
%     % add path to datapackage function for reading 'friendly_data' type
%     % addpath('/Users/jared/Documents/MATLAB/datapackage/')   % Macbook Pro
%     % addpath('/Users/jaredg/Documents/MATLAB/datapackage/')  % iMac
%     % addpath('/c/Users/Christina/Documents/2024/Studium/MasterThesis/Nexus-e_Code/nexus-e-framework-chrgraf/Shared/Connect2Calliope/datapackage/') % Christinas Laptop / cloned repo
%     % addpath('C:\Users\chrgraf\Documents\nexus-e-framework-chrgraf-Clone\nexus-e-framework-chrgraf\Shared\Connect2Calliope\datapackage\') % on RREW001 / cloned repo
% end
% 
% if strcmp(location, 'laptop')
%     datapath = 'T:\03_Student_Projects\ChristinaGraf\05_ExchangedData\53_InputForNexus_ResultFromCalliope\Results_EuroCalliope\'; % path to RRE server (can be used form christina's laptop or frm RREW001)
% elseif strcmp(location, 'euler')
%     datapath = '/cluster/scratch/chrgraf/CalliopeConverions/Results_EuroCalliope/'; % path on Euler Scratch
% else
%     % set path to Calliope results folder
%     %datapath = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/CalliopeData_2022.01.31/friendly_data_nexus/';   	%FEN network drive from Jared's Mac
%     %datapath = '~/Documents/Research/ETH_FEN/GitLab/CalliopeData/';   	%local drive on Jared's iMac
%     %datapath = '/Users/jared/Documents/Research/ETH_FEN/GitLab/EuroCalliope_Data/';     % path on jared's Macbook Pro
%     %datapath = '/Users/jaredg/Documents/Research/ETH_FEN/GitLab/CalliopeData/';         % path on jared's iMac
% end

% add path to datapackage function for reading 'friendly_data' type
addpath('/Users/jared/Documents/MATLAB/datapackage/')   % Macbook Pro
% set path to Calliope results folder
datapath = '/Users/jared/Documents/Research/ETH_FEN/GitLab/EuroCalliope_Data/';     % path on jared's Macbook Pro

% define which set of results to import (which folder)
%folder = 'friendly_storylines_2016_1H_2030';
%folder = 'CalliopeData_2022.01.31/friendly_data_nexus';
%folder = 'ScenarioData_01/2050-comp-abroad_v3';        % co2abroad, Jan 2023
%folder = 'ScenarioData_01/2050-no-comp-abroad_v3';     % co2swiss, Jan 2023
%folder = 'ScenarioData_02_c/2050-compensation_abroad-x30_ntc-progressive-1h';    	% co2abroad, ntc reduced, tech progressive
%folder = 'ScenarioData_02_c/2050-compensation_abroad-dynamic_ntc-progressive-1h';	% co2abroad, ntc current, tech progressive
% folder = 'ScenarioData_03g/2050-compensation_abroad-high-progressive-1h';             % co2abroad, ntc current, tech progressive, Feb 2024
% folder = 'ScenarioData_03g/2050-compensation_abroad-high-conservative-1h';            % co2abroad, ntc current, tech conservative, Feb 2024
% folder = 'ScenarioData_03g/2050-no_compensation_abroad-low-progressive-1h';           % co2domestic, ntc reduced, tech progressive, Feb 2024
% folder = 'ScenarioData_03g/2050-no_compensation_abroad-low-conservative-1h';          % co2domestic, ntc reduced, tech conservative, Feb 2024
% folder = 'ScenarioData_03g/2050-compensation_abroad-high-progressive-noEVflex-1h';	% co2abroad, ntc current, tech progressive, Feb 2024
% folder = 'ScenarioData_03g/2050-no_compensation_abroad-low-progressive-noEVflex-1h';  % co2abroad, ntc current, tech conservative, Feb 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-high-progressive-EVflex-HPflex-1h';      % co2abroad, ntc current, tech progressive, with EV & with HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-high-progressive-EVflex-noHPflex-1h'; 	  % co2abroad, ntc current, tech progressive, with EV & no   HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-high-progressive-noEVflex-HPflex-1h';    % co2abroad, ntc current, tech progressive, no   EV & with HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-high-progressive-noEVflex-noHPflex-1h';  % co2abroad, ntc current, tech progressive, no   EV & no   HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-low-progressive-EVflex-HPflex-1h';       % co2abroad, ntc low,     tech progressive, with EV & with HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-low-progressive-EVflex-noHPflex-1h';     % co2abroad, ntc low,     tech progressive, with EV & no   HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-low-progressive-noEVflex-HPflex-1h';     % co2abroad, ntc low,     tech progressive, no   EV & with HP flex, Nov 2024
% folder = 'ScenarioData_04/2050-compensation_abroad-low-progressive-noEVflex-noHPflex-1h';   % co2abroad, ntc low,     tech progressive, no   EV & no   HP flex, Nov 2024
% folder = 'ScenarioData_04c/2050-compensation_abroad-high-progressive-EVflex-HPflex-1h';      % s1 co2abroad, ntc current, tech progressive, with EV & with HP flex, Dec 2024
%---
% folder = 'ScenarioData_04d/2050-compensation_abroad-high-progressive-EVflex-HPflex-1h';      % s1 co2abroad, ntc current, tech progressive, with EV & with HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-high-progressive-noEVflex-HPflex-1h';    % s2 co2abroad, ntc current, tech progressive, no   EV & with HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-high-progressive-EVflex-noHPflex-1h';    % s3 co2abroad, ntc current, tech progressive, with EV & no   HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-high-progressive-noEVflex-noHPflex-1h';  % s4 co2abroad, ntc current, tech progressive, no   EV & no   HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-low-progressive-EVflex-HPflex-1h';       % s5 co2abroad, ntc low,     tech progressive, with EV & with HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-low-progressive-noEVflex-HPflex-1h';     % s6 co2abroad, ntc low,     tech progressive, no   EV & with HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-low-progressive-EVflex-noHPflex-1h';     % s7 co2abroad, ntc low,     tech progressive, with EV & no   HP flex, Jan 2025
% folder = 'ScenarioData_04d/2050-compensation_abroad-low-progressive-noEVflex-noHPflex-1h';   % s8 co2abroad, ntc low,     tech progressive, no   EV & no   HP flex, Jan 2025
%---
% folder = 'ScenarioData_04e/2050-compensation_abroad-high-progressive-EVflex-HPflex-1h';      % s1 co2abroad, ntc current, tech progressive, with EV & with HP flex, Feb 2025
% folder = 'ScenarioData_04e/2050-compensation_abroad-high-progressive-noEVflex-HPflex-1h';    % s2 co2abroad, ntc current, tech progressive, no   EV & with HP flex, Feb 2025
% folder = 'ScenarioData_04e/2050-compensation_abroad-high-progressive-EVflex-noHPflex-1h';    % s3 co2abroad, ntc current, tech progressive, with EV & no   HP flex, Feb 2025
% folder = 'ScenarioData_04e/2050-compensation_abroad-high-progressive-noEVflex-noHPflex-1h';  % s4 co2abroad, ntc current, tech progressive, no   EV & no   HP flex, Feb 2025
% folder = 'ScenarioData_04e/2050-compensation_abroad-low-progressive-EVflex-HPflex-1h';       % s5 co2abroad, ntc low,     tech progressive, with EV & with HP flex, Feb 2025
% folder = 'ScenarioData_04e/2050-compensation_abroad-low-progressive-noEVflex-HPflex-1h';     % s6 co2abroad, ntc low,     tech progressive, no   EV & with HP flex, Feb 2025
% folder = 'ScenarioData_04e/2050-compensation_abroad-low-progressive-EVflex-noHPflex-1h';     % s7 co2abroad, ntc low,     tech progressive, with EV & no   HP flex, Feb 2025
folder = 'ScenarioData_04e/2050-compensation_abroad-low-progressive-noEVflex-noHPflex-1h';   % s8 co2abroad, ntc low,     tech progressive, no   EV & no   HP flex, Feb 2025

%folder = '2050-compensation_abroad-high-conservative-1h' % I simulated this one to envelope the script%
%folder = '2050-no_compensation_abroad-low-conservative-1h'
%folder = '2050-no_compensation_abroad-low-progressive-1h'


%folder = '2050-compensation_abroad-high-progressive-1h'
% folder = '2050-compensation_abroad-high-progressive-noEVflex-1h'
%folder = '2050-no_compensation_abroad-low-progressive-1h';

% Christina Graff: automate the process of defining scenario identifiers
%{
% before continuing Data import
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% Define Input Parameters automatically, according to the chosen folder
% #1 - #3 correspond to the 3 scenario dimensions defined by PATHFNDR
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

% #1
% set the value for the CO2 compensation option = 'abroad' or 'domestic'
% 'abroad' means the Swiss CO2 target can be compensated outside CH
% 'domestic' means the Swiss CO2 targer can only be compensated inside CH

% #2
% set the value for the energy market integration option = 'high' or 'low' 
% 'high' means current NTCs and ability to import fuels
% 'low' means reduced NTCs and no importing of fuels

% #3
% set the value for the technology development option = 'progressive' or 'conservative'
% 'progressive' means ability to use new inovative technology for: generation, storage, CC process
%                     & demand flexibility in all sectors
% 'conservative' means only legacy technologies & demand flexibility inactive

% #4
% set if EV Flexibility is allowed or disabled
% 'EVflex' means EV flexibility is allowed within limits
% 'noEVflex' means no EV flexibility



% analyze the forlder name:
idx_newNameInfo = sort([strfind(folder, '-'),strfind(folder, '_')]); % identify at which position in the foldername a two piece of information is seperated
nameInfo=strings([1,length(idx_newNameInfo)]); % create empty array for saving the scenario information gathered from the folder name

for idx=1:length(idx_newNameInfo) % loop through the pices of information
    letter = idx_newNameInfo(idx)+1; % start position of new information
    if idx~=length(idx_newNameInfo) % for all but the last information, it goes until the next '-' or '_' sign (else: the last information goes till the end of the folder name)
        while letter<idx_newNameInfo(idx+1)
            nameInfo(idx)=append(nameInfo(idx),folder(letter)); % the letters of the info are written to the 
            letter=letter+1;
        end
    else % the last information goes till the end of the folder name (if: the other infos goes until the next '-' or '_' sign)
        while letter<length(folder)
            nameInfo(idx)=append(nameInfo(idx),folder(letter));
            letter=letter+1;
        end
    end
end
clear idx_newNameInfo;

% create the correct scen_opt_... variables:
for idx=1:length(nameInfo)
    if ~exist('scen_opt_1_Co2Comp') && (nameInfo(idx) == 'abroad' || nameInfo(idx) == 'domestic')
        scen_opt_1_Co2Comp = convertStringsToChars(nameInfo(idx));
    elseif nameInfo(idx) == 'no' && (nameInfo(idx+1) == 'abroad' || nameInfo(idx+2) == 'abroad')
        scen_opt_1_Co2Comp ='domestic';
    elseif nameInfo(idx) == 'high' || nameInfo(idx) == 'low'
        scen_opt_2_MarketIntegr = convertStringsToChars(nameInfo(idx));
    elseif nameInfo(idx) == 'progressive' || nameInfo(idx) == 'conservative'
        scen_opt_3_TechDev = convertStringsToChars(nameInfo(idx));

        % if not declared differently in the name, I assume:
        % - progressive scenarios does have flexibility in electric vehicles (EVflex)
        % - conservative scenarios do NOT have flexibility in electric vehicles (noEVflex)
        % this base assumptions are defined in else.
        if nameInfo(idx) == 'progressive' 
            if nameInfo(idx+1) == 'noEVflex'
                scen_opt_4_EVflex = convertStringsToChars(nameInfo(idx+1));
            else %(nameInfo(idx+1) == ''|| nameInfo(idx+1) == '1')
                scen_opt_4_EVflex = 'EVflex';
            end
        elseif nameInfo(idx) == 'conservative'
            if nameInfo(idx+1) == 'EVflex' % I have not seen conservative + EVflex yet. the possibility is implemented anyway, just in case
                scen_opt_4_EVflex = convertStringsToChars(nameInfo(idx));
            else %(nameInfo(idx+1) == ''|| nameInfo(idx+1) == '1')
                scen_opt_4_EVflex = 'noEVflex';
            end
        end
    end
end

% Check if the automatical identification did work. Otherwhise name the scenarios manueally
if ~exist('scen_opt_1_Co2Comp', 'var')||~exist('scen_opt_2_MarketIntegr','var')||~exist('scen_opt_3_TechDev','var')||~exist('scen_opt_4_EVflex','var')
    disp("The scenario could not be completely identified acording to the foldername.");
    warning("You need to define the scenario description manually!");

    % for Manual naming: run following lines, including the correct parameter:
    scen_opt_1_Co2Comp = 'abroad';   	% value = 'abroad' or 'domestic'
    scen_opt_2_MarketIntegr = 'low';
    scen_opt_3_TechDev = 'progressive';	% value = 'progressive' or 'conservative'
    scen_opt_4_EVflex = 'EVflex'; % value = 'EVflex' or 'noEVflex'
end


disp(' ');
disp('!!! Check if the automatical scenario options identification has worked correctly!!!');
disp(['scen_opt_1_Co2Comp      = ', scen_opt_1_Co2Comp]);
disp(['scen_opt_2_MarketIntegr = ', scen_opt_2_MarketIntegr]);
disp(['scen_opt_3_TechDev      = ', scen_opt_3_TechDev]);
disp(['scen_opt_4_EVHPflex     = ', scen_opt_4_EVHPflex]);


disp(' ');
if strcmp(location, 'laptop')
    disp('False: ENTER "0" if the scenario name does NOT match the imported data!')
    correct = input('True: Press Enter (or give any number >0) if the scenario name matches the imported data (folder). ');
    correct = boolean(correct);
    if ~correct
        error('Skript has been canceled (due to incorrect scenario options).');
    else
        disp('Scenario options are named correctly. Script continues...');
    end
elseif strcmp(location, 'euler')
    disp(' ')
    disp('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    disp("DON'T FORGETT TO CHECK IF THE SCENARIO OPTIONS HAVE BEEN IDENTIFIED CORRECTELY!")
    disp('the script will run anyway.')
    disp('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    disp(' ')
end
%}
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
% ... continue Data import
%^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

disp(' ');
disp('Scenario options identification:');
disp(['scen_opt_1_Co2Comp      = ', scen_opt_1_Co2Comp]);
disp(['scen_opt_2_MarketIntegr = ', scen_opt_2_MarketIntegr]);
disp(['scen_opt_3_TechDev      = ', scen_opt_3_TechDev]);
disp(['scen_opt_4_EVHPflex     = ', scen_opt_4_EVHPflex]);
disp(' ');
disp('Proceed to read Calliope datafiles...');

% create full path to datafiles
readfile = strcat(datapath,folder,'/');     % for Macs
%readfile = strcat(datapath,folder,'\');    % for Windows machines

disp(' ')
disp('=========================================================================')
disp(pwd)
disp(readfile)
disp('=========================================================================')
disp(' ')

% import all data to Matlab
%[data, meta] = datapackage('http://data.okfn.org/data/core/gdp/');
[table_data, meta_data] = datapackage_jbg(readfile);

% add path to EXPANSE profile
%path2 = '/Volumes/jaredg/02_Projects/Nexuse_Project/CH2040/';   	%FEN network drive from Jared's Mac
ReadFile2 = strcat(datapath,'CreateEmobility_DemandProfile.xlsx');
% import EXPANSE normalized profile for e-mobility (normailzed by the
% annual total, unitless)
[Expanse_emobility_normalized] = xlsread(ReadFile2,'Expanse','B2:B8761');


disp(' ')
disp(['The total execution time for pulling all data files is: ', num2str(toc(strPullData)), ' (s) '])
disp('=========================================================================')


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
% code deleted (git commit 30.5.2024)


%% Version for PATHFNDR Scenarios

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
%tables_need_list = {'flow_in','net_import','nameplate_capacity','net_transfer_capacity','names','flow_out','storage_capacity','total_system_emissions','duals'};

table_data(~ismember(table_names,tables_need_list)) = [];       % removes all rows, which are not needed (rows, where the corresbonding table name is not present in tables_need_list)
table_names(~ismember(table_names,tables_need_list)) = [];      % removes elements, which are not needed (elements, where the corresbonding table name is not present in tables_need_list)

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
    % PATHFNDR Scenarios Oct 2024 (depend on CO2 compensation, market integration, tech development, EV&HP-flexibility)
    % ------------------------------------------------
    % auto detect and set scenario name
    % First: CO2 Compensation  
    if strcmp(scen_opt_1_Co2Comp,'abroad')
            % CH is allowed to compensate abroad
            scen_opt_1_Co2Comp_name = 'CO2CompAbroad'; % value = 'CO2CompAbroad' or 'CO2CompDomest'     
    elseif strcmp(scen_opt_1_Co2Comp,'domestic')
            % CH is NOT allowed to compensate abroad
            scen_opt_1_Co2Comp_name = 'CO2CompDomest'; % value = 'CO2CompAbroad' or 'CO2CompDomest'
    else
            % throw an error message
            disp(' ')
            disp('=========================================================================')
            error('ERROR: improper CO2 compensation option defined, should be: abroad or domestic...')
    end
    
    % Second: Energy Market Integration
    if strcmp(scen_opt_2_MarketIntegr, 'high') 
        % is current grid NTCs
        scen_opt_2_MarketIntegr_name = 'MarketIntHigh';    % value = 'MarketIntHigh', 'MarketIntLow'
    elseif strcmp(scen_opt_2_MarketIntegr, 'low')
        % is reduced grid NTCs
        scen_opt_2_MarketIntegr_name = 'MarketIntLow';     % value = 'MarketIntHigh', 'MarketIntLow'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper NTC multiplier defined, should be: current or reduced or expanded...') 
    end
    
    % Third: Technology Development
    if strcmp(scen_opt_3_TechDev,'progressive')
        % 
        scen_opt_3_TechDev_name = 'TechProg'; % value = 'TechProg' or 'TechCons'
    elseif strcmp(scen_opt_3_TechDev,'conservative')
        % 
        scen_opt_3_TechDev_name = 'TechCons'; % value = 'TechProg' or 'TechCons'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper Technology Development option defined, should be: progressive or conservative...')
    end
    
    % Fourth: EV & HP Flexibility
    if strcmp(scen_opt_4_EVHPflex,'EVflex-HPflex')
        % 
        scen_opt_4_EVflex_name = 'withEVwithHPflex'; % value = 'withEVwithHPflex' or 'withEVnoHPflex' or 'noEVwithHPflex' or 'noEVnoHPflex'
    elseif strcmp(scen_opt_4_EVHPflex,'EVflex-noHPflex')
        %
        scen_opt_4_EVflex_name = 'withEVnoHPflex'; % value = 'withEVwithHPflex' or 'withEVnoHPflex' or 'noEVwithHPflex' or 'noEVnoHPflex'
    elseif strcmp(scen_opt_4_EVHPflex,'noEVflex-HPflex')
        %
        scen_opt_4_EVflex_name = 'noEVwithHPflex'; % value = 'withEVwithHPflex' or 'withEVnoHPflex' or 'noEVwithHPflex' or 'noEVnoHPflex'
    elseif strcmp(scen_opt_4_EVHPflex,'noEVflex-noHPflex')
        %
        scen_opt_4_EVflex_name = 'noEVnoHPflex'; % value = 'withEVwithHPflex' or 'withEVnoHPflex' or 'noEVwithHPflex' or 'noEVnoHPflex'
    else
        % throw an error message
        disp(' ')
        disp('=========================================================================')
        error('ERROR: improper EV & HP Flexibility option defined, should be: EVflex-HPflex or EVflex-noHPflex or noEVflex-HPflex or noEVflex-noHPflex ...')
    end
    % Set the appropriate filename
    filename = strcat('CalliopeToNexuse_PATHFNDR_',scen_opt_1_Co2Comp_name,'_',scen_opt_2_MarketIntegr_name,'_',scen_opt_3_TechDev_name,'_',scen_opt_4_EVflex_name,'.mat');
    
    % store scenario options
    CalliopeToNexuse.Scenario_current.Co2Comp       = scen_opt_1_Co2Comp_name;
    CalliopeToNexuse.Scenario_current.MarketIntegr  = scen_opt_2_MarketIntegr_name;
    CalliopeToNexuse.Scenario_current.TechDev       = scen_opt_3_TechDev_name;
    CalliopeToNexuse.Scenario_current.EVflex        = scen_opt_4_EVflex_name;
 
    
    %% Version for PATHFNDR Scenarios v2
    
    %%   
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: flow_in.csv
    %   -base electricity demands for each country, no rail (hourly)        (demand_elec, electric_heater, electric_hob)
    %   -rail electricity demands for each country (hourly)                 (demand_rail)
    %   -electrified hydrogen demands for each country (hourly)             (electrolysis, hydrogen_to_liquids)
    %   --->> EITHER the electrified hydrogen demand OR the hydrogen demand 
    %         should be forwarded to CentIv, here we still gather both
    %   -electrified mobility demands for each country (hourly)             (heavy_transport_ev, light_transport_ev)
    %   -electrified heatpump demands for each country (hourly)             (hp)
    %   -DAC electricity demands for each country (hourly)                  (dac)
    %   -electricity demand shift up for each country (hourly)              (flexibility_electricity)

    %   -hydrogen demand for the industry for each country (hourly)         (demand_industry_hydrogen)
    %   -hydrogen demand for transport for each country (hourly)            (heavy_transport_fcev, light_transport_fcev)
    %   -hydrogen demand for liquids for each country (hourly)              (hydrogen_to_liquids, hydrogen_to_methanole)
    %   -hydrogen demand for export (hourly)                                (hydrogen_distribution_export)

    %   -methane demand for the industry for each country (hourly)          (demand_industry_methane)
    %   -methane demand for residential sector for each country (hourly)    (gas_hob, methane_boiler)
    %   -methane demand for export (hourly)                                 (syn_methane_distribution_export)
    
    %   -available biofuel for gas production (annually)***                   (biofuel_to_methane)
    %   -available biogas for gas production (annually)***                    (biogas_upgrading, biogas_upgrading_ccs)

    %   -technologies, which use biofuel used for other things than gas
    %        (biofuel_boiler)               (output: heat                               (could be gathered in flowout))
    %        (biofuel_to_diesel)            (output: diesel                             (could be gathered in flowout))
    %        (biofuel_to_liquids)           (output: diesel, electricity, kerosene      (could be gathered in flowout))
    %        (biofuel_to_methanol)          (output: methanol                           (could be gathered in flowout))
    %        (chp_biofuel_extraction)       (output: electricity, heat                  (could be gathered in flowout))
    %        (chp_biofuel_extraction_ccs)   (output: electricity, heat, co2             (could be gathered in flowout))
    %   -technologies, which use biogas used for other things than gas
    %        (chp_biogas)                   (output: electricity, heat                  (could be gathered in flowout))
    %        (chp_biogas_ccs)               (output: electricity, heat, co2             (could be gathered in flowout))
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are hourly (8760 entries)
    % the values in these profiles are NOT doubled
    % these profiles depend on the scenario
    
    
    % OTHER NOTES:
    % Not included variables:
    %   -hydrogen demand for power & heating for each country (hourly) *     (chp_hydrogen)
    %   -methane demand for power & heating for each country (hourly) *      (chp_methane_extraction) Might not be implemented in GasNet yet -> we decide later if we keep the technology
    %   -(ccgt)**
    %   -(ccgt_ccs)**

    % *  neglected due to complexity to include & small contribution
    %    if combined heat-power plants should be considered one day (would be more correct)
    %    their heat and power output need to be considered!
    %    In that case: add 'chp_hydrogen' & 'chp_methane_extraction' to the
    %    H2 resp. Gas demand, in order to supply the heat demand, but also
    %    subtract the produced electricity from the electricity demand.
    %    (this amount doesn't need to be produced by another source).

    % ** because these technologies are within the scope of the P2G2P units
    %    and are therefor not included into the methane demand

    % Info: CentIv has only one hydrogen demand and one methane demand!
    %              (demands will be summed)
    %       GasNet Allowes demand variablity (eg. lower demand @high price)
    %              Price elasticities are defined for the demand sectors:
    %              civil heating, industry, mobility & agricultre.

    % *** Available biofuel and biogas to produce gas (methane resp. biomethane)
    %       Calliope does also consider other fuel types like kerosine, diesel, methanol...
    %       and biofuel and biogas can be also directly used as final energy carrier.
    %       Therefore, NOT all of the available biofuel and biogas can be used for methane production!
    %       Here the for methane production available (from Calliope dedicated) amount is captured.
    
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the electric and fuel demand profiles begins... ')
    % Initilize
    strElecLoad = tic;
    
    % detect which entry in table_data
    idx_table_flowin = find(strcmp(table_names,'flow_in'));
    %idx_table_flowout = find(strcmp(table_names,'flow_out'));
    
    %--------------------------------------------------------------------------
    % create identifiers
    %--------------------------------------------------------------------------
    
    %z1 = table_data{29}(strcmp(table_data{29}.techs,'demand_elec') & strcmp(table_data{29}.locs,'CHE'));
    
    % identify data for desired scenario
    idx_scenario1 = strcmp(table_data{idx_table_flowin}.scenario,scen_opt_1_name);
    %idx_scenario1 = strcmp(table_data{idx_table_flowin}.GTC_limit,scen_opt_1_GTClim) & table_data{idx_table_flowin}.NTC_multiplier==scen_opt_2_NTCmult & table_data{idx_table_flowin}.heat_and_transport_electrification_limit_fraction==scen_opt_3_eleclim & strcmp(table_data{idx_table_flowin}.swiss_fuel_autarky,scen_opt_4_fuelautky) & strcmp(table_data{idx_table_flowin}.swiss_net_transfer_constraint,scen_opt_5_balimpexp) ;
    
    % identify data for electricity flows
    idx_elecdem = strcmp(table_data{idx_table_flowin}.carriers,'electricity');
    % identify data for hydrogen flows for GasNet
    idx_H2dem = strcmp(table_data{idx_table_flowin}.carriers,'hydrogen');
    % identify data for methane flows for GasNet
    idx_gasdem = strcmp(table_data{idx_table_flowin}.carriers,'methane');
    % identify data for biofuel & biogas flows available for gas production for GasNet (in flow.out we could collect the produced methane (from biofuel) and biomethane (from biogas))
    biomassforgas = {'biofuel','biogas'};
    idx_biomassforgas = ismember(table_data{idx_table_flowin}.carriers,biomassforgas);

    % identify data for Base demand
    techs_base_demand = {'demand_elec','electric_heater','electric_hob'};
    idx_elecload_base = ismember(table_data{idx_table_flowin}.techs,techs_base_demand);
    % identify data for hydrogen demand
    techs_hydrogen_demand = {'electrolysis','hydrogen_to_liquids'};
    idx_elecload_hydrogen = ismember(table_data{idx_table_flowin}.techs,techs_hydrogen_demand);
    % identify data for e-mobility demand
    %techs_emobility_demand = {'heavy_transport_ev','light_transport_ev'};   % old version of Calliope EV modeling by Brynn
    techs_emobility_noflex_demand = {'heavy_duty_charging','light_duty_charging','motorcycle_charging','bus_charging'};   % new version of Calliope EV modeling by Francesco
    idx_elecload_emobility_noflex = ismember(table_data{idx_table_flowin}.techs,techs_emobility_noflex_demand);
    % identify data for e-mobility demand
    %techs_emobility_demand = {'heavy_transport_ev','light_transport_ev'};   % old version of Calliope EV modeling by Brynn
    techs_emobility_flex_demand = {'v1g_charging'};   % new version of Calliope EV modeling by Francesco
    idx_elecload_emobility_flex = ismember(table_data{idx_table_flowin}.techs,techs_emobility_flex_demand);
    % identify data for heatpump demand
    techs_heatpump_demand = {'hp'};
    idx_elecload_heatpump = ismember(table_data{idx_table_flowin}.techs,techs_heatpump_demand);
    techs_rail_demand = {'demand_rail'};
    idx_elecload_rail = ismember(table_data{idx_table_flowin}.techs,techs_rail_demand);
    techs_dac_demand = {'dac'};
    idx_elecload_dac = ismember(table_data{idx_table_flowin}.techs,techs_dac_demand);
    % also need to subtract the upward load shifting
    techs_shiftup_demand = {'flexibility_electricity'};
    idx_elecload_shiftup = ismember(table_data{idx_table_flowin}.techs,techs_shiftup_demand);

    % for GasNet: identify data for H2 demand
    % identify data for industrial H2 demand
    techs_H2industry_demand = {'demand_industry_hydrogen'};
    idx_H2load_H2industry = ismember(table_data{idx_table_flowin}.techs,techs_H2industry_demand);
    % identify data for H2 mobility demand
    techs_H2mobility_demand = {'heavy_transport_fcev','light_transport_fcev'};
    idx_H2load_H2mobility = ismember(table_data{idx_table_flowin}.techs,techs_H2mobility_demand);
    % identify data for H2 demand for liquids
    techs_H2liquids_demand = {'hydrogen_to_liquids','hydrogen_to_methanol'};
    idx_H2load_H2liquids = ismember(table_data{idx_table_flowin}.techs,techs_H2liquids_demand);
    % identify data for H2 demand for export
    techs_H2export_demand = {'hydrogen_distribution_export'};
    idx_H2load_H2export = ismember(table_data{idx_table_flowin}.techs,techs_H2export_demand);
%     % identify data for H2 power & heating demand
%     techs_H2chp_demand = {'chp_hydrogen'};
%     idx_H2load_H2chp = ismember(table_data{idx_table_flowin}.techs,techs_H2chp_demand);
%     chp deleted from the rest fo the code

    % for GasNet: identify data for gas (= methane) demand
    %identify data for industrial gas demand
    techs_gasindustry_demand = {'demand_industry_methane'};
    idx_gasload_gasindustry = ismember(table_data{idx_table_flowin}.techs,techs_gasindustry_demand);
    %identify data for residential gas demand
    techs_gasresidential_demand = {'gas_hob','methane_boiler'};
    idx_gasload_gasresidential = ismember(table_data{idx_table_flowin}.techs,techs_gasresidential_demand);
    %identify data for gas demandfor export
    techs_gasexport_demand = {'syn_methane_distribution_export'};
    idx_gasload_gasexport = ismember(table_data{idx_table_flowin}.techs,techs_gasexport_demand);
%     %identify data for gas demand for power & heating 
%     techs_gaschp_demand = {'chp_methane_extraction'};
%     idx_gasload_gaschp = ismember(table_data{idx_table_flowin}.techs,techs_gaschp_demand);
%     chp deleted from the rest fo the code
% for GasNet: identify available biofuel and biogas for gas production (methane & biomethane)
    %identify data for biofuelforgas 
    techs_biofuelforgas = {'biofuel_to_methane'};
    idx_biofuelforgas = ismember(table_data{idx_table_flowin}.techs,techs_biofuelforgas);
    %identify data for biogasforgas
    techs_biogasforgas = {'biogas_upgrading','biogas_upgrading_ccs'};
    idx_biogasforgas = ismember(table_data{idx_table_flowin}.techs,techs_biogasforgas);    

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
    [data_elecload_base_Scen1_allCH_hrly_TWh, data_elecload_hydrogen_Scen1_allCH_hrly_TWh, data_elecload_emobility_noflex_Scen1_allCH_hrly_TWh, data_elecload_emobility_flex_Scen1_allCH_hrly_TWh, data_elecload_heatpump_Scen1_allCH_hrly_TWh, data_elecload_rail_Scen1_allCH_hrly_TWh, data_elecload_dac_Scen1_allCH_hrly_TWh,data_elecload_shiftup_Scen1_allCH_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_elecdem,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility_noflex,idx_elecload_emobility_flex,idx_elecload_heatpump,idx_elecload_rail,idx_elecload_dac,idx_elecload_shiftup);
    %later when spliting eletrolysis and liquids: [data_elecload_base_Scen1_allCH_hrly_TWh, data_elecload_hydrogen1_Scen1_allCH_hrly_TWh, data_elecload_hydrogen2_Scen1_allCH_hrly_TWh, data_elecload_emobility_Scen1_allCH_hrly_TWh, data_elecload_heatpump_Scen1_allCH_hrly_TWh, data_elecload_rail_Scen1_allCH_hrly_TWh, data_elecload_dac_Scen1_allCH_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_elecdem,idx_elecload_base,idx_elecload_hydrogen1,idx_elecload_hydrogen2,idx_elecload_emobility,idx_elecload_heatpump,idx_elecload_rail,idx_elecload_dac);
    [data_H2load_industry_Scen1_allCH_hrly_TWh, data_H2load_mobility_Scen1_allCH_hrly_TWh, data_H2load_liquids_Scen1_allCH_hrly_TWh, data_H2load_export_Scen1_allCH_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_H2dem,idx_H2load_H2industry,idx_H2load_H2mobility,idx_H2load_H2liquids,idx_H2load_H2export);
    [data_gasload_industry_Scen1_allCH_hrly_TWh, data_gasload_residential_Scen1_allCH_hrly_TWh, data_gasload_export_Scen1_allCH_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_gasdem,idx_gasload_gasindustry,idx_gasload_gasresidential,idx_gasload_gasexport);
    [data_biofuelforgas_Scen1_allCH_hrly_TWh, data_biogasforgas_Scen1_allCH_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_biomassforgas,idx_biofuelforgas,idx_biogasforgas);
   
    %--------------------------------------------------------------------------
    % create hourly profiles for all DE
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allDE_hrly_TWh, data_elecload_hydrogen_Scen1_allDE_hrly_TWh, data_elecload_emobility_noflex_Scen1_allDE_hrly_TWh, data_elecload_emobility_flex_Scen1_allDE_hrly_TWh, data_elecload_heatpump_Scen1_allDE_hrly_TWh, data_elecload_rail_Scen1_allDE_hrly_TWh, data_elecload_dac_Scen1_allDE_hrly_TWh,data_elecload_shiftup_Scen1_allDE_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_germany1,idx_elecdem,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility_noflex,idx_elecload_emobility_flex,idx_elecload_heatpump,idx_elecload_rail,idx_elecload_dac,idx_elecload_shiftup);
    [data_H2load_industry_Scen1_allDE_hrly_TWh, data_H2load_mobility_Scen1_allDE_hrly_TWh, data_H2load_liquids_Scen1_allDE_hrly_TWh, data_H2load_export_Scen1_allDE_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_germany1,idx_H2dem,idx_H2load_H2industry,idx_H2load_H2mobility,idx_H2load_H2liquids,idx_H2load_H2export);
    [data_gasload_industry_Scen1_allDE_hrly_TWh, data_gasload_residential_Scen1_allDE_hrly_TWh, data_gasload_export_Scen1_allDE_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_germany1,idx_gasdem,idx_gasload_gasindustry,idx_gasload_gasresidential,idx_gasload_gasexport);
    [data_biofuelforgas_Scen1_allDE_hrly_TWh, data_biogasforgas_Scen1_allDE_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_germany1,idx_biomassforgas,idx_biofuelforgas,idx_biogasforgas);
    
    %--------------------------------------------------------------------------
    % create hourly profiles for all FR
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allFR_hrly_TWh, data_elecload_hydrogen_Scen1_allFR_hrly_TWh, data_elecload_emobility_noflex_Scen1_allFR_hrly_TWh, data_elecload_emobility_flex_Scen1_allFR_hrly_TWh, data_elecload_heatpump_Scen1_allFR_hrly_TWh, data_elecload_rail_Scen1_allFR_hrly_TWh, data_elecload_dac_Scen1_allFR_hrly_TWh,data_elecload_shiftup_Scen1_allFR_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_france1,idx_elecdem,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility_noflex,idx_elecload_emobility_flex,idx_elecload_heatpump,idx_elecload_rail,idx_elecload_dac,idx_elecload_shiftup);
    [data_H2load_industry_Scen1_allFR_hrly_TWh, data_H2load_mobility_Scen1_allFR_hrly_TWh, data_H2load_liquids_Scen1_allFR_hrly_TWh, data_H2load_export_Scen1_allFR_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_france1,idx_H2dem,idx_H2load_H2industry,idx_H2load_H2mobility,idx_H2load_H2liquids,idx_H2load_H2export);
    [data_gasload_industry_Scen1_allFR_hrly_TWh, data_gasload_residential_Scen1_allFR_hrly_TWh, data_gasload_export_Scen1_allFR_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_france1,idx_gasdem,idx_gasload_gasindustry,idx_gasload_gasresidential,idx_gasload_gasexport);
    [data_biofuelforgas_Scen1_allFR_hrly_TWh, data_biogasforgas_Scen1_allFR_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_france1,idx_biomassforgas,idx_biofuelforgas,idx_biogasforgas);
   
    %--------------------------------------------------------------------------
    % create hourly profiles for all IT
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allIT_hrly_TWh, data_elecload_hydrogen_Scen1_allIT_hrly_TWh, data_elecload_emobility_noflex_Scen1_allIT_hrly_TWh, data_elecload_emobility_flex_Scen1_allIT_hrly_TWh, data_elecload_heatpump_Scen1_allIT_hrly_TWh, data_elecload_rail_Scen1_allIT_hrly_TWh, data_elecload_dac_Scen1_allIT_hrly_TWh,data_elecload_shiftup_Scen1_allIT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_italy1,idx_elecdem,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility_noflex,idx_elecload_emobility_flex,idx_elecload_heatpump,idx_elecload_rail,idx_elecload_dac,idx_elecload_shiftup);
    [data_H2load_industry_Scen1_allIT_hrly_TWh, data_H2load_mobility_Scen1_allIT_hrly_TWh, data_H2load_liquids_Scen1_allIT_hrly_TWh, data_H2load_export_Scen1_allIT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_italy1,idx_H2dem,idx_H2load_H2industry,idx_H2load_H2mobility,idx_H2load_H2liquids,idx_H2load_H2export);
    [data_gasload_industry_Scen1_allIT_hrly_TWh, data_gasload_residential_Scen1_allIT_hrly_TWh, data_gasload_export_Scen1_allIT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_italy1,idx_gasdem,idx_gasload_gasindustry,idx_gasload_gasresidential,idx_gasload_gasexport);
    [data_biofuelforgas_Scen1_allIT_hrly_TWh, data_biogasforgas_Scen1_allIT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_italy1,idx_biomassforgas,idx_biofuelforgas,idx_biogasforgas);
   
    %--------------------------------------------------------------------------
    % create hourly profiles for all AT
    %--------------------------------------------------------------------------
    
    % use function to add hourly profiles for each demand type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [data_elecload_base_Scen1_allAT_hrly_TWh, data_elecload_hydrogen_Scen1_allAT_hrly_TWh, data_elecload_emobility_noflex_Scen1_allAT_hrly_TWh, data_elecload_emobility_flex_Scen1_allAT_hrly_TWh, data_elecload_heatpump_Scen1_allAT_hrly_TWh, data_elecload_rail_Scen1_allAT_hrly_TWh, data_elecload_dac_Scen1_allAT_hrly_TWh,data_elecload_shiftup_Scen1_allAT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_austria1,idx_elecdem,idx_elecload_base,idx_elecload_hydrogen,idx_elecload_emobility_noflex,idx_elecload_emobility_flex,idx_elecload_heatpump,idx_elecload_rail,idx_elecload_dac,idx_elecload_shiftup);
    [data_H2load_industry_Scen1_allAT_hrly_TWh, data_H2load_mobility_Scen1_allAT_hrly_TWh, data_H2load_liquids_Scen1_allAT_hrly_TWh, data_H2load_export_Scen1_allAT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_austria1,idx_H2dem,idx_H2load_H2industry,idx_H2load_H2mobility,idx_H2load_H2liquids,idx_H2load_H2export);
    [data_gasload_industry_Scen1_allAT_hrly_TWh, data_gasload_residential_Scen1_allAT_hrly_TWh, data_gasload_export_Scen1_allAT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_austria1,idx_gasdem,idx_gasload_gasindustry,idx_gasload_gasresidential,idx_gasload_gasexport);
    [data_biofuelforgas_Scen1_allAT_hrly_TWh, data_biogasforgas_Scen1_allAT_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_austria1,idx_biomassforgas,idx_biofuelforgas,idx_biogasforgas);

    
    
    %--------------------------------------------------------------------------
    % CH,DE,FR,IT,AT convert units to MWh and round to nearest MWh
    %--------------------------------------------------------------------------
    % CH
    [data_elecload_base_Scen1_CHhourly_MWh, data_elecload_hydrogen_Scen1_CHhourly_MWh, data_elecload_emobility_noflex_Scen1_CHhourly_MWh, data_elecload_emobility_flex_Scen1_CHhourly_MWh, data_elecload_heatpump_Scen1_CHhourly_MWh, data_elecload_rail_Scen1_CHhourly_MWh, data_elecload_dac_Scen1_CHhourly_MWh, data_elecload_shiftup_Scen1_CHhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allCH_hrly_TWh, data_elecload_hydrogen_Scen1_allCH_hrly_TWh, data_elecload_emobility_noflex_Scen1_allCH_hrly_TWh, data_elecload_emobility_flex_Scen1_allCH_hrly_TWh, data_elecload_heatpump_Scen1_allCH_hrly_TWh, data_elecload_rail_Scen1_allCH_hrly_TWh, data_elecload_dac_Scen1_allCH_hrly_TWh,data_elecload_shiftup_Scen1_allCH_hrly_TWh);
    [data_H2load_industry_Scen1_CHhourly_MWh, data_H2load_mobility_Scen1_CHhourly_MWh, data_H2load_liquids_Scen1_CHhourly_MWh, data_H2load_export_Scen1_CHhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_H2load_industry_Scen1_allCH_hrly_TWh,data_H2load_mobility_Scen1_allCH_hrly_TWh,data_H2load_liquids_Scen1_allCH_hrly_TWh,data_H2load_export_Scen1_allCH_hrly_TWh);
    [data_gasload_industry_Scen1_CHhourly_MWh, data_gasload_residential_Scen1_CHhourly_MWh, data_gasload_export_Scen1_CHhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_gasload_industry_Scen1_allCH_hrly_TWh, data_gasload_residential_Scen1_allCH_hrly_TWh, data_gasload_export_Scen1_allCH_hrly_TWh);
    [data_biofuelforgas_Scen1_CHsum_MWh, data_biogasforgas_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_biofuelforgas_Scen1_allCH_hrly_TWh, data_biogasforgas_Scen1_allCH_hrly_TWh);

    % DE
    [data_elecload_base_Scen1_DEhourly_MWh, data_elecload_hydrogen_Scen1_DEhourly_MWh, data_elecload_emobility_noflex_Scen1_DEhourly_MWh, data_elecload_emobility_flex_Scen1_DEhourly_MWh, data_elecload_heatpump_Scen1_DEhourly_MWh, data_elecload_rail_Scen1_DEhourly_MWh, data_elecload_dac_Scen1_DEhourly_MWh, data_elecload_shiftup_Scen1_DEhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allDE_hrly_TWh, data_elecload_hydrogen_Scen1_allDE_hrly_TWh, data_elecload_emobility_noflex_Scen1_allDE_hrly_TWh, data_elecload_emobility_flex_Scen1_allDE_hrly_TWh, data_elecload_heatpump_Scen1_allDE_hrly_TWh, data_elecload_rail_Scen1_allDE_hrly_TWh, data_elecload_dac_Scen1_allDE_hrly_TWh,data_elecload_shiftup_Scen1_allDE_hrly_TWh);
    [data_H2load_industry_Scen1_DEhourly_MWh, data_H2load_mobility_Scen1_DEhourly_MWh, data_H2load_liquids_Scen1_DEhourly_MWh, data_H2load_export_Scen1_DEhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_H2load_industry_Scen1_allDE_hrly_TWh,data_H2load_mobility_Scen1_allDE_hrly_TWh,data_H2load_liquids_Scen1_allDE_hrly_TWh,data_H2load_export_Scen1_allDE_hrly_TWh);
    [data_gasload_industry_Scen1_DEhourly_MWh, data_gasload_residential_Scen1_DEhourly_MWh, data_gasload_export_Scen1_DEhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_gasload_industry_Scen1_allDE_hrly_TWh, data_gasload_residential_Scen1_allDE_hrly_TWh, data_gasload_export_Scen1_allDE_hrly_TWh);
    % change CH [data_biofuelforgas_Scen1_CHsum_MWh, data_biogasforgas_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_biofuelforgas_Scen1_allCH_hrly_TWh, data_biogasforgas_Scen1_allCH_hrly_TWh);

    % FR
    [data_elecload_base_Scen1_FRhourly_MWh, data_elecload_hydrogen_Scen1_FRhourly_MWh, data_elecload_emobility_noflex_Scen1_FRhourly_MWh, data_elecload_emobility_flex_Scen1_FRhourly_MWh, data_elecload_heatpump_Scen1_FRhourly_MWh, data_elecload_rail_Scen1_FRhourly_MWh, data_elecload_dac_Scen1_FRhourly_MWh, data_elecload_shiftup_Scen1_FRhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allFR_hrly_TWh, data_elecload_hydrogen_Scen1_allFR_hrly_TWh, data_elecload_emobility_noflex_Scen1_allFR_hrly_TWh, data_elecload_emobility_flex_Scen1_allFR_hrly_TWh, data_elecload_heatpump_Scen1_allFR_hrly_TWh, data_elecload_rail_Scen1_allFR_hrly_TWh, data_elecload_dac_Scen1_allFR_hrly_TWh,data_elecload_shiftup_Scen1_allFR_hrly_TWh);
    [data_H2load_industry_Scen1_FRhourly_MWh, data_H2load_mobility_Scen1_FRhourly_MWh, data_H2load_liquids_Scen1_FRhourly_MWh, data_H2load_export_Scen1_FRhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_H2load_industry_Scen1_allFR_hrly_TWh,data_H2load_mobility_Scen1_allFR_hrly_TWh,data_H2load_liquids_Scen1_allFR_hrly_TWh,data_H2load_export_Scen1_allFR_hrly_TWh);
    [data_gasload_industry_Scen1_FRhourly_MWh, data_gasload_residential_Scen1_FRhourly_MWh, data_gasload_export_Scen1_FRhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_gasload_industry_Scen1_allFR_hrly_TWh, data_gasload_residential_Scen1_allFR_hrly_TWh, data_gasload_export_Scen1_allFR_hrly_TWh);
    % change CH [data_biofuelforgas_Scen1_CHsum_MWh, data_biogasforgas_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_biofuelforgas_Scen1_allCH_hrly_TWh, data_biogasforgas_Scen1_allCH_hrly_TWh);

    % IT
    [data_elecload_base_Scen1_IThourly_MWh, data_elecload_hydrogen_Scen1_IThourly_MWh, data_elecload_emobility_noflex_Scen1_IThourly_MWh, data_elecload_emobility_flex_Scen1_IThourly_MWh, data_elecload_heatpump_Scen1_IThourly_MWh, data_elecload_rail_Scen1_IThourly_MWh, data_elecload_dac_Scen1_IThourly_MWh, data_elecload_shiftup_Scen1_IThourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allIT_hrly_TWh, data_elecload_hydrogen_Scen1_allIT_hrly_TWh, data_elecload_emobility_noflex_Scen1_allIT_hrly_TWh, data_elecload_emobility_flex_Scen1_allIT_hrly_TWh, data_elecload_heatpump_Scen1_allIT_hrly_TWh, data_elecload_rail_Scen1_allIT_hrly_TWh, data_elecload_dac_Scen1_allIT_hrly_TWh,data_elecload_shiftup_Scen1_allIT_hrly_TWh);
    [data_H2load_industry_Scen1_IThourly_MWh, data_H2load_mobility_Scen1_IThourly_MWh, data_H2load_liquids_Scen1_IThourly_MWh, data_H2load_export_Scen1_IThourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_H2load_industry_Scen1_allIT_hrly_TWh,data_H2load_mobility_Scen1_allIT_hrly_TWh,data_H2load_liquids_Scen1_allIT_hrly_TWh,data_H2load_export_Scen1_allIT_hrly_TWh);
    [data_gasload_industry_Scen1_IThourly_MWh, data_gasload_residential_Scen1_IThourly_MWh, data_gasload_export_Scen1_IThourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_gasload_industry_Scen1_allIT_hrly_TWh, data_gasload_residential_Scen1_allIT_hrly_TWh, data_gasload_export_Scen1_allIT_hrly_TWh);
    % change CH [data_biofuelforgas_Scen1_CHsum_MWh, data_biogasforgas_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_biofuelforgas_Scen1_allCH_hrly_TWh, data_biogasforgas_Scen1_allCH_hrly_TWh);

    % AT
    [data_elecload_base_Scen1_AThourly_MWh, data_elecload_hydrogen_Scen1_AThourly_MWh, data_elecload_emobility_noflex_Scen1_AThourly_MWh, data_elecload_emobility_flex_Scen1_AThourly_MWh, data_elecload_heatpump_Scen1_AThourly_MWh, data_elecload_rail_Scen1_AThourly_MWh, data_elecload_dac_Scen1_AThourly_MWh, data_elecload_shiftup_Scen1_AThourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_elecload_base_Scen1_allAT_hrly_TWh, data_elecload_hydrogen_Scen1_allAT_hrly_TWh, data_elecload_emobility_noflex_Scen1_allAT_hrly_TWh, data_elecload_emobility_flex_Scen1_allAT_hrly_TWh, data_elecload_heatpump_Scen1_allAT_hrly_TWh, data_elecload_rail_Scen1_allAT_hrly_TWh, data_elecload_dac_Scen1_allAT_hrly_TWh,data_elecload_shiftup_Scen1_allAT_hrly_TWh);
    [data_H2load_industry_Scen1_AThourly_MWh, data_H2load_mobility_Scen1_AThourly_MWh, data_H2load_liquids_Scen1_AThourly_MWh, data_H2load_export_Scen1_AThourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_H2load_industry_Scen1_allAT_hrly_TWh,data_H2load_mobility_Scen1_allAT_hrly_TWh,data_H2load_liquids_Scen1_allAT_hrly_TWh,data_H2load_export_Scen1_allAT_hrly_TWh);
    [data_gasload_industry_Scen1_AThourly_MWh, data_gasload_residential_Scen1_AThourly_MWh, data_gasload_export_Scen1_AThourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_gasload_industry_Scen1_allAT_hrly_TWh, data_gasload_residential_Scen1_allAT_hrly_TWh, data_gasload_export_Scen1_allAT_hrly_TWh);
    % change CH [data_biofuelforgas_Scen1_CHsum_MWh, data_biogasforgas_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands(data_biofuelforgas_Scen1_allCH_hrly_TWh, data_biogasforgas_Scen1_allCH_hrly_TWh);

    %--------------------------------------------------------------------------
    % create different e-mobility profile using EXPANSE normalized profile
    %--------------------------------------------------------------------------
    
    % use the annual total for each country and the Expanse normalized profile
    % create a new version of the e-mobility demand profile
    % also round to the nearest MWh
    data_elecload_emobility_noflex_Scen1_CHhourly_MWh_EXP = round(sum(data_elecload_emobility_noflex_Scen1_CHhourly_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_noflex_Scen1_DEhourly_MWh_EXP = round(sum(data_elecload_emobility_noflex_Scen1_DEhourly_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_noflex_Scen1_FRhourly_MWh_EXP = round(sum(data_elecload_emobility_noflex_Scen1_FRhourly_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_noflex_Scen1_IThourly_MWh_EXP = round(sum(data_elecload_emobility_noflex_Scen1_IThourly_MWh)*Expanse_emobility_normalized,0);
    data_elecload_emobility_noflex_Scen1_AThourly_MWh_EXP = round(sum(data_elecload_emobility_noflex_Scen1_AThourly_MWh)*Expanse_emobility_normalized,0);
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Base Demand)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.BaseElecDemand_hrly.CH 	= data_elecload_base_Scen1_CHhourly_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.DE 	= data_elecload_base_Scen1_DEhourly_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.FR 	= data_elecload_base_Scen1_FRhourly_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.IT 	= data_elecload_base_Scen1_IThourly_MWh;
    CalliopeToNexuse.BaseElecDemand_hrly.AT 	= data_elecload_base_Scen1_AThourly_MWh;
    %CalliopeToNexuse.BaseElecDemand_hrly.EU 	= data_elecload_base_Scen1_EUsum_fullyr;    % this was from CH2040 datafiles    
    % annual
    CalliopeToNexuse.BaseElecDemand_yrly.CH 	= sum(data_elecload_base_Scen1_CHhourly_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.DE 	= sum(data_elecload_base_Scen1_DEhourly_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.FR 	= sum(data_elecload_base_Scen1_FRhourly_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.IT 	= sum(data_elecload_base_Scen1_IThourly_MWh);
    CalliopeToNexuse.BaseElecDemand_yrly.AT 	= sum(data_elecload_base_Scen1_AThourly_MWh);
    %CalliopeToNexuse.BaseElecDemand_yrly.EU 	= sum(data_elecload_base_Scen1_EUsum_fullyr);    % this was from CH2040 datafiles
    % units
    CalliopeToNexuse.Units.BaseElecDemand = ('MWh');
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Rail Demand)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.RailElecDemand_hrly.CH 	= data_elecload_rail_Scen1_CHhourly_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.DE 	= data_elecload_rail_Scen1_DEhourly_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.FR 	= data_elecload_rail_Scen1_FRhourly_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.IT 	= data_elecload_rail_Scen1_IThourly_MWh;
    CalliopeToNexuse.RailElecDemand_hrly.AT 	= data_elecload_rail_Scen1_AThourly_MWh;    
    % annual
    CalliopeToNexuse.RailElecDemand_yrly.CH 	= sum(data_elecload_rail_Scen1_CHhourly_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.DE 	= sum(data_elecload_rail_Scen1_DEhourly_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.FR 	= sum(data_elecload_rail_Scen1_FRhourly_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.IT 	= sum(data_elecload_rail_Scen1_IThourly_MWh);
    CalliopeToNexuse.RailElecDemand_yrly.AT 	= sum(data_elecload_rail_Scen1_AThourly_MWh);
    % units
    CalliopeToNexuse.Units.RailElecDemand = ('MWh');
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (DAC Demand)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.DacElecDemand_hrly.CH 	= data_elecload_dac_Scen1_CHhourly_MWh;
    CalliopeToNexuse.DacElecDemand_hrly.DE 	= data_elecload_dac_Scen1_DEhourly_MWh;
    CalliopeToNexuse.DacElecDemand_hrly.FR 	= data_elecload_dac_Scen1_FRhourly_MWh;
    CalliopeToNexuse.DacElecDemand_hrly.IT 	= data_elecload_dac_Scen1_IThourly_MWh;
    CalliopeToNexuse.DacElecDemand_hrly.AT 	= data_elecload_dac_Scen1_AThourly_MWh;    
    % annual
    CalliopeToNexuse.DacElecDemand_yrly.CH 	= sum(data_elecload_dac_Scen1_CHhourly_MWh);
    CalliopeToNexuse.DacElecDemand_yrly.DE 	= sum(data_elecload_dac_Scen1_DEhourly_MWh);
    CalliopeToNexuse.DacElecDemand_yrly.FR 	= sum(data_elecload_dac_Scen1_FRhourly_MWh);
    CalliopeToNexuse.DacElecDemand_yrly.IT 	= sum(data_elecload_dac_Scen1_IThourly_MWh);
    CalliopeToNexuse.DacElecDemand_yrly.AT 	= sum(data_elecload_dac_Scen1_AThourly_MWh);
    % units
    CalliopeToNexuse.Units.DacElecDemand = ('MWh');
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Demand Shift Up)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.ShiftUpElecDemand_hrly.CH 	= data_elecload_shiftup_Scen1_CHhourly_MWh;
    CalliopeToNexuse.ShiftUpElecDemand_hrly.DE 	= data_elecload_shiftup_Scen1_DEhourly_MWh;
    CalliopeToNexuse.ShiftUpElecDemand_hrly.FR 	= data_elecload_shiftup_Scen1_FRhourly_MWh;
    CalliopeToNexuse.ShiftUpElecDemand_hrly.IT 	= data_elecload_shiftup_Scen1_IThourly_MWh;
    CalliopeToNexuse.ShiftUpElecDemand_hrly.AT 	= data_elecload_shiftup_Scen1_AThourly_MWh;    
    % annual
    CalliopeToNexuse.ShiftUpElecDemand_yrly.CH 	= sum(data_elecload_shiftup_Scen1_CHhourly_MWh);
    CalliopeToNexuse.ShiftUpElecDemand_yrly.DE 	= sum(data_elecload_shiftup_Scen1_DEhourly_MWh);
    CalliopeToNexuse.ShiftUpElecDemand_yrly.FR 	= sum(data_elecload_shiftup_Scen1_FRhourly_MWh);
    CalliopeToNexuse.ShiftUpElecDemand_yrly.IT 	= sum(data_elecload_shiftup_Scen1_IThourly_MWh);
    CalliopeToNexuse.ShiftUpElecDemand_yrly.AT 	= sum(data_elecload_shiftup_Scen1_AThourly_MWh);
    % units
    CalliopeToNexuse.Units.ShiftUpElecDemand = ('MWh');
    
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Electrified Demands)
    %--------------------------------------------------------------------------
    % units
    CalliopeToNexuse.Units.ElectrifiedDemands = ('MWh');
    
    % next save the electrified demand profiles (hourly)
    % CH
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen                    = data_elecload_hydrogen_Scen1_CHhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_noflex_Calliope	= data_elecload_emobility_noflex_Scen1_CHhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_noflex_Expanse	= data_elecload_emobility_noflex_Scen1_CHhourly_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_flex_Calliope 	= data_elecload_emobility_flex_Scen1_CHhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump                  	= data_elecload_heatpump_Scen1_CHhourly_MWh;
    % DE
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_hydrogen                    = data_elecload_hydrogen_Scen1_DEhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_noflex_Calliope 	= data_elecload_emobility_noflex_Scen1_DEhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_noflex_Expanse 	= data_elecload_emobility_noflex_Scen1_DEhourly_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_emobility_flex_Calliope 	= data_elecload_emobility_flex_Scen1_DEhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.DE_heatpump                    = data_elecload_heatpump_Scen1_DEhourly_MWh;
    % FR
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_hydrogen                    = data_elecload_hydrogen_Scen1_FRhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_noflex_Calliope   = data_elecload_emobility_noflex_Scen1_FRhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_noflex_Expanse    = data_elecload_emobility_noflex_Scen1_FRhourly_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_emobility_flex_Calliope 	= data_elecload_emobility_flex_Scen1_FRhourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.FR_heatpump                    = data_elecload_heatpump_Scen1_FRhourly_MWh;
    % IT
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_hydrogen                    = data_elecload_hydrogen_Scen1_IThourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_noflex_Calliope 	= data_elecload_emobility_noflex_Scen1_IThourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_noflex_Expanse    = data_elecload_emobility_noflex_Scen1_IThourly_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_emobility_flex_Calliope 	= data_elecload_emobility_flex_Scen1_IThourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.IT_heatpump                    = data_elecload_heatpump_Scen1_IThourly_MWh;
    % AT
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_hydrogen                    = data_elecload_hydrogen_Scen1_AThourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_noflex_Calliope 	= data_elecload_emobility_noflex_Scen1_AThourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_noflex_Expanse    = data_elecload_emobility_noflex_Scen1_AThourly_MWh_EXP;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_emobility_flex_Calliope 	= data_elecload_emobility_flex_Scen1_AThourly_MWh;
    CalliopeToNexuse.ElectrifiedDemands_hrly.AT_heatpump                    = data_elecload_heatpump_Scen1_AThourly_MWh;
    
    % next save the electrified demand profiles (annual)
    % CH
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_hydrogen                    = sum(data_elecload_hydrogen_Scen1_CHhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_noflex_Calliope	= sum(data_elecload_emobility_noflex_Scen1_CHhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_noflex_Expanse	= sum(data_elecload_emobility_noflex_Scen1_CHhourly_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_emobility_flex_Calliope     = sum(data_elecload_emobility_flex_Scen1_CHhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.CH_heatpump                    = sum(data_elecload_heatpump_Scen1_CHhourly_MWh);
    % DE
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_hydrogen                    = sum(data_elecload_hydrogen_Scen1_DEhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_noflex_Calliope 	= sum(data_elecload_emobility_noflex_Scen1_DEhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_noflex_Expanse  	= sum(data_elecload_emobility_noflex_Scen1_DEhourly_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_emobility_flex_Calliope     = sum(data_elecload_emobility_flex_Scen1_DEhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.DE_heatpump                    = sum(data_elecload_heatpump_Scen1_DEhourly_MWh);
    % FR
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_hydrogen                    = sum(data_elecload_hydrogen_Scen1_FRhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_noflex_Calliope	= sum(data_elecload_emobility_noflex_Scen1_FRhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_noflex_Expanse 	= sum(data_elecload_emobility_noflex_Scen1_FRhourly_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_emobility_flex_Calliope     = sum(data_elecload_emobility_flex_Scen1_FRhourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.FR_heatpump                    = sum(data_elecload_heatpump_Scen1_FRhourly_MWh);
    % IT
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_hydrogen                    = sum(data_elecload_hydrogen_Scen1_IThourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_noflex_Calliope 	= sum(data_elecload_emobility_noflex_Scen1_IThourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_noflex_Expanse	= sum(data_elecload_emobility_noflex_Scen1_IThourly_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_emobility_flex_Calliope     = sum(data_elecload_emobility_flex_Scen1_IThourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.IT_heatpump                    = sum(data_elecload_heatpump_Scen1_IThourly_MWh);
    % AT
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_hydrogen                    = sum(data_elecload_hydrogen_Scen1_AThourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_noflex_Calliope 	= sum(data_elecload_emobility_noflex_Scen1_AThourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_noflex_Expanse	= sum(data_elecload_emobility_noflex_Scen1_AThourly_MWh_EXP);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_emobility_flex_Calliope     = sum(data_elecload_emobility_flex_Scen1_AThourly_MWh);
    CalliopeToNexuse.ElectrifiedDemands_yrly.AT_heatpump                    = sum(data_elecload_heatpump_Scen1_AThourly_MWh);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation (Hydrogen Demand)
    %--------------------------------------------------------------------------

    % hourly

    % CH
    CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.CH   = data_H2load_industry_Scen1_CHhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.CH   = data_H2load_mobility_Scen1_CHhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.CH    = data_H2load_liquids_Scen1_CHhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.exportH2Demand_hrly.CH     = data_H2load_export_Scen1_CHhourly_MWh;
    % DE
    CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.DE   = data_H2load_industry_Scen1_DEhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.DE   = data_H2load_mobility_Scen1_DEhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.DE    = data_H2load_liquids_Scen1_DEhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.exportH2Demand_hrly.DE    = data_H2load_export_Scen1_DEhourly_MWh;
    % FR
    CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.FR   = data_H2load_industry_Scen1_FRhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.FR   = data_H2load_mobility_Scen1_FRhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.FR    = data_H2load_liquids_Scen1_FRhourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.exportH2Demand_hrly.FR    = data_H2load_export_Scen1_FRhourly_MWh;
    % IT
    CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.IT   = data_H2load_industry_Scen1_IThourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.IT   = data_H2load_mobility_Scen1_IThourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.IT    = data_H2load_liquids_Scen1_IThourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.exportH2Demand_hrly.IT    = data_H2load_export_Scen1_IThourly_MWh;
    % AT
    CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.AT   = data_H2load_industry_Scen1_AThourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.AT   = data_H2load_mobility_Scen1_AThourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.AT    = data_H2load_liquids_Scen1_AThourly_MWh;
    CalliopeToNexuse.H2Demand_hrly.exportH2Demand_hrly.AT    = data_H2load_export_Scen1_AThourly_MWh;

    % hourly total hydrogen demand (without export):
    % CH
    CalliopeToNexuse.H2Demand_hrly.totalH2Demand_hrly.CH = CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.CH + CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.CH + CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.CH;
    % DE
    CalliopeToNexuse.H2Demand_hrly.totalH2Demand_hrly.DE = CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.DE + CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.DE + CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.DE;
    % FR
    CalliopeToNexuse.H2Demand_hrly.totalH2Demand_hrly.FR = CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.FR + CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.FR + CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.FR;
    % IT
    CalliopeToNexuse.H2Demand_hrly.totalH2Demand_hrly.IT = CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.IT + CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.IT + CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.IT;
    % AT
    CalliopeToNexuse.H2Demand_hrly.totalH2Demand_hrly.AT = CalliopeToNexuse.H2Demand_hrly.industryH2Demand_hrly.AT + CalliopeToNexuse.H2Demand_hrly.mobilityH2Demand_hrly.AT + CalliopeToNexuse.H2Demand_hrly.liquidsH2Demand_hrly.AT;


    % annual:

    % CH
    CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.CH   = sum(data_H2load_industry_Scen1_CHhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.CH   = sum(data_H2load_mobility_Scen1_CHhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.CH    = sum(data_H2load_liquids_Scen1_CHhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.exportH2Demand_yrly.CH    = sum(data_H2load_export_Scen1_CHhourly_MWh);
    % DE
    CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.DE   = sum(data_H2load_industry_Scen1_DEhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.DE   = sum(data_H2load_mobility_Scen1_DEhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.DE    = sum(data_H2load_liquids_Scen1_DEhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.exportH2Demand_yrly.DE    = sum(data_H2load_export_Scen1_DEhourly_MWh);
    % FR
    CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.FR   = sum(data_H2load_industry_Scen1_FRhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.FR   = sum(data_H2load_mobility_Scen1_FRhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.FR    = sum(data_H2load_liquids_Scen1_FRhourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.exportH2Demand_yrly.FR    = sum(data_H2load_export_Scen1_FRhourly_MWh);
    % IT
    CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.IT   = sum(data_H2load_industry_Scen1_IThourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.IT   = sum(data_H2load_mobility_Scen1_IThourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.IT    = sum(data_H2load_liquids_Scen1_IThourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.exportH2Demand_yrly.IT    = sum(data_H2load_export_Scen1_IThourly_MWh);
    % AT
    CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.AT   = sum(data_H2load_industry_Scen1_AThourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.AT   = sum(data_H2load_mobility_Scen1_AThourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.AT    = sum(data_H2load_liquids_Scen1_AThourly_MWh);
    CalliopeToNexuse.H2Demand_yrly.exportH2Demand_yrly.AT    = sum(data_H2load_export_Scen1_AThourly_MWh);

    % annual total hydrogen demand (without export):
    % CH
    CalliopeToNexuse.H2Demand_yrly.totalH2Demand_yrly.CH = CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.CH + CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.CH + CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.CH;
    % DE
    CalliopeToNexuse.H2Demand_yrly.totalH2Demand_yrly.DE = CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.DE + CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.DE + CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.DE;
    % FR
    CalliopeToNexuse.H2Demand_yrly.totalH2Demand_yrly.FR = CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.FR + CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.FR + CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.FR;
    % IT
    CalliopeToNexuse.H2Demand_yrly.totalH2Demand_yrly.IT = CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.IT + CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.IT + CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.IT;
    % AT
    CalliopeToNexuse.H2Demand_yrly.totalH2Demand_yrly.AT = CalliopeToNexuse.H2Demand_yrly.industryH2Demand_yrly.AT + CalliopeToNexuse.H2Demand_yrly.mobilityH2Demand_yrly.AT + CalliopeToNexuse.H2Demand_yrly.liquidsH2Demand_yrly.AT;

    % units
    CalliopeToNexuse.Units.industryH2Demand = ('MWh');
    CalliopeToNexuse.Units.mobilityH2Demand = ('MWh');
    CalliopeToNexuse.Units.liquidsH2Demand = ('MWh');
    CalliopeToNexuse.Units.exportH2Demand = ('MWh');
    CalliopeToNexuse.Units.totalH2Demand = ('MWh');


    %--------------------------------------------------------------------------
    % save processed data for DBcreation (methane Demand)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.CH 	= data_gasload_industry_Scen1_CHhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.DE 	= data_gasload_industry_Scen1_DEhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.FR 	= data_gasload_industry_Scen1_FRhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.IT 	= data_gasload_industry_Scen1_IThourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.AT 	= data_gasload_industry_Scen1_AThourly_MWh;

    CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.CH 	= data_gasload_residential_Scen1_CHhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.DE 	= data_gasload_residential_Scen1_DEhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.FR 	= data_gasload_residential_Scen1_FRhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.IT 	= data_gasload_residential_Scen1_IThourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.AT 	= data_gasload_residential_Scen1_AThourly_MWh;

    CalliopeToNexuse.gasDemand_hrly.exportgasDemand_hrly.CH 	= data_gasload_export_Scen1_CHhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.exportgasDemand_hrly.DE 	= data_gasload_export_Scen1_DEhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.exportgasDemand_hrly.FR 	= data_gasload_export_Scen1_FRhourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.exportgasDemand_hrly.IT 	= data_gasload_export_Scen1_IThourly_MWh;
    CalliopeToNexuse.gasDemand_hrly.exportgasDemand_hrly.AT 	= data_gasload_export_Scen1_AThourly_MWh;

    % hourly total gas demand (without export):
    % CH
    CalliopeToNexuse.gasDemand_hrly.totalgasDemand_hrly.CH = CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.CH + CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.CH;
    % DE
    CalliopeToNexuse.gasDemand_hrly.totalgasDemand_hrly.DE = CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.DE + CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.DE;
    % FR
    CalliopeToNexuse.gasDemand_hrly.totalgasDemand_hrly.FR = CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.FR + CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.FR;
    % IT
    CalliopeToNexuse.gasDemand_hrly.totalgasDemand_hrly.IT = CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.IT + CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.IT;
    % AT
    CalliopeToNexuse.gasDemand_hrly.totalgasDemand_hrly.AT = CalliopeToNexuse.gasDemand_hrly.industrygasDemand_hrly.AT + CalliopeToNexuse.gasDemand_hrly.residentialgasDemand_hrly.AT;


    % annual
    CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.CH 	= sum(data_gasload_industry_Scen1_CHhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.DE 	= sum(data_gasload_industry_Scen1_DEhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.FR 	= sum(data_gasload_industry_Scen1_FRhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.IT 	= sum(data_gasload_industry_Scen1_IThourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.AT 	= sum(data_gasload_industry_Scen1_AThourly_MWh);

    CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.CH 	= sum(data_gasload_residential_Scen1_CHhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.DE 	= sum(data_gasload_residential_Scen1_DEhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.FR 	= sum(data_gasload_residential_Scen1_FRhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.IT 	= sum(data_gasload_residential_Scen1_IThourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.AT 	= sum(data_gasload_residential_Scen1_AThourly_MWh);

    CalliopeToNexuse.gasDemand_yrly.exportgasDemand_yrly.CH 	= sum(data_gasload_export_Scen1_CHhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.exportgasDemand_yrly.DE 	= sum(data_gasload_export_Scen1_DEhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.exportgasDemand_yrly.FR 	= sum(data_gasload_export_Scen1_FRhourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.exportgasDemand_yrly.IT 	= sum(data_gasload_export_Scen1_IThourly_MWh);
    CalliopeToNexuse.gasDemand_yrly.exportgasDemand_yrly.AT 	= sum(data_gasload_export_Scen1_AThourly_MWh);

    % annual total gas demand (without export):
    % CH
    CalliopeToNexuse.gasDemand_yrly.totalgasDemand_yrly.CH = CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.CH + CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.CH;
    % DE
    CalliopeToNexuse.gasDemand_yrly.totalgasDemand_yrly.DE = CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.DE + CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.DE;
    % FR
    CalliopeToNexuse.gasDemand_yrly.totalgasDemand_yrly.FR = CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.FR + CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.FR;
    % IT
    CalliopeToNexuse.gasDemand_yrly.totalgasDemand_yrly.IT = CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.IT + CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.IT;
    % AT
    CalliopeToNexuse.gasDemand_yrly.totalgasDemand_yrly.AT = CalliopeToNexuse.gasDemand_yrly.industrygasDemand_yrly.AT + CalliopeToNexuse.gasDemand_yrly.residentialgasDemand_yrly.AT;


    % units
    CalliopeToNexuse.Units.industrygasDemand = ('MWh');
    CalliopeToNexuse.Units.residentialgasDemand = ('MWh');
    CalliopeToNexuse.Units.exportgasDemand = ('MWh');
    CalliopeToNexuse.Units.totalgasDemand = ('MWh');


    %--------------------------------------------------------------------------
    % save processed data for DBcreation (available biofuel and biogas for gas production)
    %--------------------------------------------------------------------------
    % hourly
    CalliopeToNexuse.gasDemand_hrly.biofuel_forgas_hrly.CH 	= data_biofuelforgas_Scen1_CHsum_MWh;
    CalliopeToNexuse.gasDemand_hrly.biogas_forgas_hrly.CH 	    = data_biogasforgas_Scen1_CHsum_MWh;
    % annual
    CalliopeToNexuse.gasDemand_yrly.biofuel_forgas_yrly.CH 	= sum(data_biofuelforgas_Scen1_CHsum_MWh);
    CalliopeToNexuse.gasDemand_yrly.biogas_forgas_yrly.CH 	    = sum(data_biogasforgas_Scen1_CHsum_MWh);
    % units
    CalliopeToNexuse.Units.biofuel_forgas = ('MWh');
    CalliopeToNexuse.Units.biogas_forgas = ('MWh');

    %% do all steps for the rest of the biofuel used for other things than gas

    %identify data for biofuel_boiler
    techs_biofuel_boiler = {'biofuel_boiler'};
    idx_biofuel_boiler = ismember(table_data{idx_table_flowin}.techs,techs_biofuel_boiler);
    %identify data for biofuel_to_diesel
    techs_biofuel_to_diesel = {'biofuel_to_diesel'};
    idx_biofuel_to_diesel = ismember(table_data{idx_table_flowin}.techs,techs_biofuel_to_diesel);
    %identify data for biofuel_to_liquids
    techs_biofuel_to_liquids = {'biofuel_to_liquids'};
    idx_biofuel_to_liquids = ismember(table_data{idx_table_flowin}.techs,techs_biofuel_to_liquids);
    %identify data for biofuel_to_methanol
    techs_biofuel_to_methanol = {'biofuel_to_methanol'};
    idx_biofuel_to_methanol = ismember(table_data{idx_table_flowin}.techs,techs_biofuel_to_methanol);
    %identify data for chp_biofuel_extraction
    techs_chp_biofuel_extraction = {'chp_biofuel_extraction', 'chp_biofuel_extraction_ccs'};
    idx_chp_biofuel_extraction = ismember(table_data{idx_table_flowin}.techs,techs_chp_biofuel_extraction);

    %identify data for chp_biogas
    techs_chp_biogas = {'chp_biogas','chp_biogas_ccs'};
    idx_chp_biogas = ismember(table_data{idx_table_flowin}.techs,techs_chp_biogas);

    % create hourly profiles for all CH
    [data_biofuel_boiler_Scen1_allCH_hrly_TWh, data_biofuel_to_diesel_Scen1_allCH_hrly_TWh, data_biofuel_to_liquids_Scen1_allCH_hrly_TWh, data_biofuel_to_methanol_Scen1_allCH_hrly_TWh, data_chp_biofuel_extraction_Scen1_allCH_hrly_TWh, data_chp_biogas_Scen1_allCH_hrly_TWh] = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_swiss1,idx_biomassforgas, idx_biofuel_boiler,idx_biofuel_to_diesel, idx_biofuel_to_liquids, idx_biofuel_to_methanol, idx_chp_biofuel_extraction, idx_chp_biogas);
    % CH, convert units to MWh
    [data_biofuel_boiler_Scen1_CHhourly_MWh, data_biofuel_to_diesel_Scen1_CHhourly_MWh, data_biofuel_to_liquids_Scen1_CHhourly_MWh, data_biofuel_to_methanol_Scen1_CHhourly_MWh, data_chp_biofuel_extraction_Scen1_CHhourly_MWh, data_chp_biogas_Scen1_CHhourly_MWh] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_biofuel_boiler_Scen1_allCH_hrly_TWh, data_biofuel_to_diesel_Scen1_allCH_hrly_TWh, data_biofuel_to_liquids_Scen1_allCH_hrly_TWh, data_biofuel_to_methanol_Scen1_allCH_hrly_TWh, data_chp_biofuel_extraction_Scen1_allCH_hrly_TWh, data_chp_biogas_Scen1_allCH_hrly_TWh);
    
    % save processed data for DBcreation (available biofuel and biogas for gas production)
    % hourly
    CalliopeToNexuse.biofuel_hrly.biofuel_forboiler_hrly.CH         = data_biofuel_boiler_Scen1_CHhourly_MWh;
    CalliopeToNexuse.biofuel_hrly.biofuel_fordiesel_hrly.CH         = data_biofuel_to_diesel_Scen1_CHhourly_MWh;
    CalliopeToNexuse.biofuel_hrly.biofuel_forliquids_hrly.CH        = data_biofuel_to_liquids_Scen1_CHhourly_MWh;
    CalliopeToNexuse.biofuel_hrly.biofuel_formethanol_hrly.CH       = data_biofuel_to_methanol_Scen1_CHhourly_MWh;
    CalliopeToNexuse.biofuel_hrly.biofuel_forchp_hrly.CH            = data_chp_biofuel_extraction_Scen1_CHhourly_MWh;
    CalliopeToNexuse.biofuel_hrly.biogas_forchp_hrly.CH             = data_chp_biogas_Scen1_CHhourly_MWh;
    % annual
    CalliopeToNexuse.biofuel_yrly.biofuel_forboiler_yrly.CH         = sum(data_biofuel_boiler_Scen1_CHhourly_MWh);
    CalliopeToNexuse.biofuel_yrly.biofuel_fordiesel_yrly.CH         = sum(data_biofuel_to_diesel_Scen1_CHhourly_MWh);
    CalliopeToNexuse.biofuel_yrly.biofuel_forliquids_yrly.CH        = sum(data_biofuel_to_liquids_Scen1_CHhourly_MWh);
    CalliopeToNexuse.biofuel_yrly.biofuel_formethanol_yrly.CH       = sum(data_biofuel_to_methanol_Scen1_CHhourly_MWh);
    CalliopeToNexuse.biofuel_yrly.biofuel_forchp_yrly.CH            = sum(data_chp_biofuel_extraction_Scen1_CHhourly_MWh);
    CalliopeToNexuse.biofuel_yrly.biogas_forchp_yrly.CH             = sum(data_chp_biogas_Scen1_CHhourly_MWh);
    % units
    CalliopeToNexuse.Units.biofuel_forboiler    = ('MWh');
    CalliopeToNexuse.Units.biofuel_fordiesel    = ('MWh');
    CalliopeToNexuse.Units.biofuel_forliquids   = ('MWh');
    CalliopeToNexuse.Units.biofuel_formethanol  = ('MWh');
    CalliopeToNexuse.Units.biofuel_forchp       = ('MWh');
    CalliopeToNexuse.Units.biogas_forchp        = ('MWh');



    disp(' ')
    disp(['The total processing time for the electricity and fuel demand profiles is: ', num2str(toc(strElecLoad)), ' (s) '])
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
    
    %   -list of transformation technologies & supply technlogies, producing methane (added on the 22.7. of July 24; chrgraf)

    % (neglected July 2024 chrgraf:
    %   -list added with non electric generator types (e.g. technology =
    %   chp_hydrogen, Carrier = heat, used for hydrogen demand)
    %   for GasNet, added by chrgraf June 2024)
    
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

%     % identify data for heat production (added for GasNet)
%     idx_heatgen = strcmp(table_data{idx_table_GenCapacities}.carriers,'heat')&(strcmp(table_data{idx_table_GenCapacities}.techs,'chp_hydrogen')|strcmp(table_data{idx_table_GenCapacities}.techs,'chp_methane_extraction'));
% %     idx_heatgen(10,:) = strcmp(table_data{idx_table_GenCapacities}.carriers(10,:),'heat')&&(strcmp(table_data{idx_table_GenCapacities}.techs(10,:),'chp_hydrogen')||strcmp(table_data{idx_table_GenCapacities}.techs(10,:),'chp_methane_extraction'));
% %     disp(idx_heatgen(10,:));

    %     %idx_CARRIERload_base_Scen1{k}       = varargin{k}         & idx_scenario1     & idx_country     & idx_CARRIERdem;
%     % ignore all tech types but chp ('biofuel_to_liquids' is not in New data files)
%     techs_ignore_list = {'biofuel_boiler', 'chp_biofuel_extraction', 'chp_biofuel_extraction_ccs', 'chp_biogas', 'chp_biogas_css', 'chp_wte_back_pressure','chp_wte_back_pressure_css', 'electric_heater', 'flexibility_heat', 'heat_storage_big','heat_storage_small','hp', 'methane_boiler'};
%     idx_heatgen(ismember(table_data{idx_table_GenCapacities}.techs,techs_ignore_list)) = false;

    % identify data for hydrogen supply
        % note: it does identify all technologies with hydrogen as carrier (for % both flow_in and flow out)
        % note: regarding flow_in:
        %     - all techs                       -> included
        % note: regarding flow_out:
        %     - electrolysis                    -> included
        %     - hydrogen_distribution_import    -> included
        %     - hydrogen_storage                -> included
        %     - hydrogen_underground_storage    -> included
    idx_h2sup = strcmp(table_data{idx_table_GenCapacities}.carriers,'hydrogen');
    techs_ignore_list_h2sup = {};
    idx_h2sup(ismember(table_data{idx_table_GenCapacities}.techs,techs_ignore_list_h2sup)) = false;

    % identify data for gas supply
        % note: it does identify all technologies with methane as carrier (for % both flow_in and flow out)
        % note: regarding flow_in:
        %     - all techs                       -> included
        % note: regarding flow_out, I'm interested in the syn_methane produced by biofuel or H2 & imported and also the biomethane produced by biogas
        %     - biofuel_to_methane              -> included
        %     - hydrogen_to_methane             -> included
        %     - methane_storage                 -> ignored  (scope of GasNet)
        %     - methane_supply                  -> ToDo: SHOULD I ignore it or not? (is fossil methane)
        %     - syn_methane_distribution_import -> ToDo: SHOULD I ignore it or not?
        %     - biogas_upgrading                -> included (biomethane)
        %     - biogas_upgrading_ccs            -> included (biomethane)
        %     - biomethane_import               -> included (biomethane)

    techs_methane_supply = {'methane','biomethane'};
    idx_gassup = ismember(table_data{idx_table_GenCapacities}.carriers,techs_methane_supply);
    techs_ignore_list_gassup = {};
    %techs_ignore_list_gassup = {'methane_storage','methane_supply','syn_methane_distribution_import'};
    idx_gassup(ismember(table_data{idx_table_GenCapacities}.techs,techs_ignore_list_gassup)) = false;


    
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

%CHECK:    IF WE WOULD READ MULTIPLE SCENARIOS 'tabledata_elecgens_Scen1' WOULD INCLUDE ALL SCENARIOS, RIGHT? I CAN'T SEE THE SELECTION OF THE SCENARIO.
%TODO:  -> IF I'M RIGHT, RENAME 'tabledata_elecgens_Scen1' TO 'tabledata_elecgens'

    % separate only electricity generators
    tabledata_elecgens_Scen1 = table_data{idx_table_GenCapacities}(idx_elecgen,:);
    % get list of techs for electricity generators
    techs_elecgens = unique(tabledata_elecgens_Scen1.techs,'stable');
    % save this list for DB creation
    CalliopeToNexuse.techs_list_elec = techs_elecgens;

%     %--------------------------------------------------------------------------
%     % get heat generator types (added for GasNet)
%     %--------------------------------------------------------------------------
%     % separate only heat generators
%     tabledata_heatgens_Scen1 = table_data{idx_table_GenCapacities}(idx_heatgen,:);
%     % get list of techs for electricity generators
%     techs_heatgens = unique(tabledata_heatgens_Scen1.techs,'stable');
%     % save this list for DB creation
%     CalliopeToNexuse.techs_list_heat = techs_heatgens;

    
    %--------------------------------------------------------------------------
    % get hydrogen unit types (added for GasNet) 
    %--------------------------------------------------------------------------
    % separate only hydrogen techs
    tabledata_h2sup_Scen1 = table_data{idx_table_GenCapacities}(idx_h2sup,:);
    % get list of techs for gas supply
    techs_h2sup = unique(tabledata_h2sup_Scen1.techs,'stable');
    % save this list for DB creation
    CalliopeToNexuse.techs_list_h2sup = techs_h2sup;


    %--------------------------------------------------------------------------
    % get gas unit types (added for GasNet) 
    %--------------------------------------------------------------------------
    % separate only gas techs
    tabledata_gassup_Scen1 = table_data{idx_table_GenCapacities}(idx_gassup,:);
    % get list of techs for gas supply
    techs_gassup = unique(tabledata_gassup_Scen1.techs,'stable');
    % save this list for DB creation
    CalliopeToNexuse.techs_list_gassup = techs_gassup;



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
    tabledata_elecgens_Scen1_CH = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_CH,2:7);
    tabledata_elecgens_Scen1_DE = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_DE,2:7);
    tabledata_elecgens_Scen1_FR = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_FR,2:7);
    tabledata_elecgens_Scen1_IT = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_IT,2:7);
    tabledata_elecgens_Scen1_AT = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_AT,2:7);
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
%CHECK: WHAT IS THIS GOOD FOR?
%       There are the expected differences:
%       - in ScenAll also the above ignored technologies are listed.
%       - the very small technologies are not rounded.
%TODO:  Delete this quick check? Do it first right?
    
    tabledata_elecgens_ScenAll_DE = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'DEU') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_DE_sort = sortrows(tabledata_elecgens_ScenAll_DE,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_FR = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'FRA') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_FR_sort = sortrows(tabledata_elecgens_ScenAll_FR,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_IT = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'ITA') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_IT_sort = sortrows(tabledata_elecgens_ScenAll_IT,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    tabledata_elecgens_ScenAll_AU = table_data{idx_table_GenCapacities}(strcmp(table_data{idx_table_GenCapacities}.locs,'AUT') & strcmp(table_data{idx_table_GenCapacities}.carriers,'electricity'),:);
    %tabledata_elecgens_ScenAll_AU_sort = sortrows(tabledata_elecgens_ScenAll_AU,{'techs','GTC_limit','NTC_multiplier','swiss_fuel_autarky','swiss_net_transfer_constraint'});
    
%     %--------------------------------------------------------------------------
%     % save processed data for DBcreation
%     %--------------------------------------------------------------------------
%     
%     % all non-Swiss regions
%     CalliopeToNexuse.GenTypeParams.DE = tabledata_elecgens_Scen1_DE;
%     CalliopeToNexuse.GenTypeParams.FR = tabledata_elecgens_Scen1_FR;
%     CalliopeToNexuse.GenTypeParams.IT = tabledata_elecgens_Scen1_IT;
%     CalliopeToNexuse.GenTypeParams.AT = tabledata_elecgens_Scen1_AT;
%     %CalliopeToNexuse.GenTypeParams.EU = tabledata_elecgens_Scen1_EU;
%     % also save Swiss regions
%     CalliopeToNexuse.GenTypeParams.CH = tabledata_elecgens_Scen1_CH;
%     %CalliopeToNexuse.GenCapacities_CH_canton  = tabledata_elecgens_Scen1_CH;
%     % units
%     CalliopeToNexuse.Units.GenCapacities = ('MW');
    
%     disp(' ')
%     disp(['The total processing time for the generator capacities is: ', num2str(toc(strGenCap)), ' (s) '])
%     disp('=========================================================================')

%     %--------------------------------------------------------------------------
%     % separate heat Gen Capacities for each country, for this scenario
%     %--------------------------------------------------------------------------
%     
%     % create identifiers for each country's generators
%     idx_heatgens_Scen1_CH   = idx_heatgen & idx_scenario3 & idx_swiss3;
%     idx_heatgens_Scen1_DE   = idx_heatgen & idx_scenario3 & idx_germany3;
%     idx_heatgens_Scen1_FR   = idx_heatgen & idx_scenario3 & idx_france3;
%     idx_heatgens_Scen1_IT   = idx_heatgen & idx_scenario3 & idx_italy3;
%     idx_heatgens_Scen1_AT   = idx_heatgen & idx_scenario3 & idx_austria3;
%     
%     % get entries for each country in given scenario
%     tabledata_heatgens_Scen1_CH = table_data{idx_table_GenCapacities}(idx_heatgens_Scen1_CH,2:7);
%     tabledata_heatgens_Scen1_DE = table_data{idx_table_GenCapacities}(idx_heatgens_Scen1_DE,2:7);
%     tabledata_heatgens_Scen1_FR = table_data{idx_table_GenCapacities}(idx_heatgens_Scen1_FR,2:7);
%     tabledata_heatgens_Scen1_IT = table_data{idx_table_GenCapacities}(idx_heatgens_Scen1_IT,2:7);
%     tabledata_heatgens_Scen1_AT = table_data{idx_table_GenCapacities}(idx_heatgens_Scen1_AT,2:7);
%     %tabledata_elecgens_Scen1_EU = table_data{idx_table_GenCapacities}(idx_elecgens_Scen1_EU,6:10);
%     
%     % convert units to MW (from TW) and round to nearest 0.1
%     tabledata_heatgens_Scen1_CH.nameplate_capacity = round(tabledata_heatgens_Scen1_CH.nameplate_capacity*1000*1000,1);
%     tabledata_heatgens_Scen1_DE.nameplate_capacity = round(tabledata_heatgens_Scen1_DE.nameplate_capacity*1000*1000,1);
%     tabledata_heatgens_Scen1_FR.nameplate_capacity = round(tabledata_heatgens_Scen1_FR.nameplate_capacity*1000*1000,1);
%     tabledata_heatgens_Scen1_IT.nameplate_capacity = round(tabledata_heatgens_Scen1_IT.nameplate_capacity*1000*1000,1);
%     tabledata_heatgens_Scen1_AT.nameplate_capacity = round(tabledata_heatgens_Scen1_AT.nameplate_capacity*1000*1000,1);
%     %tabledata_heatgens_Scen1_EU.nameplate_capacity = round(tabledata_heatgens_Scen1_EU.nameplate_capacity*1000*1000,1);
%     % also modify 'unit' column
%     tabledata_heatgens_Scen1_CH.unit = repmat({'mw'},size(tabledata_heatgens_Scen1_CH,1),1);
%     tabledata_heatgens_Scen1_DE.unit = repmat({'mw'},size(tabledata_heatgens_Scen1_DE,1),1);
%     tabledata_heatgens_Scen1_FR.unit = repmat({'mw'},size(tabledata_heatgens_Scen1_FR,1),1);
%     tabledata_heatgens_Scen1_IT.unit = repmat({'mw'},size(tabledata_heatgens_Scen1_IT,1),1);
%     tabledata_heatgens_Scen1_AT.unit = repmat({'mw'},size(tabledata_heatgens_Scen1_AT,1),1);
%     %tabledata_heatgens_Scen1_EU.unit = repmat({'mw'},size(tabledata_heatgens_Scen1_EU,1),1);
    
%     %--------------------------------------------------------------------------
%     % set any capacities < 5 MW to = 0 (eliminate really small capacities)
%     %--------------------------------------------------------------------------
%     tabledata_heatgens_Scen1_DE.nameplate_capacity( tabledata_heatgens_Scen1_DE.nameplate_capacity < 5 ) = 0;
%     tabledata_heatgens_Scen1_FR.nameplate_capacity( tabledata_heatgens_Scen1_FR.nameplate_capacity < 5 ) = 0;
%     tabledata_heatgens_Scen1_IT.nameplate_capacity( tabledata_heatgens_Scen1_IT.nameplate_capacity < 5 ) = 0;
%     tabledata_heatgens_Scen1_AT.nameplate_capacity( tabledata_heatgens_Scen1_AT.nameplate_capacity < 5 ) = 0;
%     tabledata_heatgens_Scen1_CH.nameplate_capacity( tabledata_heatgens_Scen1_CH.nameplate_capacity < 5 ) = 0;
    
 
  
%     %--------------------------------------------------------------------------
%     % save processed data for DBcreation
%     %--------------------------------------------------------------------------
%     
%     % all non-Swiss regions
%     CalliopeToNexuse.HeatGenTypeParams.DE = tabledata_heatgens_Scen1_DE;
%     CalliopeToNexuse.HeatGenTypeParams.FR = tabledata_heatgens_Scen1_FR;
%     CalliopeToNexuse.HeatGenTypeParams.IT = tabledata_heatgens_Scen1_IT;
%     CalliopeToNexuse.HeatGenTypeParams.AT = tabledata_heatgens_Scen1_AT;
%     %CalliopeToNexuse.HeatGenTypeParams.EU = tabledata_heatgens_Scen1_EU;
%     % also save Swiss regions
%     CalliopeToNexuse.HeatGenTypeParams.CH = tabledata_heatgens_Scen1_CH;
%     %CalliopeToNexuse.GenCapacities_CH_canton  = tabledata_heatgens_Scen1_CH;
%     % units
%     CalliopeToNexuse.Units.HeatGenCapacities = ('MW');

    
    %--------------------------------------------------------------------------
    % separate hydrogen unit Capacities for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for each country's generators
    idx_h2sup_Scen1_CH   = idx_h2sup & idx_scenario3 & idx_swiss3;
    idx_h2sup_Scen1_DE   = idx_h2sup & idx_scenario3 & idx_germany3;
    idx_h2sup_Scen1_FR   = idx_h2sup & idx_scenario3 & idx_france3;
    idx_h2sup_Scen1_IT   = idx_h2sup & idx_scenario3 & idx_italy3;
    idx_h2sup_Scen1_AT   = idx_h2sup & idx_scenario3 & idx_austria3;
    
    % get entries for each country in given scenario
    tabledata_h2sup_Scen1_CH = table_data{idx_table_GenCapacities}(idx_h2sup_Scen1_CH,2:7); 
    tabledata_h2sup_Scen1_DE = table_data{idx_table_GenCapacities}(idx_h2sup_Scen1_DE,2:7);
    tabledata_h2sup_Scen1_FR = table_data{idx_table_GenCapacities}(idx_h2sup_Scen1_FR,2:7);
    tabledata_h2sup_Scen1_IT = table_data{idx_table_GenCapacities}(idx_h2sup_Scen1_IT,2:7);
    tabledata_h2sup_Scen1_AT = table_data{idx_table_GenCapacities}(idx_h2sup_Scen1_AT,2:7);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_h2sup_Scen1_CH.nameplate_capacity = round(tabledata_h2sup_Scen1_CH.nameplate_capacity*1000*1000,1);
    tabledata_h2sup_Scen1_DE.nameplate_capacity = round(tabledata_h2sup_Scen1_DE.nameplate_capacity*1000*1000,1);
    tabledata_h2sup_Scen1_FR.nameplate_capacity = round(tabledata_h2sup_Scen1_FR.nameplate_capacity*1000*1000,1);
    tabledata_h2sup_Scen1_IT.nameplate_capacity = round(tabledata_h2sup_Scen1_IT.nameplate_capacity*1000*1000,1);
    tabledata_h2sup_Scen1_AT.nameplate_capacity = round(tabledata_h2sup_Scen1_AT.nameplate_capacity*1000*1000,1);

    tabledata_h2sup_Scen1_CH.unit = repmat({'mw'},size(tabledata_h2sup_Scen1_CH,1),1);
    tabledata_h2sup_Scen1_DE.unit = repmat({'mw'},size(tabledata_h2sup_Scen1_DE,1),1);
    tabledata_h2sup_Scen1_FR.unit = repmat({'mw'},size(tabledata_h2sup_Scen1_FR,1),1);
    tabledata_h2sup_Scen1_IT.unit = repmat({'mw'},size(tabledata_h2sup_Scen1_IT,1),1);
    tabledata_h2sup_Scen1_AT.unit = repmat({'mw'},size(tabledata_h2sup_Scen1_AT,1),1);

    
%     %--------------------------------------------------------------------------
%     % set any capacities < 5 MW to = 0 (eliminate really small capacities)
%     %--------------------------------------------------------------------------
%     tabledata_h2sup_Scen1_DE.nameplate_capacity( tabledata_h2sup_Scen1_DE.nameplate_capacity < 5 ) = 0;
%     tabledata_h2sup_Scen1_FR.nameplate_capacity( tabledata_h2sup_Scen1_FR.nameplate_capacity < 5 ) = 0;
%     tabledata_h2sup_Scen1_IT.nameplate_capacity( tabledata_h2sup_Scen1_IT.nameplate_capacity < 5 ) = 0;
%     tabledata_h2sup_Scen1_AT.nameplate_capacity( tabledata_h2sup_Scen1_AT.nameplate_capacity < 5 ) = 0;
%     tabledata_h2sup_Scen1_CH.nameplate_capacity( tabledata_h2sup_Scen1_CH.nameplate_capacity < 5 ) = 0;


    %--------------------------------------------------------------------------
    % separate gas unit Capacities for each country, for this scenario
    %--------------------------------------------------------------------------
    
    % create identifiers for each country's generators
    idx_gassup_Scen1_CH   = idx_gassup & idx_scenario3 & idx_swiss3;
    idx_gassup_Scen1_DE   = idx_gassup & idx_scenario3 & idx_germany3;
    idx_gassup_Scen1_FR   = idx_gassup & idx_scenario3 & idx_france3;
    idx_gassup_Scen1_IT   = idx_gassup & idx_scenario3 & idx_italy3;
    idx_gassup_Scen1_AT   = idx_gassup & idx_scenario3 & idx_austria3;
    
    % get entries for each country in given scenario
    tabledata_gassup_Scen1_CH = table_data{idx_table_GenCapacities}(idx_gassup_Scen1_CH,2:7); 
    tabledata_gassup_Scen1_DE = table_data{idx_table_GenCapacities}(idx_gassup_Scen1_DE,2:7);
    tabledata_gassup_Scen1_FR = table_data{idx_table_GenCapacities}(idx_gassup_Scen1_FR,2:7);
    tabledata_gassup_Scen1_IT = table_data{idx_table_GenCapacities}(idx_gassup_Scen1_IT,2:7);
    tabledata_gassup_Scen1_AT = table_data{idx_table_GenCapacities}(idx_gassup_Scen1_AT,2:7);
    
    % convert units to MW (from TW) and round to nearest 0.1
    tabledata_gassup_Scen1_CH.nameplate_capacity = round(tabledata_gassup_Scen1_CH.nameplate_capacity*1000*1000,1);
    tabledata_gassup_Scen1_DE.nameplate_capacity = round(tabledata_gassup_Scen1_DE.nameplate_capacity*1000*1000,1);
    tabledata_gassup_Scen1_FR.nameplate_capacity = round(tabledata_gassup_Scen1_FR.nameplate_capacity*1000*1000,1);
    tabledata_gassup_Scen1_IT.nameplate_capacity = round(tabledata_gassup_Scen1_IT.nameplate_capacity*1000*1000,1);
    tabledata_gassup_Scen1_AT.nameplate_capacity = round(tabledata_gassup_Scen1_AT.nameplate_capacity*1000*1000,1);

    tabledata_gassup_Scen1_CH.unit = repmat({'mw'},size(tabledata_gassup_Scen1_CH,1),1);
    tabledata_gassup_Scen1_DE.unit = repmat({'mw'},size(tabledata_gassup_Scen1_DE,1),1);
    tabledata_gassup_Scen1_FR.unit = repmat({'mw'},size(tabledata_gassup_Scen1_FR,1),1);
    tabledata_gassup_Scen1_IT.unit = repmat({'mw'},size(tabledata_gassup_Scen1_IT,1),1);
    tabledata_gassup_Scen1_AT.unit = repmat({'mw'},size(tabledata_gassup_Scen1_AT,1),1);

    
%     %--------------------------------------------------------------------------
%     % set any capacities < 5 MW to = 0 (eliminate really small capacities)
%     %--------------------------------------------------------------------------
%     tabledata_gassup_Scen1_DE.nameplate_capacity( tabledata_gassup_Scen1_DE.nameplate_capacity < 5 ) = 0;
%     tabledata_gassup_Scen1_FR.nameplate_capacity( tabledata_gassup_Scen1_FR.nameplate_capacity < 5 ) = 0;
%     tabledata_gassup_Scen1_IT.nameplate_capacity( tabledata_gassup_Scen1_IT.nameplate_capacity < 5 ) = 0;
%     tabledata_gassup_Scen1_AT.nameplate_capacity( tabledata_gassup_Scen1_AT.nameplate_capacity < 5 ) = 0;
%     tabledata_gassup_Scen1_CH.nameplate_capacity( tabledata_gassup_Scen1_CH.nameplate_capacity < 5 ) = 0;
    
    
 
  
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    % save the nameplate_capacities for 
    %   - electric generator
    %   - hydrogen units
    %   - gas units
    %--------------------------------------------------------------------------
    
    % all non-Swiss regions
    CalliopeToNexuse.GenTypeParams.DE = [tabledata_elecgens_Scen1_DE; tabledata_h2sup_Scen1_DE; tabledata_gassup_Scen1_DE];
    CalliopeToNexuse.GenTypeParams.FR = [tabledata_elecgens_Scen1_FR; tabledata_h2sup_Scen1_FR; tabledata_gassup_Scen1_FR];
    CalliopeToNexuse.GenTypeParams.IT = [tabledata_elecgens_Scen1_IT; tabledata_h2sup_Scen1_IT; tabledata_gassup_Scen1_IT];
    CalliopeToNexuse.GenTypeParams.AT = [tabledata_elecgens_Scen1_AT; tabledata_h2sup_Scen1_AT; tabledata_gassup_Scen1_AT];
    %CalliopeToNexuse.GenTypeParams.EU =[tabledata_elecgens_Scen1_EU;  tabledata_h2sup_Scen1_EU; tabledatagassups_Scen1_EU];
    % also save Swiss regions
    CalliopeToNexuse.GenTypeParams.CH = [tabledata_elecgens_Scen1_CH; tabledata_h2sup_Scen1_CH; tabledata_gassup_Scen1_CH];
    %CalliopeToNexuse.GenCapacities_CH_canton  = [tabledata_elecgens_Scen1_CH; tabledata_h2sup_Scen1_CH; tabledata_gassup_Scen1_CH];
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
    techs_gas = unique(tabledata_GenFOMCost_Scen1_CHsep.techs,'stable');
    
    % loop over each tech to find first entry FOM cost
    for i7 = 1:length(techs_gas)
        
        % identify all rows with this tech
        idx_tech_GenFOMCost_Scen1_CHsep     = find(strcmp(tabledata_GenFOMCost_Scen1_CHsep.techs, techs_gas(i7)),1,'first');
        
        % sum all entries for these timesteps
        data_GenFOMCost_Scen1_CHall(i7,1)    = tabledata_GenFOMCost_Scen1_CHsep.annual_cost_per_nameplate_capacity(idx_tech_GenFOMCost_Scen1_CHsep);
        
    end
    
    % create tabledata for all CH Gen Invstement cost
    tabledata_GenFOMCost_Scen1_CHall = tabledata_GenFOMCost_Scen1_CHsep(1:length(techs_gas),:);         % initialize with correct length
    tabledata_GenFOMCost_Scen1_CHall.techs = techs_gas;                                                 % replace list of techs
    tabledata_GenFOMCost_Scen1_CHall.locs = repmat({'CHE'},length(techs_gas),1);                        % replace locs
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
    %   -methane supply (for GasNet)
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these profiles are hourly (8760 entries)
    % the values in these profiles are NOT doubled
    % these profiles depend on the scenario
    
    disp(' ')
    disp('=========================================================================')
    disp('Processing the Fixed Injection profiles begins... ')
    disp('(for electricity, co2 , hydrogen and gas (methane) ...)')
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
%     % identify data for heat production     (added for GasNet)
%     idx_heatgen6 = strcmp(table_data{idx_table_FixedInj}.carriers,'heat');
    % identify data for methane production/supply 
    idx_h2sup6 = strcmp(table_data{idx_table_FixedInj}.carriers,'hydrogen');
    techs_methane_supply6 = {'methane','biomethane'};
    idx_gassup6 = ismember(table_data{idx_table_FixedInj}.carriers,techs_methane_supply6);
    
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
    [techs_CO2_Capture_CH, data_CO2_Capture_Scen5_CH_Mt, tabledata_CO2_Capture_Scen5_CH_Mt] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_swiss5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.CH);
    
%     % Heat supply via chp  (added for GasNet)
%     % use function to add hourly profiles for the defined heat generator types
%     % (will loop over each timestep to add up all CH, assumes same 
%     % timesteps in each type of data_elecload, will add if multiple regions
%     % -or- if multiple techs for each demand type -or- both)
%     [techs_HeatSupply_CH, data_HeatSupply_Scen5_CH_MWh, tabledata_HeatSupply_Scen5_CH_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_swiss5,idx_heatgen6,CalliopeToNexuse.HeatGenTypeParams.CH);

    % Fixed Hydrogen Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedH2Inj_CH, data_FixedH2Inj_Scen5_CH_MWh, tabledata_FixedH2Inj_Scen5_CH_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_swiss5,idx_h2sup6,CalliopeToNexuse.GenTypeParams.CH);

    % Fixed Gas Injections
    % use function to add hourly profiles for each generator type
    % (will loop over each timestep to add up all CH, assumes same 
    % timesteps in each type of data_elecload, will add if multiple regions
    % -or- if multiple techs for each demand type -or- both)
    [techs_FixedGasInj_CH, data_FixedGasInj_Scen5_CH_MWh, tabledata_FixedGasInj_Scen5_CH_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_swiss5,idx_gassup6,CalliopeToNexuse.GenTypeParams.CH);

    
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
    [techs_CO2_Capture_DE, data_CO2_Capture_Scen5_DE_Mt, tabledata_CO2_Capture_Scen5_DE_Mt] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_germany5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.DE);
    
    % Fixed Hydrogen Injections
    [techs_FixedH2Inj_DE, data_FixedH2Inj_Scen5_DE_MWh, tabledata_FixedH2Inj_Scen5_DE_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_germany5,idx_h2sup6,CalliopeToNexuse.GenTypeParams.DE);

     %Fixed Gas Injections
    [techs_FixedGasInj_DE, data_FixedGasInj_Scen5_DE_MWh, tabledata_FixedGasInj_Scen5_DE_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_germany5,idx_gassup6,CalliopeToNexuse.GenTypeParams.DE);
     
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
    [techs_CO2_Capture_FR, data_CO2_Capture_Scen5_FR_Mt, tabledata_CO2_Capture_Scen5_FR_Mt] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_france5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.FR);
    
    % Fixed Hydrogen Injections
    [techs_FixedH2Inj_FR, data_FixedH2Inj_Scen5_FR_MWh, tabledata_FixedH2Inj_Scen5_FR_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_france5,idx_h2sup6,CalliopeToNexuse.GenTypeParams.FR);

    %Fixed Gas Injections
    [techs_FixedGasInj_FR, data_FixedGasInj_Scen5_FR_MWh, tabledata_FixedGasInj_Scen5_FR_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_france5,idx_gassup6,CalliopeToNexuse.GenTypeParams.FR);
       
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
    [techs_CO2_Capture_IT, data_CO2_Capture_Scen5_IT_Mt, tabledata_CO2_Capture_Scen5_IT_Mt] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_italy5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.IT);
    
    % Fixed Hydrogen Injections
    [techs_FixedH2Inj_IT, data_FixedH2Inj_Scen5_IT_MWh, tabledata_FixedH2Inj_Scen5_IT_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_italy5,idx_h2sup6,CalliopeToNexuse.GenTypeParams.IT);

    %Fixed Gas Injections
    [techs_FixedGasInj_IT, data_FixedGasInj_Scen5_IT_MWh, tabledata_FixedGasInj_Scen5_IT_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_italy5,idx_gassup6,CalliopeToNexuse.GenTypeParams.IT);
     
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
    [techs_CO2_Capture_AT, data_CO2_Capture_Scen5_AT_Mt, tabledata_CO2_Capture_Scen5_AT_Mt] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_austria5,idx_co2capt6,CalliopeToNexuse.GenTypeParams.AT);
    
    % Fixed Hydrogen Injections
    [techs_FixedH2Inj_AT, data_FixedH2Inj_Scen5_AT_MWh, tabledata_FixedH2Inj_Scen5_AT_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_austria5,idx_h2sup6,CalliopeToNexuse.GenTypeParams.AT);

    %Fixed Gas Injections
    [techs_FixedGasInj_AT, data_FixedGasInj_Scen5_AT_MWh, tabledata_FixedGasInj_Scen5_AT_MWh] = Func_GetCalliopeGenerationData_OneCountry(table_data,idx_table_FixedInj,timesteps_num5,idx_scenario6,idx_austria5,idx_gassup6,CalliopeToNexuse.GenTypeParams.AT);
    
    
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
    CalliopeToNexuse.CO2_Capture_hrly.CH = tabledata_CO2_Capture_Scen5_CH_Mt;
    CalliopeToNexuse.CO2_Capture_hrly.DE = tabledata_CO2_Capture_Scen5_DE_Mt;
    CalliopeToNexuse.CO2_Capture_hrly.FR = tabledata_CO2_Capture_Scen5_FR_Mt;
    CalliopeToNexuse.CO2_Capture_hrly.IT = tabledata_CO2_Capture_Scen5_IT_Mt;
    CalliopeToNexuse.CO2_Capture_hrly.AT = tabledata_CO2_Capture_Scen5_AT_Mt;
    % units
    % saved after yearly

%     % Heat Supply
%     CalliopeToNexuse.HeatSupply_hrly.CH = tabledata_HeatSupply_Scen5_CH_MWh;
%  %TODO if needed save it yearly.

    % Fixed Hydrogen Injections
    CalliopeToNexuse.FixedH2Inj_hrly.CH = tabledata_FixedH2Inj_Scen5_CH_MWh;
    CalliopeToNexuse.FixedH2Inj_hrly.DE = tabledata_FixedH2Inj_Scen5_DE_MWh;
    CalliopeToNexuse.FixedH2Inj_hrly.FR = tabledata_FixedH2Inj_Scen5_FR_MWh;
    CalliopeToNexuse.FixedH2Inj_hrly.IT = tabledata_FixedH2Inj_Scen5_IT_MWh;
    CalliopeToNexuse.FixedH2Inj_hrly.AT = tabledata_FixedH2Inj_Scen5_AT_MWh;
    % units
    CalliopeToNexuse.Units.FixedH2Inj = ('MWh');

    % Fixed Gas Injections
    CalliopeToNexuse.FixedGasInj_hrly.CH = tabledata_FixedGasInj_Scen5_CH_MWh;
    CalliopeToNexuse.FixedGasInj_hrly.DE = tabledata_FixedGasInj_Scen5_DE_MWh;
    CalliopeToNexuse.FixedGasInj_hrly.FR = tabledata_FixedGasInj_Scen5_FR_MWh;
    CalliopeToNexuse.FixedGasInj_hrly.IT = tabledata_FixedGasInj_Scen5_IT_MWh;
    CalliopeToNexuse.FixedGasInj_hrly.AT = tabledata_FixedGasInj_Scen5_AT_MWh;
    % units
    CalliopeToNexuse.Units.FixedGasInj = ('MWh');
    
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
    tabledata_CO2_Capture_Scen5_CH_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_CH_Mt,1),'VariableNames',techs_CO2_Capture_CH);
    tabledata_CO2_Capture_Scen5_DE_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_DE_Mt,1),'VariableNames',techs_CO2_Capture_DE);
    tabledata_CO2_Capture_Scen5_FR_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_FR_Mt,1),'VariableNames',techs_CO2_Capture_FR);
    tabledata_CO2_Capture_Scen5_IT_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_IT_Mt,1),'VariableNames',techs_CO2_Capture_IT);
    tabledata_CO2_Capture_Scen5_AT_fullyr_sum    = array2table(sum(data_CO2_Capture_Scen5_AT_Mt,1),'VariableNames',techs_CO2_Capture_AT);

    % Fixed Hydrogen Injections
    tabledata_FixedH2Inj_Scen5_CH_fullyr_sum    = array2table(sum(data_FixedH2Inj_Scen5_CH_MWh,1),'VariableNames',techs_FixedH2Inj_CH);
    tabledata_FixedH2Inj_Scen5_DE_fullyr_sum    = array2table(sum(data_FixedH2Inj_Scen5_DE_MWh,1),'VariableNames',techs_FixedH2Inj_DE);
    tabledata_FixedH2Inj_Scen5_FR_fullyr_sum    = array2table(sum(data_FixedH2Inj_Scen5_FR_MWh,1),'VariableNames',techs_FixedH2Inj_FR);
    tabledata_FixedH2Inj_Scen5_IT_fullyr_sum    = array2table(sum(data_FixedH2Inj_Scen5_IT_MWh,1),'VariableNames',techs_FixedH2Inj_IT);
    tabledata_FixedH2Inj_Scen5_AT_fullyr_sum    = array2table(sum(data_FixedH2Inj_Scen5_AT_MWh,1),'VariableNames',techs_FixedH2Inj_AT);
    
    % Fixed Gas Injections
    tabledata_FixedGasInj_Scen5_CH_fullyr_sum    = array2table(sum(data_FixedGasInj_Scen5_CH_MWh,1),'VariableNames',techs_FixedGasInj_CH);
    tabledata_FixedGasInj_Scen5_DE_fullyr_sum    = array2table(sum(data_FixedGasInj_Scen5_DE_MWh,1),'VariableNames',techs_FixedGasInj_DE);
    tabledata_FixedGasInj_Scen5_FR_fullyr_sum    = array2table(sum(data_FixedGasInj_Scen5_FR_MWh,1),'VariableNames',techs_FixedGasInj_FR);
    tabledata_FixedGasInj_Scen5_IT_fullyr_sum    = array2table(sum(data_FixedGasInj_Scen5_IT_MWh,1),'VariableNames',techs_FixedGasInj_IT);
    tabledata_FixedGasInj_Scen5_AT_fullyr_sum    = array2table(sum(data_FixedGasInj_Scen5_AT_MWh,1),'VariableNames',techs_FixedGasInj_AT);
    
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
    
    % Fixed H2 Injections
    CalliopeToNexuse.FixedH2Inj_yrly.CH = tabledata_FixedH2Inj_Scen5_CH_fullyr_sum;
    CalliopeToNexuse.FixedH2Inj_yrly.DE = tabledata_FixedH2Inj_Scen5_DE_fullyr_sum;
    CalliopeToNexuse.FixedH2Inj_yrly.FR = tabledata_FixedH2Inj_Scen5_FR_fullyr_sum;
    CalliopeToNexuse.FixedH2Inj_yrly.IT = tabledata_FixedH2Inj_Scen5_IT_fullyr_sum;
    CalliopeToNexuse.FixedH2Inj_yrly.AT = tabledata_FixedH2Inj_Scen5_AT_fullyr_sum;

    % Fixed Gas Injections
    CalliopeToNexuse.FixedGasInj_yrly.CH = tabledata_FixedGasInj_Scen5_CH_fullyr_sum;
    CalliopeToNexuse.FixedGasInj_yrly.DE = tabledata_FixedGasInj_Scen5_DE_fullyr_sum;
    CalliopeToNexuse.FixedGasInj_yrly.FR = tabledata_FixedGasInj_Scen5_FR_fullyr_sum;
    CalliopeToNexuse.FixedGasInj_yrly.IT = tabledata_FixedGasInj_Scen5_IT_fullyr_sum;
    CalliopeToNexuse.FixedGasInj_yrly.AT = tabledata_FixedGasInj_Scen5_AT_fullyr_sum;


    disp(' ')
    disp(['The total processing time for the Fixed Injection & CO2 Capture profiles is: ', num2str(toc(strFixedInj)), ' (s) '])
    disp('=========================================================================')
    
    %% --------------------------------------------------------------------------
%     % Preparation for temporary calculations on chp:
%     %--------------------------------------------------------------------------
% 
%     [data_H2load_chp_Scen1_CHsum_MWh_NotRounded] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_H2load_chp_Scen1_allCH_hrly_TWh);
%     CalliopeToNexuse.chpH2Demand_NotRounded_hrly.CH        = data_H2load_chp_Scen1_CHsum_MWh_NotRounded;
%     [data_gasload_chp_Scen1_CHsum_MWh_NotRounded] = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(data_gasload_chp_Scen1_allCH_hrly_TWh);
%     CalliopeToNexuse.chpgasDemand_NotRounded_hrly.CH        = data_gasload_chp_Scen1_CHsum_MWh_NotRounded;

    %% --------------------------------------------------------------------------
%     % Temporary calculations on chp:
%     %--------------------------------------------------------------------------
%     
%     disp("Temporary calculations on chp:");
%     disp("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
%     disp("Note: for Now the H2 and Gas Demand for chp are writen into the CalliopeToNexus.mat file but are not used to determin the overall H2 and Gas Demand in MySQL.");
%     disp("If one day the chp should be used (for both heat & ELECTRICITY!) , redo this section and calculate the standard deviation.");
%     disp("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
% 
% 
%     % Chp: Import data of flow_out done also the parts in nameplate_capacity
%     % Is the sum of chp_hydrogen ‘electricity’ + chp_hydrogen ‘heat’ = chp_hydrogen ‘hydrogen’ ?
%     % If yes: -> don’t use chp_hydrogen ‘hydrogen’ from flow in but chp_hydrogen ‘heat’ from flow_out
% 
%     % for Hydrogen:
%     chp_output = CalliopeToNexuse.FixedInj_hrly.CH{:,'chp_hydrogen'} + CalliopeToNexuse.HeatSupply_hrly.CH{:,'chp_hydrogen'};
%     disp('Sum of chp_output');
%     disp(sum(chp_output));
%     disp('Sum of chp_hydrogen input=');
%     disp(sum(CalliopeToNexuse.chpH2Demand_NotRounded_hrly.CH));
%     for t=1:8760
%         chp_diff(t) = chp_output(t) - CalliopeToNexuse.chpH2Demand_NotRounded_hrly.CH(t);
%     end 
%     chp_diff =chp_diff';
%     disp("Mean of chp difference, pos = more output (electricity + heat) than input (hydrogen), neg = input > output");
%     disp(mean(chp_diff));
%    
%     analysis_chp.H2 = table(CalliopeToNexuse.chpH2Demand_NotRounded_hrly.CH,CalliopeToNexuse.FixedInj_hrly.CH{:,6},CalliopeToNexuse.HeatSupply_hrly.CH{:,1},chp_output,chp_diff, 'VariableNames', {'H2_input', 'elec_output', 'heat_output', 'summed_output', 'differenz = output - input'});
%     
%     analysis = table(analysis_chp.H2.elec_output./analysis_chp.H2.H2_input, analysis_chp.H2.heat_output./analysis_chp.H2.H2_input, analysis_chp.H2.summed_output./analysis_chp.H2.H2_input, 'VariableNames', {'elec_fraction', 'heat_fraction', 'output_fraction'});
%     analysis_chp.H2 = [analysis_chp.H2, analysis]; 
%     clear analysis;
% 
%     % for Methane:
%     %chp_out = electricity + heat 
%     chp_output = CalliopeToNexuse.FixedInj_hrly.CH{:,'chp_methane_extraction'} + CalliopeToNexuse.HeatSupply_hrly.CH{:,'chp_methane_extraction'};
%     disp('Sum of chp_output');
%     disp(sum(chp_output));
%     disp('Sum of chp_methane input=');
%     disp(sum(CalliopeToNexuse.chpgasDemand_NotRounded_hrly.CH));
%     for t=1:8760
%         chp_diff(t) = chp_output(t) - CalliopeToNexuse.chpgasDemand_NotRounded_hrly.CH(t);
%     end 
%     chp_diff =chp_diff';
%     disp("Mean of chp difference, pos = more output (electricity + heat) than input (methane), neg = input > output");
%     disp(mean(chp_diff));
%    
%     analysis_chp.gas = table(CalliopeToNexuse.chpgasDemand_NotRounded_hrly.CH,CalliopeToNexuse.FixedInj_hrly.CH{:,'chp_methane_extraction'},CalliopeToNexuse.HeatSupply_hrly.CH{:,'chp_methane_extraction'},chp_output,chp_diff', 'VariableNames', {'gas_input', 'elec_output', 'heat_output', 'summed_output', 'differenz = output - input'});
%     
%     analysis = table(analysis_chp.gas.elec_output./analysis_chp.gas.gas_input, analysis_chp.gas.heat_output./analysis_chp.gas.gas_input, analysis_chp.gas.summed_output./analysis_chp.gas.gas_input, 'VariableNames', {'elec_fraction', 'heat_fraction', 'output_fraction'});
%     analysis_chp.gas = [analysis_chp.gas, analysis]; 
%     clear analysis;
% 
%     disp('mean analysis_chp.gas.heat_fraction');
%     disp(mean(analysis_chp.gas.heat_fraction));
%     disp(max(analysis_chp.gas.heat_fraction)-mean(analysis_chp.gas.heat_fraction));
%     disp(min(analysis_chp.gas.heat_fraction)-mean(analysis_chp.gas.heat_fraction));
% %TODO find the ratio between hydrogen input & heat output
% 
% %TODO find the ratio between methane input & heat output


    %%
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % Process data from: total_system_emissions.csv
    %   -annual CO2 emissions for each region by tech type
    %^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    % these values are annual
    % ?? are these values the CO2 emitted or CO2 captured
    
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
    %   -hourly prices for electricity, CO2, H2, BioGas, SynGas, Gas (overall Methane) & Coal
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
    idx_gasprice9 = strcmp(table_data{idx_table_Duals}.carriers,'methane');
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
    [data_Prices_CH(:,5),name_GasPrices,units_GasPrices]                        = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_gasprice9);
    [data_Prices_CH(:,6),name_H2Prices,units_H2Prices]                          = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_h2price9);
    [data_Prices_CH(:,7),name_CoalPrices,units_CoalPrices]                      = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_swiss9,idx_coalprice9);
    % DE
    [data_Prices_DE(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_elecprice9);
    [data_Prices_DE(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_co2price9);
    [data_Prices_DE(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_biomethaneprice9);
    [data_Prices_DE(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_synmethaneprice9);
    [data_Prices_DE(:,5),z1,z2]	= Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_gasprice9);
    [data_Prices_DE(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_h2price9);
    [data_Prices_DE(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_germany9,idx_coalprice9);
    % FR
    [data_Prices_FR(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_elecprice9);
    [data_Prices_FR(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_co2price9);
    [data_Prices_FR(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_biomethaneprice9);
    [data_Prices_FR(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_synmethaneprice9);
    [data_Prices_FR(:,5),z1,z2]	= Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_gasprice9);
    [data_Prices_FR(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_h2price9);
    [data_Prices_FR(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_france9,idx_coalprice9);
    % IT
    [data_Prices_IT(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_elecprice9);
    [data_Prices_IT(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_co2price9);
    [data_Prices_IT(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_biomethaneprice9);
    [data_Prices_IT(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_synmethaneprice9);
    [data_Prices_IT(:,5),z1,z2]	= Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_gasprice9);
    [data_Prices_IT(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_h2price9);
    [data_Prices_IT(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_italy9,idx_coalprice9);
    % AT
    [data_Prices_AT(:,1),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_elecprice9);
    [data_Prices_AT(:,2),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_co2price9);
    [data_Prices_AT(:,3),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_biomethaneprice9);
    [data_Prices_AT(:,4),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_synmethaneprice9);
    [data_Prices_AT(:,5),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_gasprice9);
    [data_Prices_AT(:,6),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_h2price9);
    [data_Prices_AT(:,7),z1,z2] = Func_GetCalliopePriceData_OneCountry(table_data,idx_table_Duals,idx_scenario9,idx_austria9,idx_coalprice9);
    
    % organize the price column names
    prices_colnames = {'Electricity','CO2','BioMethane','SynMethane','Gas','Hydrogen','Coal'};  % [name_ElecPrices,name_CO2Prices,name_BioMethanePrices,name_SynMethanePrices,name_GasPrices,name_H2Prices,name_CoalPrices];
    
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
    CalliopeToNexuse.Units.GasPrices  = cell2mat(units_GasPrices);
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
        Dem_pack_Base_tpRes(hr_start_pack:hr_end_pack)          = CalliopeToNexuse.BaseElecDemand_hrly.CH(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_Rail_tpRes(hr_start_pack:hr_end_pack)          = CalliopeToNexuse.RailElecDemand_hrly.CH(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_H2_tpRes(hr_start_pack:hr_end_pack)            = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_Emob_noflex_tpRes(hr_start_pack:hr_end_pack)   = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_noflex_Calliope(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_Emob_flex_tpRes(hr_start_pack:hr_end_pack)     = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_flex_Calliope(hr_start_full(d1):hr_end_full(d1));
        Dem_pack_HP_tpRes(hr_start_pack:hr_end_pack)            = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump(hr_start_full(d1):hr_end_full(d1));
    end
    
    % now unpack back to 8760 hours
    hr_start_unpack = (days1-1)*24 + 1;
    hr_end_unpack = hr_start_unpack + tpRes*24-1;
    for d2 = 1:length(days1)-1
        hr_start_pack2 = (d2-1)*24 + 1;
        hr_end_pack2 = hr_start_pack2 + 23;
        Dem_unpack_Base_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))        = repmat(Dem_pack_Base_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_Rail_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))        = repmat(Dem_pack_Rail_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_H2_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))          = repmat(Dem_pack_H2_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_Emob_noflex_tpRes(hr_start_unpack(d2):hr_end_unpack(d2)) = repmat(Dem_pack_Emob_noflex_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_Emob_flex_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))   = repmat(Dem_pack_Emob_flex_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
        Dem_unpack_HP_tpRes(hr_start_unpack(d2):hr_end_unpack(d2))          = repmat(Dem_pack_HP_tpRes(hr_start_pack2:hr_end_pack2),1,tpRes);
    end
    % for last day only repeat for the remaining days
    days_remain = 365 - days1(end) + 1;
    Dem_unpack_Base_tpRes(hr_start_unpack(end):8760)        = repmat(Dem_pack_Base_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_Rail_tpRes(hr_start_unpack(end):8760)        = repmat(Dem_pack_Rail_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_H2_tpRes(hr_start_unpack(end):8760)          = repmat(Dem_pack_H2_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_Emob_noflex_tpRes(hr_start_unpack(end):8760) = repmat(Dem_pack_Emob_noflex_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_Emob_flex_tpRes(hr_start_unpack(end):8760)   = repmat(Dem_pack_Emob_flex_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    Dem_unpack_HP_tpRes(hr_start_unpack(end):8760)          = repmat(Dem_pack_HP_tpRes(hr_start_pack:hr_end_pack),1,days_remain);
    
    % calc monthly sums
    for m = 1:length(Hours_month_start)
        Dem_unpack_Mthly_Base_TWh(m)        = sum(Dem_unpack_Base_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_Rail_TWh(m)        = sum(Dem_unpack_Rail_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_H2_TWh(m)          = sum(Dem_unpack_H2_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_Emob_noflex_TWh(m) = sum(Dem_unpack_Emob_noflex_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_Emob_flex_TWh(m)   = sum(Dem_unpack_Emob_flex_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_unpack_Mthly_HP_TWh(m)          = sum(Dem_unpack_HP_tpRes(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
    end
    
    % get annual sums
    Dem_unpack_Yrly_Base_TWh        = sum(Dem_unpack_Mthly_Base_TWh);
    Dem_unpack_Yrly_Rail_TWh        = sum(Dem_unpack_Mthly_Rail_TWh);
    Dem_unpack_Yrly_H2_TWh          = sum(Dem_unpack_Mthly_H2_TWh);
    Dem_unpack_Yrly_Emob_noflex_TWh = sum(Dem_unpack_Mthly_Emob_noflex_TWh);
    Dem_unpack_Yrly_Emob_flex_TWh   = sum(Dem_unpack_Mthly_Emob_flex_TWh);
    Dem_unpack_Yrly_HP_TWh          = sum(Dem_unpack_Mthly_HP_TWh);
    
    % --------------------------------------------------------------------
    % for Original dat from Calliope
    for m = 1:length(Hours_month_start)
        
        Dem_Rail_monthly_TWh(m)         = sum(CalliopeToNexuse.RailElecDemand_hrly.CH(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_Base_monthly_TWh(m)         = sum(CalliopeToNexuse.BaseElecDemand_hrly.CH(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_H2_monthly_TWh(m)           = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_Emob_noflex_monthly_TWh(m)  = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_noflex_Calliope(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_Emob_flex_monthly_TWh(m)    = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_flex_Calliope(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        Dem_HP_monthly_TWh(m)           = sum(CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump(Hours_month_start(m):Hours_month_end(m))) / (1000*1000);
        
    end
    
    Dem_Rail_Tot_TWh        = sum(Dem_Rail_monthly_TWh);
    Dem_Base_Tot_TWh        = sum(Dem_Base_monthly_TWh);
    Dem_H2_Tot_TWh          = sum(Dem_H2_monthly_TWh);
    Dem_Emob_noflex_Tot_TWh = sum(Dem_Emob_noflex_monthly_TWh);
    Dem_Emob_flex_Tot_TWh   = sum(Dem_Emob_flex_monthly_TWh);
    Dem_HP_Tot_TWh          = sum(Dem_HP_monthly_TWh);
    
    %--------------------------------------------------------------------------
    % save processed data for DBcreation
    %--------------------------------------------------------------------------
    % hourly - original
    CalliopeToNexuse.DemandCompare.Orig_DemBase_CH_Profile_MWh          = CalliopeToNexuse.BaseElecDemand_hrly.CH';
    CalliopeToNexuse.DemandCompare.Orig_DemRail_CH_Profile_MWh          = CalliopeToNexuse.RailElecDemand_hrly.CH';
    CalliopeToNexuse.DemandCompare.Orig_DemH2_CH_Profile_MWh            = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_hydrogen';
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_noflex_CH_Profile_MWh   = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_noflex_Calliope';
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_flex_CH_Profile_MWh     = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_emobility_flex_Calliope';
    CalliopeToNexuse.DemandCompare.Orig_DemHP_CH_Profile_MWh            = CalliopeToNexuse.ElectrifiedDemands_hrly.CH_heatpump';
    % hourly - 8-day resample
    CalliopeToNexuse.DemandCompare.tpRes8day_DemBase_CH_Profile_MWh         = Dem_unpack_Base_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemRail_CH_Profile_MWh         = Dem_unpack_Rail_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemH2_CH_Profile_MWh           = Dem_unpack_H2_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_noflex_CH_Profile_MWh  = Dem_unpack_Emob_noflex_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_flex_CH_Profile_MWh    = Dem_unpack_Emob_flex_tpRes;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemHP_CH_Profile_MWh           = Dem_unpack_HP_tpRes;
    % monthly - original
    CalliopeToNexuse.DemandCompare.Orig_DemBase_CH_Monthly_TWh          = Dem_Base_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemRail_CH_Monthly_TWh          = Dem_Rail_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemH2_CH_Monthly_TWh            = Dem_H2_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_noflex_CH_Monthly_TWh   = Dem_Emob_noflex_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_flex_CH_Monthly_TWh     = Dem_Emob_flex_monthly_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemHP_CH_Monthly_TWh            = Dem_HP_monthly_TWh;
    % monthly - 8-day resample
    CalliopeToNexuse.DemandCompare.tpRes8day_DemBase_CH_Monthly_TWh         = Dem_unpack_Mthly_Base_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemRail_CH_Monthly_TWh         = Dem_unpack_Mthly_Rail_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemH2_CH_Monthly_TWh           = Dem_unpack_Mthly_H2_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_noflex_CH_Monthly_TWh  = Dem_unpack_Mthly_Emob_noflex_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_flex_CH_Monthly_TWh    = Dem_unpack_Mthly_Emob_flex_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemHP_CH_Monthly_TWh           = Dem_unpack_Mthly_HP_TWh;
    % annual - original
    CalliopeToNexuse.DemandCompare.Orig_DemBase_CH_Annual_TWh           = Dem_Base_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemRail_CH_Annual_TWh           = Dem_Rail_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemH2_CH_Annual_TWh             = Dem_H2_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_noflex_CH_Annual_TWh    = Dem_Emob_noflex_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemEmob_flex_CH_Annual_TWh      = Dem_Emob_flex_Tot_TWh;
    CalliopeToNexuse.DemandCompare.Orig_DemHP_CH_Annual_TWh             = Dem_HP_Tot_TWh;
    % annual - 8-day resample
    CalliopeToNexuse.DemandCompare.tpRes8day_DemBase_CH_Annual_TWh          = Dem_unpack_Yrly_Base_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemRail_CH_Annual_TWh          = Dem_unpack_Yrly_Rail_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemH2_CH_Annual_TWh            = Dem_unpack_Yrly_H2_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_noflex_CH_Annual_TWh   = Dem_unpack_Yrly_Emob_noflex_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemEmob_flex_CH_Annual_TWh     = Dem_unpack_Yrly_Emob_flex_TWh;
    CalliopeToNexuse.DemandCompare.tpRes8day_DemHP_CH_Annual_TWh            = Dem_unpack_Yrly_HP_TWh;
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


%% This function aggregates the demand data desired for one country and one carrier
%  note this function replaces the 3 functions:
%  Func_GetCalliopeDemandData_OneCountry
%  Func_GetCalliopeH2DemandData_OneCountry
%  Func_GetCalliopegasDemandData_OneCountry
%  All 3 have been deleted on the 30.5.24 in commit 'delete 3 Func and apply Func_GetCalliopeCARRIERDemandData_OneCountry' 

function varargout = Func_GetCalliopeCARRIERDemandData_OneCountry(table_data,idx_table_flowin,timesteps_num,idx_scenario1,idx_country,idx_CARRIERdem,varargin)
    % varargin  = [inx_CARRIER_SECTOR{1}, inx_CARRIER_SECTOR{2}, ...]
    % varargout = [data_CARRIERload_SECTOR{1}_Scen1_sum_hrly, data_CARRIERload_SECTOR{1}_Scen1_sum_halfyr, ...]

    % each time the function is applied, it's applied only for one CARRIER
    % (either elec, hydrogen or gas)

   
    % Electricity demand is devided in 6 SECORS and has therefor 6 input arguments:     
    %            idx_elecload_base     
    %            idx_elecload_hydrogen 
    %            idx_elecload_emobility
    %            idx_elecload_heatpump 
    %            idx_elecload_rail     
    %            idx_elecload_dac      
    % Hydrogen has following input arguments:
    % ev delete  idx_H2load_H2chp     
    %            idx_H2load_H2industry
    %            idx_H2load_H2mobility
    %            idx_H2load_H2liquids 
    %   TODO:    idx_H2load_H2export
    % Methane has following input arguments:
    %            idx_gasload_gasindustry    
    %            idx_gasload_gasresidential 
    %   TODO:    idx_gasload_gasexport



    % Check if the number of variable input arguments matches nargout
    if length(varargin) ~= nargout
        error('The number of sectors in input output must match.');
    end


    for k = 1:length(varargin) % k is refering to the SECTOR
        
        % create identifiers for given region and each demand
        % (picks the indices of the "flow_in-subtable" of table_data, which fullfill all, the correct technology, scenario, location & carrier)
        idx_CARRIERload_base_Scen1{k}       = varargin{k}         & idx_scenario1     & idx_country     & idx_CARRIERdem;

        % get demands for given region in given scenario
        % (copies the needed subsubtable)
        tabledata_CARRIERload_SECTOR_Scen1{k}      = table_data{idx_table_flowin}(idx_CARRIERload_base_Scen1{k},:);


        % old comment:
        % loop over each timestep to add for region (assumes same timesteps
        % in each type of data_CARRIERload), will add if multiple regions or if multiple
        % techs for each demand type or both

% THE FUNTION DOES NOT WORK FOR MULTIPLE REGIONS! OR COULD idx_country BE AN ARRAY? IT IS NEVER APPLIED LIKE THAT.
% QUESTION, DOES THE FUNCTION NEED TO BE EXTENDED TO MAKE IT WORK FOR MULTIPLE COUNTRIES?

        % NEW COMMENT:
        % loop over each time step
        % add up the demands for the multiple technologies within one sector
        % note: assumes same timesteps in each type of data_CARRIERload

        for i2 = 1:length(timesteps_num)
            % identify all rows with this timestep
            idx_t_CARRIERload_SECTOR{k}         = tabledata_CARRIERload_SECTOR_Scen1{k}.timesteps == timesteps_num(i2);
            % sum all entries for these timesteps
            varargout{k}(i2,1) = sum(tabledata_CARRIERload_SECTOR_Scen1{k}.flow_in(idx_t_CARRIERload_SECTOR{k}));
        % = data_CARRIERload_SECTOR_Scen1_sum_hrly{k}(i2,1) = varargout{k}(i2,1)
        
        end % done for one timestep

    end % done for 1 SECTOR (1 input argument)


end % end of function, done for 1 CARRIER


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
        if data_FixedInj_Scen_TWh(i11,i10)>1
            disp('timestep, technology');
        end

        %example:
        % It might happen, that  a technology eg.
        % 'chp_methane_extraction' does not have values at each hour of the
        % year! sum(idx_FixedInj_tech) < 8760
        % in this case the entry  data_FixedInj_Scen_TWh(i11,i10) will be 0

        % TODO:
%Question: Are there cases where we have more than one entry for one
%technology in one hour? only then there is a real summation!

        
        % get data for this tech type
        %data_FixedInj_Scen_TWh(:,i10) = tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech);
        % convert to MWh and round to nearest MWh
        %data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
        
    end % done for one timestep
    
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
        
    end % done for one technology
    
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

%  analog to Func_GetCalliopeGenerationData_OneCountry, just with conversion 100kt to Mt (instead of TWh to MWh)

function [techs_CO2_Capture, data_CO2_Capture_Scen_Mt, tabledata_CO2_Capture_Scen_Mt] = Func_GetCalliopeCo2captureData_OneCountry(table_data,idx_table_CO2_Capture,timesteps_num,idx_scenario,idx_country,idx_co2,table_capacities)
disp("function CO2 Capturing entered")
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
        data_CO2_Capture_Scen_100kt(i11,i10) = sum(tabledata_CO2_Capture_Scen.flow_out(idx_CO2_Capture_tech_t));
        
        % get data for this tech typegas
        %data_FixedInj_Scen_TWh(:,i10) = tabledata_FixedInj_Scen.flow_out(idx_FixedInj_tech);
        % convert to MWh and round to nearest MWh
        %data_FixedInj_Scen_MWh(:,i10) = Func_ConvertCalliopeProfile_TWh(data_FixedInj_Scen_TWh(:,i10),1);
        
    end
    
    if length(techs_CO2_Capture)>0
        % convert to million-tonne (in 100k-tonne) and round to nearest kg
        data_CO2_Capture_Scen_Mt(:,i10) = Func_ConvertCalliopeProfile_Mtonne(data_CO2_Capture_Scen_100kt(:,i10),9);
        %disp(class(data_CO2_Capture_Scen_Mt(:,i10)))
    else
        disp("ToD0: create empty data_CO2_Capture_Scen_Mt(:,i10)")
        data_CO2_Capture_Scen_Mt =[];
    end
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
%{
% check if any techs need to be removed
if counter1 > 0
    % remove any techs with no capacity defined (techs list and data)
    techs_CO2_Capture(techs_ToRemove) = [];
    data_CO2_Capture_Scen_Mt(:,techs_ToRemove) = [];
else
    % all techs are ok
end
 %}
if length(techs_CO2_Capture)>0
    % create table for saving all the CH profiles
    tabledata_CO2_Capture_Scen_Mt = array2table(data_CO2_Capture_Scen_Mt,'VariableNames',techs_CO2_Capture);
else
    disp("no CO2 techs available")
    tabledata_CO2_Capture_Scen_Mt =[];
end


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



    
%% Function Func_ConvertCalliopeProfiles_electrification did convert a set of profiles of every 2nd hour in TWh into a profile of every hour in MWh
% deleted in commit on 3.6.24 "delete Func_ConvertCalliopeProfiles_electrification. (obsolete with new CalliopeResults haveing data for every (instead of 2nd) hour)"



%% This function converts a set of hourly profiles in TWh into profiles in MWh
%  also rounds the values to the nearest desired decimal

% each time the function is applied, it's applied only for one CARRIER
% (either elec, hydrogen or gas)
% This function replaces the 3 old (per carrier individual) convertion functions on the 3.6.24 in commit 'delete 3 old and apply the new Func_ConvertCalliopeProfiles_TWh_demands' 


function varargout = Func_ConvertCalliopeProfiles_TWh_demands(varargin)
    % varargin  = [data_CARRIERload_SECTOR{1}_fullyr_TWh, data_CARRIERload_SECTOR{2}_fullyr_TWh, ...]
    % varargout = [data_CARRIERload_SECTOR{1}_fullyr_MWh, data_CARRIERload_SECTOR{2}_fullyr_MWh, ...]
    
    % Check if the number of variable input arguments matches nargout
    if length(varargin) ~= nargout
        error('The number of sectors in input output must match.');
    end

    for k = 1:length(varargin) % k is refering to the SECTOR
        % convert each profile to MWh and round to nearest MWh
        varargout{k}       = round(varargin{k} * 1000 * 1000,0);
    end % done for 1 SECTOR (1 input argument)


    %TODO (Think)

%-> When appliing the function I type:
% [data_H2load_chp_Scen1_CHsum_MWh, data_H2load_industry_Scen1_CHsum_MWh, data_H2load_mobility_Scen1_CHsum_MWh, data_H2load_liquids_Scen1_CHsum_MWh] = Func_ConvertCalliopeProfiles_TWh_demands
% (data_H2load_chp_Scen1_allCH_hrly_TWh,data_H2load_industry_Scen1_allCH_hrly_TWh,data_H2load_mobility_Scen1_allCH_hrly_TWh,data_H2load_liquids_Scen1_allCH_hrly_TWh)
% ? Why do we call the result a sum? (input allCH, result CHsum)


end % end of function, done for 1 CARRIER

%% This function converts a set of hourly profiles in TWh into profiles in MWh
% But it does not round. Needed to find the chp fraction of heat output

function varargout = Func_ConvertCalliopeProfiles_TWh_demands_NotRounded(varargin)
    % varargin  = [data_CARRIERload_SECTOR{1}_fullyr_TWh, data_CARRIERload_SECTOR{2}_fullyr_TWh, ...]
    % varargout = [data_CARRIERload_SECTOR{1}_fullyr_MWh_NotRounded, data_CARRIERload_SECTOR{2}_fullyr_MWh_NotRounded, ...]
    
    % Check if the number of variable input arguments matches nargout
    if length(varargin) ~= nargout
        error('The number of sectors in input output must match.');
    end

    for k = 1:length(varargin) % k is refering to the SECTOR
        % convert each profile to MWh and round to nearest MWh
        varargout{k}       = (varargin{k} * 1000 * 1000);
    end % done for 1 SECTOR (1 input argument)

end % end of function, done for 1 CARRIER


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

% convert each profile to Million-tonne and round based on passed rnd_to # decimals
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






