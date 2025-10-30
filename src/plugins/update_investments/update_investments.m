function update_investments(parameters, databaseConnection, resultsFolder)
    CentIvToMYSQL = load_centiv_results(resultsFolder);
    FuncNexuse_Database_UpdateInvestments( ...
        parameters, databaseConnection, CentIvToMYSQL);
end

function centivResults = load_centiv_results(resultsFolder)
    % Extracted from Centiv/matlab/cgepiModule.m
    
    % Nodal generation of DistIv for all Swiss nodes for eMark (has hourly 
    % resolution for a full year)
    T_DistIvGeneration = readtable_robust( ...
        fullfile(resultsFolder, 'GenerationDistIv_hourly_CH.csv'));
    T_DistIvGeneration = T_DistIvGeneration(2:end,2:end);

    % New investments (generators) - CH
    T_NewInvestments = readtable_robust( ...
        fullfile(resultsFolder, 'NewUnits.xlsx'));
    T_NewInvestments = T_NewInvestments(:,2:end);                

    % Additional reserve - CH
    T_ReserveArguments = readtable_robust( ...
        fullfile(resultsFolder, 'AddReserve_Args.xlsx'));
    T_ReserveArguments = T_ReserveArguments(:,2:end);

    % New investments (branches) - only built
    % This file contains only candidate lines which are built
    T_NewLineInvestOnlyBuilt = readtable_robust( ...
        fullfile(resultsFolder, 'NewLinesOnlyOneStatus.xlsx'));

    % generator investments
    if (~isempty(T_NewInvestments))
        % GenName column from database for newly built units
        centivResults.has_new_gen = true;
        centivResults.GenName = T_NewInvestments.GenName;
        centivResults.Pmax = T_NewInvestments.Pmax; %in MW
        centivResults.Pmin = T_NewInvestments.Pmin; %in MW
    else
        % no new generators, so create blank entries
        centivResults.has_new_gen = false;
        centivResults.GenName = {''};
        centivResults.Pmax = [];
        centivResults.Pmin = [];
    end
    % branch investments (transformers and transmission lines)
    if (~isempty(T_NewLineInvestOnlyBuilt))
        % LineName column from database for newly built branch elements
        centivResults.has_new_line = true;
        centivResults.LineBuiltName = T_NewLineInvestOnlyBuilt.LineName;
    else
        % no new lines, so create blank entries
        centivResults.has_new_line = false;
        centivResults.LineBuiltName = {''};
    end
    % added tertiary reserves
    ArgWindUp = T_ReserveArguments.Multiplier_WindUP; % [-]
    ArgWindDown = T_ReserveArguments.Multiplier_WindDOWN; % [-]
    ArgSolarUp = T_ReserveArguments.Multiplier_SolarUP; % [-]
    ArgSolarDown = T_ReserveArguments.Multiplier_SolarDOWN; % [-]

    % in MW (installed PV capacity @ distribution level)
    DistIvSolarInstalled = T_ReserveArguments.DistIv_Solar_MW;

    % in MW (installed solar capacity @ transmission level)
    CentIvSolarInstalled = T_ReserveArguments.CentIv_Solar_MW;

    % in MW (installed Wind capacity @ transmission level)
    CentIvWindInstalled = T_ReserveArguments.CentIv_Wind_MW;

    % Hourly additional TCR due to new solar/wind
    centivResults.TCR_UP_add = ...
        ArgSolarUp * (DistIvSolarInstalled + CentIvSolarInstalled) ...
        + ArgWindUp * CentIvWindInstalled; % in MW

    % Hourly additional TCR due to new solar/wind)
    centivResults.TCR_DOWN_add = ...
        ArgSolarDown * (DistIvSolarInstalled + CentIvSolarInstalled) ...
        + ArgWindDown * CentIvWindInstalled; % in MW

    centivResults.Generation_DistIv = T_DistIvGeneration;
end     

function output_table = readtable_robust(filename)
    % This function is created to have a robust way of reading CSV files.
    
    % According to https://ch.mathworks.com/help/matlab/ref/readtable.html:
    % "Starting in R2020a, the readtable function read an input
    % file as though it automatically called the detectImportOptions 
    % function on the file. It can detect data types, discard extra 
    % header lines, and fill in missing values."
    
    % If the Matlab version is older than R2020a (version number: 9.8)
    if verLessThan('matlab', '9.8')
        output_table = readtable(filename);
    else % If newer than R2020a.
        output_table = readtable(filename, 'Format','auto');
    end
end    