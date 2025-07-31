% =========================================================================
%>
%> (c)ETH Zurich 2020
%>
%> 
%> 
%> author : Jared Garrison
%> email: garrison@fen.ethz.ch
%>
%> project : Nexus-e
%> 
%> 
%> 
%>
%> ========================================================================
%> 
%> @brief function to update MySQL input data with new generator
%>        investments and added reserve requirements
%> 
%> DistIv Profiles: will update the profiles only for the nodes with
%>        profiles provided from CentIv interface
%> 
%> @note  the order of the invested generators coming from the
%>        CentIvToMySQL does not matter, the GenName and Pmax are
%>        immediately reordered to match the order in the data pulled from 
%>        the database
%> 
%> 
%> 
% =========================================================================

function [ ] = FuncNexuse_Database_UpdateInvestments(wkspace,conn,CentIvToMySQL)
    % This Function pushes updates to the MySQL database so new generators,
    % and other data are used as input into the next scenario-year
    % simulation within a given scenario.
    
    % The updates include:
    %   1) Dispatchable Gens  (fully built)
    %      A) add to gendata: create new Gens (rows) for the fully Invested
    %         unit, change GenName to "_Inv" instead of "_Cd", also change  
    %         idGen number, StartYr and EndYr, add this entry to the end of 
    %         gendata
    %      B) edit genconfiguration: edit all appropriate years, change 
    %         candidate unit status to 0, and also modify for new idGen and 
    %         GenName; Note, no need to modify idProfile b/c full capacity 
    %         is built
    %   2) Dispatchable Gens (partially built)
    %      A) add to gendata, create new Gens (rows) for both the Invested 
    %         portion (name should be "_Inv1" with "_PtInv" at end) and  
    %         remaining Candidate portion (name should be "_Cd1" with 
    %         "_PtCd" at end), added both to end of gendata
    %      B) calc capacities for invested and remaining portion
    %      C) For Inv portion: must add gens in genconfiguration for all 
    %         appropriate configs
    %         1) will edit idGen, GenName, CandidateUnit=0, Pmax, InvCost; 
    %         2) must check and create new profiles (if profile is in MWh)
    %            for any unit with a profile, push new profiles for both
    %            Inv and Cd portion to profiledata table, update idProfile 
    %            in GenConfig for Inv portion
    %         3) insert new entries for this Inv gen's info to all
    %            genconfigurations
    %      D) For Cd portion: must update gens in genconfiguration for all
    %         appropriate configs; modify the idGen, GenName and Pmax; if
    %         any gen has a profile in MWh then edit the idProfile based
    %         on the new profiles created in step C2
    %   3) NonDispatchable Gens (fully built)
    %      A) add to gendata: create new Gens (rows) for the fully Invested
    %         unit, change GenName to "_Inv" instead of "_Cd", also change  
    %         idGen number, StartYr and EndYr, add this entry to the end of 
    %         gendata
    %      B) edit genconfiguration: edit all appropriate years, change 
    %         candidate unit status to 0, and also modify for new idGen and 
    %         GenName; Note, no need to modify idProfile b/c full capacity 
    %         is built
    %   4) NonDispatchable Gens (partially built)
    %      A) add to gendata, create new Gens (rows) for both the Invested 
    %         portion (name should be "_Inv1" with "_PtInv" at end) and  
    %         remaining Candidate portion (name should be "_Cd1" with 
    %         "_PtCd" at end), added both to end of gendata
    %      B) calc capacities for invested and remaining portion
    %      C) For Inv portion: must add gens in genconfiguration for all 
    %         appropriate configs
    %         1) will edit idGen, GenName, CandidateUnit=0, Pmax, InvCost; 
    %         2) must check and create new profiles (if profile is in MWh)
    %            for any unit with a profile, push new profiles for both
    %            Inv and Cd portion to profiledata table, update idProfile 
    %            in GenConfig for Inv portion
    %         3) insert new entries for this Inv gen's info to all
    %            genconfigurations
    %      D) For Cd portion: must update gens in genconfiguration for all
    %         appropriate configs; modify the idGen, GenName and Pmax; if
    %         any gen has a profile in MWh then edit the idProfile based
    %         on the new profiles created in step C2
    
    %      - add to gendata, create new Gens (rows) for the Invested 
    %        portion, name should be "_Inv", added to end of gendata
    %      - calc capacities for invested and remaining portion
    %      - edit profiledata, for remaining Candidate portion of all
    %        partially built (only if profiles are in MWh)
    %      - add to profiledata, for all Invested portion (new Gens)
    %      - add to genconfiguration, add new Invested gens to all needed 
    %        GenConfigurations with correct idGen, GenName, CandidateUnit, 
    %        idProfile, and Pmax
    %      - update genconfiguration, for all remaining Candidate gens to
    %        all needed GenConfigurations with correct GenName and Pmax
    
    %   5) Reserve Profiles
    %      - edit profiledata, for Tertiary Up & Down to include the added
    %        amount determined in CentIv based on newly added Wind & PV
    %        capacities
    %   6) DistIv Generation Profiles
    %      - edit distprofiles, for every node replace profile to reflect
    %        the generation in DistIv at each node (note this should only
    %        include the injection from generation but not the load
    %        shifting from DSM nor BSS)
    %   7) CentIv Transmission Expansion
    %      - 
    
    
    
    % 
    %conn.AutoCommit = 'off';
    
    disp('_______________________________________________________________')
    disp(['Updates to MySQL include... '])

    python_worflow_is_running = all( ...
        isfield(CentIvToMySQL, ["has_new_gen", "has_new_line"]));
    if python_worflow_is_running
        % When running this process with the Matlab engine for Python the values
        % in CentIvToMySQL.GenName and CentIvToMySQL.LineBuiltName when there is
        % no investment are transformed from {''} to {[]}. Here we change them 
        % back.
        if ~CentIvToMySQL.has_new_gen
            CentIvToMySQL.GenName = {''};
        end
        if ~CentIvToMySQL.has_new_line
            CentIvToMySQL.LineBuiltName = {''};
        end
    end

    %% Get scenario data from database
    
    scenId = wkspace.scenId;    % get the correct scenario ID
    
    confignum_start = wkspace.startyear;
    confignum_end = wkspace.endyear;
    
    % retrieve scenario information from DB
    ScenData = fetch(conn,['SELECT name,idNetworkConfig,idMarketsConfig,idGenConfig,idLoadConfig,idAnnualTargetsConfig,JSON_PRETTY(runParamDataStructure) as runParamDataStructure,Year FROM scenarioconfiguration WHERE idScenario=',int2str(scenId),';']);
    %scen_name = cell2mat(ScenData.name);
    idNetworkConfig = ScenData.idNetworkConfig;
    %idMarketsConfig = ScenData.idMarketsConfig;
    idGenConfig = ScenData.idGenConfig;
    %idLoadConfig = ScenData.idLoadConfig;
    %idRenewTargetConfig = ScenData.idRenewTargetConfig;
    Year = wkspace.scenYear;
    dbName = wkspace.dbName;
    
    %% Update Generators - identify
    
    %>>>>>>>>>>------------------------------------------------------------
    % Identify which GenConfigs should be updated based on the simulation
    % year
    %>>>>>>>>>>------------------------------------------------------------
    
    % Gen Config Info
    % get any existing data from genConfigInfo
    GenConfigInfo_Data = select(conn,['SELECT * FROM ',dbName,'.genconfiginfo']);
    
    % determine all GenConfig to update (current and future years)
    ToUpdate_GenConfigs = GenConfigInfo_Data.idGenConfig( GenConfigInfo_Data.year >= Year );
    
    % remove ToUpdate_GenConfigs any configs that were not actually
    % simulated
    % remove any configs before start year config
    ToUpdate_GenConfigs(ToUpdate_GenConfigs < confignum_start) = [];
    % remove any configs after end year config
    ToUpdate_GenConfigs(ToUpdate_GenConfigs > confignum_end) = [];
    %------------------------------------------------------------<<<<<<<<<<
    
    disp([' ->Gen Configurations to edit: '])
    for y1=1:length(ToUpdate_GenConfigs)
        disp(['    Year = ',num2str(GenConfigInfo_Data.year(ToUpdate_GenConfigs(y1))),', idGenConfig = ',num2str(ToUpdate_GenConfigs(y1))])
    end
    
    %>>>>>>>>>>------------------------------------------------------------
    % Determine which gens to update are dispatchable or non-dispatchable
    %>>>>>>>>>>------------------------------------------------------------
    
    % get gen data from SQl
    genTable = getGenData(conn,idGenConfig);
    
    % reorder the data for new investments to be in proper order according
    % to the genTable order
    [zdummy, order_inv_all] = ismember(genTable.GenName, CentIvToMySQL.GenName);
    % remove all non-relevant entries
    order_inv_all(order_inv_all==0) = [];
    % reorder all appropriate Invested data provided over this interface
    % this is currently only the .GenName and .Pmax
    CentIvToMySQL.GenName = CentIvToMySQL.GenName(order_inv_all);
    CentIvToMySQL.Pmax    = CentIvToMySQL.Pmax(order_inv_all);
    CentIvToMySQL.Pmin = CentIvToMySQL.Pmin(order_inv_all);
    
    % *****EDIT*****
    % may need to add a rounding of the Pmax from CentIv because of a
    % trailing decimal issue
    CentIvToMySQL.Pmax    = round(CentIvToMySQL.Pmax,2);
    
    % get the Unit type ('Dispatchable' or 'NonDispatchable'), dispatchable
    % have discret candidates and if invested only need to change the
    % candidate binary to a 0, non-dispatchable have continuous candidates
    % and if invested need to be copied and both original and copy must
    % edit the Pmax,
    % order of this must match the order of CentIvToMySQL (must be sure
    % just in case the order of genTable is not same as order of
    % CentIvToMySQL)
    GensInv_GenName = genTable.GenName( find(cell2mat(cellfun(@(x) ismember(x, CentIvToMySQL.GenName), genTable.GenName, 'UniformOutput', 0))) );
    GensInv_UnitType = genTable.UnitType( find(cell2mat(cellfun(@(x) ismember(x, CentIvToMySQL.GenName), genTable.GenName, 'UniformOutput', 0))) );
    % get correct order of gens in CentIvToMySQL
    %[zdummy, order_inv_temp] = ismember(CentIvToMySQL.GenName, GensInv_GenName(:, 1));
    % reorder the UnitType to match CentIvToMySQL
    %GensInv_UnitType = GensInv_UnitType(order_inv_temp);
    
    % initialize idx as false
    idx_disp = false;
    idx_nondisp = false;
    % identify those that are Dispatchable vs Non-Dispatchable
    idx_disp    = cell2mat(cellfun(@(x) ismember(x, {'Dispatchable'}), GensInv_UnitType, 'UniformOutput', 0));
    idx_nondisp = ~idx_disp;
    %------------------------------------------------------------<<<<<<<<<<
    
    
    % get the original genconfiguraiton table data
    GenConfigurationData = fetch(conn,['SELECT * FROM genconfiguration']);
    % get the original gendata table data
    GenData = fetch(conn,['SELECT * FROM gendata']);
    % get the largest idGen (in case I need to define a new generator and 
    % must not use an existing idGen even if not in this genconfig)
    idGen_last = max(GenData.idGen);
    
    %% Update Generators - dispatchable
    %>>>>>>>>>>------------------------------------------------------------
    % Update all Dispatchable Gens - only discrete investments version
    %>>>>>>>>>>------------------------------------------------------------
    % For Dispatchable gens, just change the binary for 'CandidateUnit' in
    % the genconfiguration table for all necessary GenConfigs, also edit 
    % the GenName)
    
    %{
    disp([' ->Invested Dispatchable Gens:'])
    
    % test if any dispatchable gens need to be updated
    if sum(idx_disp) > 0
        
        % get gen names of these to update
        genNames_disp = CentIvToMySQL.GenName(idx_disp);
        
        % get the Pmax of these to update
        disp_Pmax = CentIvToMySQL.Pmax(idx_disp);
        
        % also adjust the name of the built generator (change from Cd
        % to Inv)
        Str_beg = extractBefore(genNames_disp,"Cd"); 	% get the gen name string before the 'Cd' (will use this exactly in the new name for the invested)
        Str_end = extractAfter(genNames_disp,"Cd");      % get the gen name string after the 'Cd' (will use this exactly in the new name for the invested)
        Str_name = strcat(Str_beg,'Inv',Str_end); 	% create new name for the new Invested generator (replace 'Cd' with 'Inv')
        
        % define info for MySQL table to be updated
        tablename1 = 'genconfiguration';
        colnames1 = {'GenName','CandidateUnit'};
        %newdata1 = {0};
        
        % loop over all GenConfigs that need to be updated
        for c1=1:length(ToUpdate_GenConfigs)
            
            % loop over all dispatchable gens to update
            for g1=1:sum(idx_disp)
                
                % define new data to insert for this gen
                newdata1 = {Str_name{g1},0};
                
                % define WHERE clause for this update
                whereclause1 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_disp{g1},'")')};
                
                % send this update to the database
                update(conn,tablename1,colnames1,newdata1,whereclause1)
                
                % don't repeat display
                if c1==1
                    disp(['    Pmax = ',num2str(disp_Pmax(g1)),' MW, ',Str_name{g1}])
                end
                
            end
            
        end
        
        
        
    else
        % no updates needed to Dispatchable generators
        disp(['    none'])
    end
    %------------------------------------------------------------<<<<<<<<<<
    %}
    
    %% Update Generators - dispatchable
    %>>>>>>>>>>------------------------------------------------------------
    % Update all Dispatchable Gens - discrete and continuous version
    %>>>>>>>>>>------------------------------------------------------------
    
    disp([' ->Invested Dispatchable Gens:'])
    
    % test if any dispatchable gens need to be updated
    if sum(idx_disp) > 0
        
        % get gen names of these to update
        genNames_disp = CentIvToMySQL.GenName(idx_disp);
        
        % get full Pmax of these invested disp gens
        GensInv_disp_GenName = genTable.GenName( find(cell2mat(cellfun(@(x) ismember(x, genNames_disp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_disp_Pmax    = genTable.Pmax(    find(cell2mat(cellfun(@(x) ismember(x, genNames_disp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_disp_Pmin    = genTable.Pmin(    find(cell2mat(cellfun(@(x) ismember(x, genNames_disp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_disp_Emax    = genTable.Emax(    find(cell2mat(cellfun(@(x) ismember(x, genNames_disp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_disp_Emin    = genTable.Emin(    find(cell2mat(cellfun(@(x) ismember(x, genNames_disp), genTable.GenName, 'UniformOutput', 0))) );
        % get correct order of gens in CentIvToMySQL
        %[zdummy, order_inv_disp] = ismember(genNames_disp, GensInv_disp_GenName(:, 1));
        % reorder the Pmax to match CentIvToMySQL
        %GensInv_disp_Pmax = GensInv_disp_Pmax(order_inv_disp);
        
        % identify all disp that are NOT fully built
        idx_disp_partially = true(sum(idx_disp),1);     % initialize all as partially built
        idx_disp_partially(GensInv_disp_Pmax==CentIvToMySQL.Pmax(idx_disp) & GensInv_disp_Pmin==CentIvToMySQL.Pmin(idx_disp)) = false;   % change for any that are fully built, must check both Pmax and Pmin since DAC only has Pmin
        % identify all disp that ARE fully built
        idx_disp_fully = ~idx_disp_partially;
        
        % FIRST: update for any FULLY built DISP gens
        %   A) must add new generator to gendata with new idGen, GenName,
        %      StartYr, EndYr
        %   B) must edit gens in genconfiguration for all appropriate
        %      configs; will edit idGen, GenName, CandidateUnit=0;
        %      Note, no need to modify idProfile b/c full capacity is built
        %------------------------------------------------------------------
        
        disp([' ->Invested Dispatchable Gens (fully built):'])
        
        % test if any dispatchable gens are FULLY built
        if sum(idx_disp_fully) > 0
            
            % get gen names of these to update
            genNames_disp_fully = genNames_disp(idx_disp_fully);
            
            % get the Pmax & Pmin of these to update
            disp_fully_Pmax = GensInv_disp_Pmax(idx_disp_fully);
            disp_fully_Pmin = GensInv_disp_Pmin(idx_disp_fully);
            
            % also adjust the name of the built generator 
            % (change from Cd to Inv)
            Str_beg = extractBefore(genNames_disp_fully,"Cd"); 	% get the gen name string before the 'Cd' (will use this exactly in the new name for the invested)
            Str_end = extractAfter(genNames_disp_fully,"Cd"); 	% get the gen name string after the 'Cd' (will use this exactly in the new name for the invested)
            Str_name = strcat(Str_beg,'Inv',Str_end);         	% create new name for the new Invested generator (replace 'Cd' with 'Inv')
            
            
            % Step A: add new gens (Invested) to gendata table
            %--------------------------------------------------------------
            % copy from candidate and only need to change idGen number, 
            % GenName, StYr, EndYr,
            
            % reset the original gendata table data
            GenData = fetch(conn,['SELECT * FROM gendata']);
            
            % reset the largest idGen (need to define a new generator and 
            % must not use an existing idGen even if not in this genconfig)
            idGen_last = max(GenData.idGen);
            
            % define the idGen for all new generators that need to be created
            idGen_new = (idGen_last+1:idGen_last+sum(idx_disp_fully))';
            
            % get the gendata for all these gens from the Cd's in gendata,
            % no need to resort because it gets data one generator at a
            % time
            for ig = 1:length(genNames_disp_fully)
                GenData_new_disp_fully(ig,:) = GenData( find(cell2mat(cellfun(@(x) ismember(x, genNames_disp_fully(ig)), GenData.GenName, 'UniformOutput', 0)),1,'first'),: );
            end
            % overwrite the idGen with the new ones
            GenData_new_disp_fully.idGen = idGen_new;
            % overwrite the StartYr to be the year it's built
            GenData_new_disp_fully.StartYr = repmat(Year,[length(idGen_new),1]);
            % overwrite EndYr to be 2100
            GenData_new_disp_fully.EndYr = repmat(2100,[length(idGen_new),1]);
            % overwrite the GenNames with the new Invested ones
            GenData_new_disp_fully.GenName = Str_name;
            % replace NaN in eta_dis,eta_ch with BLANKS
            vars = {'eta_dis','eta_ch'};
            GenData_new_disp_fully.eta_dis = num2cell(GenData_new_disp_fully.eta_dis);    % for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenData_new_disp_fully.eta_ch = num2cell(GenData_new_disp_fully.eta_ch);      % for some reason I needed to convert these columns to cell arrays of double instead of just double
            ztemp = GenData_new_disp_fully{:,vars};
            ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
            GenData_new_disp_fully{:,vars} = ztemp; clear ztemp;
            
            % get the column names of the GenData table, needed to use
            % datainsert to put new data back into MySQL
            colNames = GenData_new_disp_fully.Properties.VariableNames;
            
            % push new generator data to add to the gendata MySQl table
            datainsert(conn,'gendata',colNames,GenData_new_disp_fully);
            
            % display info about newly created gens
            for d1=1:length(genNames_disp_fully)
                disp(['    Added Fully Built to gendata: idGen = ',num2str(GenData_new_disp_fully.idGen(d1)),', ','Pmax = ',num2str(disp_fully_Pmax(d1)),', ','Pmin = ',num2str(disp_fully_Pmin(d1)),', ',GenData_new_disp_fully.GenName{d1}])
            end
            
            
            % Step B: add the new gens (Invested) to each genconfiguration
            %--------------------------------------------------------------
            % modify idGen,GenName,CandidateUnit
            % (no need to modify idProfile b/c full capacity is built)
            
            % define info for MySQL table to be updated
            tablename1 = 'genconfiguration';
            colnames1 = {'idGen','GenName','CandidateUnit'};
            
            % loop over all GenConfigs that need to be updated
            for c1=1:length(ToUpdate_GenConfigs)
                
                % loop over all dispatchable fully built gens to update
                for g1=1:sum(idx_disp_fully)
                    
                    % define new data to insert for this gen 
                    % (idGen & GenName & CandidateStatus=0)
                    newdata1 = {idGen_new(g1),Str_name{g1},0};
                    
                    % define WHERE clause for this update
                    whereclause1 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_disp_fully{g1},'")')};
                    
                    % send this update to the database
                    update(conn,tablename1,colnames1,newdata1,whereclause1)
                    
                    % don't repeat display
                    if c1==1
                        disp(['    Edit  Fully Built in genconfigs: idGen = ',num2str(idGen_new(g1)),', Pmax = ',num2str(disp_fully_Pmax(g1)),' MW, ','Pmin = ',num2str(disp_fully_Pmin(d1)),' MW, ',Str_name{g1}])
                    end
                    
                end
                
            end
            
        else
            % no updates needed to Dispatchable FULLY built generators
            disp(['    none'])
        end
        %------------------------------------------------------------------
        %}
        
        % SECOND: update for any PARTIALLY built DISP gens 
        % (need to copy gendata info and update for both Inv and Cd)
        %   A) must add new generators to gendata with new idGen, GenName,
        %      StartYr, EndYr (both new partial candidate and new partial
        %      invested)
        %   B) Calc the Inv and Cd Pmax/Pmin portions
        %   C) For Inv portion: must add gens in genconfiguration for all 
        %      appropriate configs
        %      1) will edit idGen, GenName, CandidateUnit=0, Pmax, Pmin, 
        %         InvCost; 
        %      2) must check and create new profiles (if profile is in MWh)
        %         for any unit with a profile, push new profiles for both
        %         Inv and Cd portion to profiledata table, update idProfile 
        %         in GenConfig for Inv portion
        %      3) insert new entries for this Inv gen's info to all
        %         genconfigurations
        %   D) For Cd portion: must update gens in genconfiguration for all
        %      appropriate configs; modify the idGen, GenName, Pmax and 
        %      Pmin; if any gen has a profile in MWh then edit the 
        %      idProfile based on the new profiles created in step C2
        %------------------------------------------------------------------
        
        disp([' ->Invested Dispatchable Gens (partially built):'])
        
        % test if any dispatchable gens are PARTIALLY built
        if sum(idx_disp_partially) > 0
            
            % get gen names of these to update
            genNames_disp_partially = genNames_disp(idx_disp_partially);
            
            
            % Step A: add new gens (Invested & remaining Candidate) to 
            % gendata table
            %--------------------------------------------------------------
            % copy from candidate and only need to change idGen number, 
            % GenName, StYr, EndYr;
            % for each partially invested unit I must add TWO new gen to
            % the gendata table 
            
            % reset the original gendata table data
            GenData = fetch(conn,['SELECT * FROM gendata']);
            
            % reset the largest idGen (need to define two new generators 
            % for each partially built unit and must not use an existing 
            % idGen even if not in this genconfig)
            idGen_last = max(GenData.idGen);
            
            % define the idGen for all new generators that need to be created
            idGen_new = (idGen_last+1:idGen_last+2*sum(idx_disp_partially))';
            
            % initialize index for the Inv and Cd portion
            idx_disp_partially_inv = false(2*length(genNames_disp_partially),1);
            idx_disp_partially_cd  = false(2*length(genNames_disp_partially),1);
            
            % get the gendata for all these gens from the Cd's in gendata
            % no need to resort because it gets data one generator at a
            % time
            % must create two entries since I will define a new unit in
            % GenData for both the Inv and Cd portion
            for ig = 1:length(genNames_disp_partially)
                idx_sta = 2*ig - 1;
                idx_end = idx_sta + 1;
                GenData_new_disp_partially(idx_sta,:) = GenData( find(cell2mat(cellfun(@(x) ismember(x, genNames_disp_partially(ig)), GenData.GenName, 'UniformOutput', 0)),1,'first'),: );
                GenData_new_disp_partially(idx_end,:) = GenData( find(cell2mat(cellfun(@(x) ismember(x, genNames_disp_partially(ig)), GenData.GenName, 'UniformOutput', 0)),1,'first'),: );
                % also need an indicator for which of these newly defined
                % gens in GenData are the Inv or Cd portion
                idx_disp_partially_inv(idx_sta) = true;
                idx_disp_partially_cd(idx_end) = true;
            end
            % overwrite the idGen with the new ones
            GenData_new_disp_partially.idGen = idGen_new;
            % overwrite the StartYr to be the year it's built
            GenData_new_disp_partially.StartYr = repmat(Year,[length(idGen_new),1]);
            % overwrite EndYr to be 2100
            GenData_new_disp_partially.EndYr = repmat(2100,[length(idGen_new),1]);
            % also adjust the name of the built generator (change from Cd
            % to Inv)
            Str_beg_Inv = extractBefore(genNames_disp_partially,"Cd");      % get the gen name string before the 'Cd' (will use this exactly in the new name for both invested and candidate)
            Str_end_Inv = extractAfter(genNames_disp_partially,"Cd");       % get the gen name string after the 'Cd' (will use this exactly in the new name for the invested)
            Str_name_Inv = strcat(Str_beg_Inv,'Inv',Str_end_Inv,"_PtInv");	% create new name for the new Invested generator
            Str_beg_Cd = genNames_disp_partially;                           % use the full original name  
            Str_name_Cd = strcat(Str_beg_Cd,'_PtCd');                       % add a '_PtCd' for each time a portion remains as a candidate
            % need to get names in correct order
            for ig =  1:length(genNames_disp_partially)
                idx_sta = 2*ig - 1;
                idx_end = idx_sta + 1;
                Str_name_InvCd(idx_sta,1) = Str_name_Inv(ig);
                Str_name_InvCd(idx_end,1) = Str_name_Cd(ig);
            end
            
            % overwrite the GenNames with the new Invested ones
            GenData_new_disp_partially.GenName = Str_name_InvCd;
            % replace NaN in eta_dis,eta_ch with BLANKS
            vars = {'eta_dis','eta_ch'};
            GenData_new_disp_partially.eta_dis = num2cell(GenData_new_disp_partially.eta_dis);    % for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenData_new_disp_partially.eta_ch = num2cell(GenData_new_disp_partially.eta_ch);      % for some reason I needed to convert these columns to cell arrays of double instead of just double
            ztemp = GenData_new_disp_partially{:,vars};
            ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
            GenData_new_disp_partially{:,vars} = ztemp; clear ztemp;
            
            % get the column names of the GenData table, needed to use
            % datainsert to put new data back into MySQL
            colNames = GenData_new_disp_partially.Properties.VariableNames;
            
            % push new generator data to add to the gendata MySQl table
            datainsert(conn,'gendata',colNames,GenData_new_disp_partially);
            
            % display info about newly created gens
%             for d1=1:length(genNames_disp_partially)
%                 idx_sta = 2*d1 - 1;
%                 idx_end = idx_sta + 1;
%                 disp(['    Added Partial Newly Built to gendata: idGen = ',num2str(GenData_new_disp_partially.idGen(idx_sta)),', ','Pmax = ',num2str(Pmax_inv_disp(d1)),', ',GenData_new_disp_partially.GenName{idx_sta}])
%                 disp(['    Added Partial Candidate   to gendata: idGen = ',num2str(GenData_new_disp_partially.idGen(idx_end)),', ','Pmax = ',num2str(Pmax_cd_disp(d1)) ,', ',GenData_new_disp_partially.GenName{idx_end}])
%             end
            %--------------------------------------------------------------
            
            
            % Step B: Calc the Inv and Cd portion power capacities
            %--------------------------------------------------------------
            
            % calculate the Pmax_inv and Pmax_cd
            Pinv_disp = CentIvToMySQL.Pmax(idx_disp);
            Pmax_orig_disp = GensInv_disp_Pmax(idx_disp_partially);
            Pmax_inv_disp = Pinv_disp(idx_disp_partially);
            Pmax_cd_disp = Pmax_orig_disp - Pmax_inv_disp;
            % FOR other parameters: Pmin, Emax, Emin...
            % OPTION 1: calculate the Pmin_inv and Pmin_cd (for any newly built
            % storages)
            % THIS DOESN'T WORK NOW because CentIv only sends 0's for Pmin
            % of partially built conventionals (GasCC) and DB has nonZero
            % as original Pmin for these candidates. Need to check if these
            % gas units are supposed to be continuous investment decisions
            % and if so set Pmin=0 in database
            %Pinv_disp_min = CentIvToMySQL.Pmin(idx_disp);
            %Pmin_orig_disp = GensInv_disp_Pmin(idx_disp_partially);
            %Emax_orig_disp = GensInv_disp_Emax(idx_disp_partially);
            %Emin_orig_disp = GensInv_disp_Emin(idx_disp_partially);
            %Pmin_inv_disp1 = Pinv_disp_min(idx_disp_partially);
            %Pmin_cd_disp1 = GensInv_disp_Pmin(idx_disp_partially) - Pmin_inv_disp1;
            % OPTION 2: assume the Pmax ratio of invested/original is same
            % for Pmin, round to 1 decimal
            % THIS CREATES AN ERROR for any gens with negative Pmin (like
            % batteries and DAC)
            %Pmin_orig_disp = GensInv_disp_Pmin(idx_disp_partially);
            %Pmin_inv_disp2 = round(Pmin_orig_disp .* (Pmax_inv_disp ./ Pmax_orig_disp),1);
            %Pmin_cd_disp2 = round(Pmin_orig_disp .* (Pmax_cd_disp ./ Pmax_orig_disp),1);
            % OPTION 3: calculate the Pmin_inv and Pmin_cd similar to Pmax
            PminInvestmentForDispatchables = CentIvToMySQL.Pmin(idx_disp);
            Pmin_orig_disp = GensInv_disp_Pmin(idx_disp_partially);
            Pmin_inv_disp2 = PminInvestmentForDispatchables(idx_disp_partially);
            Pmin_cd_disp2 = Pmin_orig_disp - Pmin_inv_disp2;
            % also make same calculation for Emax_inv / Emax_cd
            Emax_orig_disp = GensInv_disp_Emax(idx_disp_partially);
            Emax_inv_disp2 = round(Emax_orig_disp .* (Pmax_inv_disp ./ Pmax_orig_disp),1);
            Emax_cd_disp2 = round(Emax_orig_disp .* (Pmax_cd_disp ./ Pmax_orig_disp),1);
            % also make same calculation for Emin_inv / Emin_cd
            Emin_orig_disp = GensInv_disp_Emin(idx_disp_partially);
            Emin_inv_disp2 = round(Emin_orig_disp .* (Pmax_inv_disp ./ Pmax_orig_disp),1);
            Emin_cd_disp2 = round(Emin_orig_disp .* (Pmax_cd_disp ./ Pmax_orig_disp),1);
            
            % display info about newly created gens
            for d1=1:length(genNames_disp_partially)
                idx_sta = 2*d1 - 1;
                idx_end = idx_sta + 1;
                disp(['    Added Partial Newly Built to gendata: idGen = ',num2str(GenData_new_disp_partially.idGen(idx_sta)),', ','Pmax = ',num2str(Pmax_inv_disp(d1)),', ','Pmin = ',num2str(Pmin_inv_disp2(d1)),', ','Emax = ',num2str(Emax_inv_disp2(d1)),', ','Emin = ',num2str(Emin_inv_disp2(d1)),', ',GenData_new_disp_partially.GenName{idx_sta}])
                disp(['    Added Partial Candidate   to gendata: idGen = ',num2str(GenData_new_disp_partially.idGen(idx_end)),', ','Pmax = ',num2str(Pmax_cd_disp(d1)) ,', ','Pmin = ',num2str(Pmin_cd_disp2(d1)) ,', ','Emax = ',num2str(Emax_cd_disp2(d1)) ,', ','Emin = ',num2str(Emin_cd_disp2(d1)) ,', ',GenData_new_disp_partially.GenName{idx_end}])
            end
            %--------------------------------------------------------------
            
            
            % Step C: add the new gens (Invested) to each genconfiguration
            %--------------------------------------------------------------
            % copy candidate and modify idGen,GenName,CandidateUnit,Pmax
            % (maybe modify idProfile if profile is in MWh)
            
            % set idGen numbers for the newly built Inv portion
            idGen_new_disp_partially_inv = GenData_new_disp_partially.idGen(idx_disp_partially_inv);
            
            % get the column names of the GenConfiguration table, needed to
            % use datainsert to put new data back into MySQL
            colNames2 = GenConfigurationData.Properties.VariableNames;
            
            % get the genconfiguration data for the original candidate gens
            % (note this gets these gen's data ALL genconfigurations)
            GenConfigurationData_toupdate = GenConfigurationData( find(cell2mat(cellfun(@(x) ismember(x, genNames_disp_partially), GenConfigurationData.GenName, 'UniformOutput', 0))),: );
            % remove any genconfigs I shouldn't be updating
            GenConfigurationData_toupdate(~ismember(GenConfigurationData_toupdate.idGenConfig,ToUpdate_GenConfigs),:)=[];

            % Step C1: edit main data for Inv portion
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            % modify the idGen, GenName, CandidateUnit, Pmax, and Pmin for 
            % all the genconfigs I need to update
            % Note: I do need to edit Pmin, Emax, and Emin because BESS as
            % candidate units
            GenConfigurationData_new_disp_partially = GenConfigurationData_toupdate;
            GenConfigurationData_new_disp_partially.idGen = repmat( GenData_new_disp_partially.idGen(idx_disp_partially_inv),length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_disp_partially.GenName = repmat( GenData_new_disp_partially.GenName(idx_disp_partially_inv),length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_disp_partially.CandidateUnit = repmat( zeros(length(genNames_disp_partially),1),length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_disp_partially.Pmax = repmat( Pmax_inv_disp,length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_disp_partially.Pmin = repmat( Pmin_inv_disp2,length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_disp_partially.Emax = repmat( Emax_inv_disp2,length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_disp_partially.Emin = repmat( Emin_inv_disp2,length(ToUpdate_GenConfigs),1 );
            
            % get the correct InvestCost to set for these built gens based
            % on the year it was built (get InvCost for current
            % idGenConfig, and repeat for all GenConfigs that need to be 
            % updated)
            GenConfigurationData_new_disp_partially.InvCost = repmat( GenConfigurationData_toupdate.InvCost(find(GenConfigurationData_toupdate.idGenConfig == idGenConfig)) ,length(ToUpdate_GenConfigs),1 );
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            % Step C2: calculate and create a new profile for the Inv and
            % Cd portion gens (only needed if profiles are in MWh)
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            % get the original profiles for these gens
            ProfileData = fetch(conn,['SELECT * FROM profiledata']);
            
            % get the largest ProfileID so I can define a new ProfileID
            idProfile_last = max(ProfileData.idProfile);
            
            % get the idProfile of all disp_partially built gens
            for g1=1:length(genNames_disp_partially)
                disp_partially_idProfile(g1,1) = GenConfigurationData_toupdate.idProfile( find(strcmp(GenConfigurationData_toupdate.GenName,genNames_disp_partially{g1}),1) );
            end
            
            % create a vector for tracking these profile IDs if they change
            % or not
            disp_partially_idProfile_new_inv = disp_partially_idProfile;
            disp_partially_idProfile_new_cand = disp_partially_idProfile;
            
            % only proceed if one or more of these partially built disp
            % gens has a profile
            if any(~isnan(disp_partially_idProfile))
                
                % create an index for only those partially built disp gens
                % with a profile
                idx_disp_partially_profile = false(length(genNames_disp_partially),1);
                idx_disp_partially_profile(~isnan(disp_partially_idProfile)) = true;
                
                % test if any of these profiles are defined in MWh
                idx_disp_partially_editprofiles = strcmp( ProfileData.unit(disp_partially_idProfile(idx_disp_partially_profile) ), 'MWh');
                
                % only complete if a partially built disp uses MWh profile
                if any(idx_disp_partially_editprofiles)
                    
                    % create list of idProfiles that must be updated
                    disp_partially_editprofiles_idProfile = disp_partially_idProfile(idx_disp_partially_editprofiles);
                    
                    genNames_disp_partially_editprofiles_inv  = Str_name_Inv(idx_disp_partially_editprofiles);
                    genNames_disp_partially_editprofiles_cand = Str_name_Cd(idx_disp_partially_editprofiles);
                    
                    % for these that use MWh get the original profiles and 
                    % calculate the new profiles for both remaining 
                    % candidate and the invested gen
                    for g2=1:length(disp_partially_editprofiles_idProfile)
                        
                        % get the row index for these
                        disp_partially_editprofiles_idx(g2,1) = find(ProfileData.idProfile==disp_partially_editprofiles_idProfile(g2));
                        
                        % pull the profile data as a single row numeric vector
                        profileData_vals = getTimeSeriesFromDatabase(conn,disp_partially_editprofiles_idProfile(g2))';
                        
                        % now create two new profiles for this gen's remaining
                        % candidate and invested portions
                        profileData_vals_cand = profileData_vals * (Pmax_cd_disp(g2)/Pmax_orig_disp(g2));
                        profileData_vals_inv = profileData_vals * (Pmax_inv_disp(g2)/Pmax_orig_disp(g2));
                        
                        % now convert these new profiles into the string format for
                        % the database; new one keeps up to 15 digits after decimal
                        profileData_text_cand = strjoin(arrayfun(@(x) num2str(x,'%10.15f'),profileData_vals_cand,'UniformOutput',false),',');
                        profileData_text_inv = strjoin(arrayfun(@(x) num2str(x,'%10.15f'),profileData_vals_inv,'UniformOutput',false),',');
                        % add brackets to complete the JSON text for the profile
                        profileData_text_cand = {strcat('[',profileData_text_cand,']')};
                        profileData_text_inv = {strcat('[',profileData_text_inv,']')};
                        
                        % store the partially built 'inv' gen profiles for
                        % later
                        profileData_text_inv_all(g2,1) = profileData_text_inv;
                        profileData_text_cand_all(g2,1) = profileData_text_cand;
                        
                        % OLD code no longer needed
                        %{
                        % update the candidate profile (ONLY need to reset the
                        % profile itself)
                        tablename3 = 'profiledata';
                        colnames3 = {'timeSeries'};
                        newdata3 = profileData_text_cand;
                        % define WHERE clause for this update
                        whereclause3 = {strcat('WHERE (idProfile = "',num2str(disp_partially_editprofiles_idProfile(g2)),'")')};
                        % send this update to the database
                        update(conn,tablename3,colnames3,newdata3,whereclause3)
                        
                        % display update to candidate profiles
                        disp(['    Edit Remaining Candidate in profiledata: Pcnd/Pmax ratio = ',num2str(Pmax_cd_disp(g2)/Pmax_orig_disp(g2)),', Pbuilt = ',num2str(Pmax_cd_disp(g2)),' MW out of Pmax = ',num2str(Pmax_orig_disp(g2)),' MW, ',Str_name_Cd{g2}])
                        
                        
                        
                        % save these profile id's for later insert into
                        % genconfiguration data
                        idProfiles_disp_partially_new_inv(g2,1) = idProfile_last + g2;
                        
                        % modify the idProfile associated with this gen for all
                        % the genconfigs I need to update
                        GenConfigurationData_new.idProfile( find(strcmp(GenConfigurationData_new.GenName,Str_name_Inv{g2,1})) ) = idProfiles_disp_partially_new(g2,1);
                        %}
                        
                    end
                    
                    % create the new profile for the partially Inv/Cd gens
                    % get the profiledata for these profiles
                    ProfileData_new_inv = ProfileData(disp_partially_editprofiles_idx,:);
                    ProfileData_new_cand = ProfileData(disp_partially_editprofiles_idx,:);
                    % overwrite the idProfile with the new ones
                    ProfileData_new_inv.idProfile = idProfile_last + [1:length(disp_partially_editprofiles_idProfile)]';
                    ProfileData_new_cand.idProfile = idProfile_last + length(disp_partially_editprofiles_idProfile) + [1:length(disp_partially_editprofiles_idProfile)]';
                    disp_partially_idProfile_new_inv = ProfileData_new_inv.idProfile;
                    disp_partially_idProfile_new_cand = ProfileData_new_cand.idProfile;
                    % overwrite the timeSeries with the new ones
                    ProfileData_new_inv.timeSeries = profileData_text_inv_all;
                    ProfileData_new_cand.timeSeries = profileData_text_cand_all;
                    % get the column names of the GenData table, needed to use
                    % datainsert to put new data back into MySQL
                    colNames = ProfileData_new_inv.Properties.VariableNames;
                    
                    % push new profile data to add to the gendata MySQl table
                    datainsert(conn,'profiledata',colNames,ProfileData_new_inv);
                    datainsert(conn,'profiledata',colNames,ProfileData_new_cand);
                    
                    % display new profiles for Invested
                    for d1=1:length(disp_partially_editprofiles_idProfile)
                        disp(['    Added Partial Newly Built to profiledata: idProfile = ',num2str(ProfileData_new_inv.idProfile(d1)), ', Pblt/Pmax ratio = ',num2str(Pmax_inv_disp(d1)/Pmax_orig_disp(d1)),', Pbuilt = ',num2str(Pmax_inv_disp(d1)),' MW out of Pmax = ',num2str(Pmax_orig_disp(d1)),' MW, ',genNames_disp_partially_editprofiles_inv{d1}])
                        disp(['    Added Partial Candidate   to profiledata: idProfile = ',num2str(ProfileData_new_cand.idProfile(d1)),', Pblt/Pmax ratio = ',num2str(Pmax_cd_disp(d1)/Pmax_orig_disp(d1)), ', Pbuilt = ',num2str(Pmax_cd_disp(d1)), ' MW out of Pmax = ',num2str(Pmax_orig_disp(d1)),' MW, ',genNames_disp_partially_editprofiles_cand{d1}])
                    end
                    
                    % modify the idProfile associated with only the Inv
                    % portion gen for all the genconfigs I need to update
                    for d1=1:length(disp_partially_editprofiles_idProfile)
                        GenConfigurationData_new_disp_partially.idProfile( find(strcmp(GenConfigurationData_new_disp_partially.GenName,genNames_disp_partially_editprofiles_inv{d1,1})) ) = ProfileData_new_inv.idProfile(d1);
                    end
                    
                else
                    % no edits to profiles needed, none of the profiles is in
                    % MWh
                end
                
            else
                % no edits to profiles needed, no partially built disp gens
                % use a profile
            end
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            % Step C3: remove NaNs and insert new entries for Inv portion 
            % to genconfigurations
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            % replace NaN in Emin,Emax,E_ini,HedgeRatio with BLANKS
            vars = {'idProfile','Emin','Emax','E_ini','HedgeRatio'};
            GenConfigurationData_new_disp_partially.idProfile = num2cell(GenConfigurationData_new_disp_partially.idProfile);  	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_disp_partially.Emin = num2cell(GenConfigurationData_new_disp_partially.Emin);            	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_disp_partially.Emax = num2cell(GenConfigurationData_new_disp_partially.Emax);            	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_disp_partially.E_ini = num2cell(GenConfigurationData_new_disp_partially.E_ini);          	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_disp_partially.HedgeRatio = num2cell(GenConfigurationData_new_disp_partially.HedgeRatio);	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            ztemp = GenConfigurationData_new_disp_partially{:,vars};
            ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
            GenConfigurationData_new_disp_partially{:,vars} = ztemp; clear ztemp;
            
            % push new generator (invested) data to add to the 
            % genconfiguration MySQl table (push all configurations at
            % once)
            datainsert(conn,'genconfiguration',colNames2,GenConfigurationData_new_disp_partially);
            
            % display info genconfig update for newly created gens
            for d1=1:sum(idx_disp_partially_inv) 
                disp(['    Added Partial Newly Built to genconfigs: idGen = ',num2str(idGen_new_disp_partially_inv(d1)),', ','Pmax = ',num2str(Pmax_inv_disp(d1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(d1)),'), ','Pmin = ',num2str(Pmin_inv_disp2(d1)),', ','Emax = ',num2str(Emax_inv_disp2(d1)) ,', ','Emin = ',num2str(Emin_inv_disp2(d1)) ,', ','with idProfile = ',num2str(disp_partially_idProfile_new_inv(d1)),', ',Str_name_Inv{d1}])
            end
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            %-------------------------------------------------------------- 
            
            
            % Step D: edit remaining (Candidate) in each genconfiguration
            %--------------------------------------------------------------
            % modify the idGen, GenName, Pmax, Pmin, Emax, Emin 
            % (if these have a profile in MWh then edit the idProfile based  
            % on new profiles created above)
            
            % define info for MySQL table to be updated
            tablename2 = 'genconfiguration';
            %colnames2 = {'idGen','GenName','Pmax'};
            %newdata2 = {Str_name_Cd,Pmax_cd};
            
            % set idGen numbers for the remaining Cd portion
            idGen_new_disp_partially_cand = GenData_new_disp_partially.idGen(idx_disp_partially_cd);
            
            % loop over all GenConfigs that need to be updated
            for c1=1:length(ToUpdate_GenConfigs)
                
                % loop over all dispatchable partially built gens to update
                for g1=1:sum(idx_disp_partially_cd)
                    
                    % test if this partially built gen had a profile
                    disp_partially_idProfile_cd = GenConfigurationData_toupdate.idProfile( find(strcmp(GenConfigurationData_toupdate.GenName,genNames_disp_partially{g1}),1) );
                    
                    % only proceed if this partially built disp gens has a 
                    % profile
                    if ~isnan(disp_partially_idProfile_cd)
                        
                        % test if any of these profiles are defined in MWh
                        idx_disp_partially_editprofiles_cd = strcmp( ProfileData.unit(disp_partially_idProfile_cd), 'MWh');
                        
                        % only complete if a partially built disp uses MWh profile
                        if idx_disp_partially_editprofiles_cd
                            % this partially built Cd portion has a profile
                            % in MWh
                            
                            % define info for MySQL table to be updated
                            colnames2 = {'idGen','GenName','idProfile','Pmax','Pmin','Emax','Emin'};
                            
                            % setup the changes to this candidate's GenName and
                            % Pmax
                            newdata2 = {idGen_new_disp_partially_cand(g1),Str_name_Cd{g1},disp_partially_idProfile_new_cand(g1),Pmax_cd_disp(g1),Pmin_cd_disp2(g1),Emax_cd_disp2(g1),Emin_cd_disp2(g1)};
                            % check for any NaN and replace with blank
                            newdata2(cellfun(@(x) any(isnan(x)),newdata2)) = {''};
                            
                            
                            % define WHERE clause for this update
                            whereclause2 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_disp_partially{g1},'")')};
                            
                            % send this update to the database
                            update(conn,tablename2,colnames2,newdata2,whereclause2)
                            
                            % don't repeat display
                            if c1==1
                                %disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_disp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_disp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(g1)),'), with idProfile = ',num2str(disp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                                disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_disp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_disp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(g1)),'), ','Pmin = ',num2str(Pmin_cd_disp2(g1)),', ','Emax = ',num2str(Emax_cd_disp2(g1)) ,', ','Emin = ',num2str(Emin_cd_disp2(g1)) ,', ','with idProfile = ',num2str(disp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                            end
                            
                        else
                            % this partially built Cd portion has a
                            % profile, but not in MWh, so leave idProfile
                            % alone
                            
                            % define info for MySQL table to be updated
                            colnames2 = {'idGen','GenName','Pmax','Pmin','Emax','Emin'};
                            
                            % setup the changes to this candidate's GenName and
                            % Pmax
                            newdata2 = {idGen_new_disp_partially_cand(g1),Str_name_Cd{g1},Pmax_cd_disp(g1),Pmin_cd_disp2(g1),Emax_cd_disp2(g1),Emin_cd_disp2(g1)};
                            % check for any NaN and replace with blank
                            newdata2(cellfun(@(x) any(isnan(x)),newdata2)) = {''};
                            
                            % define WHERE clause for this update
                            whereclause2 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_disp_partially{g1},'")')};
                            
                            % send this update to the database
                            update(conn,tablename2,colnames2,newdata2,whereclause2)
                            
                            % don't repeat display
                            if c1==1
                                %disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_disp_partially_cand(g1)),', Pmax = ',num2str(Pmax_cd_disp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(g1)),'), with idProfile = ',num2str(disp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                                disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_disp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_disp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(g1)),'), ','Pmin = ',num2str(Pmin_cd_disp2(g1)),', ','Emax = ',num2str(Emax_cd_disp2(g1)) ,', ','Emin = ',num2str(Emin_cd_disp2(g1)) ,', ','with idProfile = ',num2str(disp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                            end
                            
                        end
                        
                    else
                        % this partially built Cd portion does NOT have a
                        % profile
                        
                        % define info for MySQL table to be updated
                        colnames2 = {'idGen','GenName','Pmax','Pmin','Emax','Emin'};
                        
                        % setup the changes to this candidate's GenName and
                        % Pmax
                        newdata2 = {idGen_new_disp_partially_cand(g1),Str_name_Cd{g1},Pmax_cd_disp(g1),Pmin_cd_disp2(g1),Emax_cd_disp2(g1),Emin_cd_disp2(g1)};
                        % check for any NaN and replace with blank
                        newdata2(cellfun(@(x) any(isnan(x)),newdata2)) = {''};
                        
                        % define WHERE clause for this update
                        whereclause2 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_disp_partially{g1},'")')};
                        
                        % send this update to the database
                        update(conn,tablename2,colnames2,newdata2,whereclause2)
                        
                        % don't repeat display
                        if c1==1
                            %disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_disp_partially_cand(g1)),', Pmax = ',num2str(Pmax_cd_disp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(g1)),'), with idProfile = ',num2str(disp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                            disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_disp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_disp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_disp(g1)),'), ','Pmin = ',num2str(Pmin_cd_disp2(g1)),', ','Emax = ',num2str(Emax_cd_disp2(g1)) ,', ','Emin = ',num2str(Emin_cd_disp2(g1)) ,', ','with idProfile = ',num2str(disp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                        end
                            
                    end
                    
                end
                
            end
            %--------------------------------------------------------------
            
            
        else
            % no updates needed to Dispatchable PARTIALLY built generators
            disp(['    none'])
        end
        %------------------------------------------------------------------
        
    else
        % no updates needed to ANY Dispatchable generators
        disp(['    none'])
    end
    
    
    %------------------------------------------------------------<<<<<<<<<<
    
    
    %% Update Generators - nondispatchable
    %>>>>>>>>>>------------------------------------------------------------
    % Update all Non-Dispatchable Gens - discreet and continuous version
    %>>>>>>>>>>------------------------------------------------------------
    
    disp([' ->Invested NonDispatchable Gens:'])
    
    % test if any non-dispatchable gens need to be updated
    if sum(idx_nondisp) > 0
        
        % get gen names of these to update
        genNames_nondisp = CentIvToMySQL.GenName(idx_nondisp);
        
        % get full Pmax of these invested nondisp gens
        GensInv_nondisp_GenName = genTable.GenName( find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_nondisp_Pmax    = genTable.Pmax(    find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_nondisp_Pmin    = genTable.Pmin(    find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_nondisp_Emax    = genTable.Emax(    find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp), genTable.GenName, 'UniformOutput', 0))) );
        GensInv_nondisp_Emin    = genTable.Emin(    find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp), genTable.GenName, 'UniformOutput', 0))) );
        
        % identify all nondisp that are NOT fully built
        idx_nondisp_partially = true(sum(idx_nondisp),1);     % initialize all as partially built
        idx_nondisp_partially(GensInv_nondisp_Pmax==CentIvToMySQL.Pmax(idx_nondisp)) = false;   % change for any that are fully built
        % identify all nondisp that ARE fully built
        idx_nondisp_fully = ~idx_nondisp_partially;
        
        % FIRST: update for any FULLY built NON-DISP gens
        %   A) must add new generator to gendata with new idGen, GenName,
        %      StartYr, EndYr
        %   B) must edit gens in genconfiguration for all appropriate
        %      configs; will edit idGen, GenName, CandidateUnit=0;
        %      Note, no need to modify idProfile b/c full capacity is built
        %------------------------------------------------------------------
        
        disp([' ->Invested NonDispatchable Gens (fully built):'])
        
        % test if any nondispatchable gens are FULLY built
        if sum(idx_nondisp_fully) > 0
            
            % get gen names of these to update
            genNames_nondisp_fully = genNames_nondisp(idx_nondisp_fully);
            
            % get the Pmax of these to update
            nondisp_fully_Pmax = GensInv_nondisp_Pmax(idx_nondisp_fully);
            
            % also adjust the name of the built generator (change from Cd
            % to Inv)
            Str_beg = extractBefore(genNames_nondisp_fully,"Cd"); 	% get the gen name string before the 'Cd' (will use this exactly in the new name for the invested)
            Str_end = extractAfter(genNames_nondisp_fully,"Cd"); 	% get the gen name string after the 'Cd' (will use this exactly in the new name for the invested)
            Str_name = strcat(Str_beg,'Inv',Str_end);              	% create new name for the new Invested generator (replace 'Cd' with 'Inv')
            
            % Step A: add new gens (Invested) to gendata table
            %--------------------------------------------------------------
            % copy from candidate and only need to change idGen number, 
            % GenName, StYr, EndYr,
            
            % reset the original gendata table data
            GenData = fetch(conn,['SELECT * FROM gendata']);
            
            % reset the largest idGen (need to define a new generator and 
            % must not use an existing idGen even if not in this genconfig)
            idGen_last = max(GenData.idGen);
            
            % define the idGen for all new generators that need to be created
            idGen_new = (idGen_last+1:idGen_last+sum(idx_nondisp_fully))';
            
            % get the gendata for all these gens from the Cd's in gendata,
            % no need to resort because it gets data one generator at a
            % time
            for ig = 1:length(genNames_nondisp_fully)
                GenData_new_nondisp_fully(ig,:) = GenData( find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp_fully(ig)), GenData.GenName, 'UniformOutput', 0)),1,'first'),: );
            end
            % overwrite the idGen with the new ones
            GenData_new_nondisp_fully.idGen = idGen_new;
            % overwrite the StartYr to be the year it's built
            GenData_new_nondisp_fully.StartYr = repmat(Year,[length(idGen_new),1]);
            % overwrite EndYr to be 2100
            GenData_new_nondisp_fully.EndYr = repmat(2100,[length(idGen_new),1]);
            % overwrite the GenNames with the new Invested ones
            GenData_new_nondisp_fully.GenName = Str_name;
            % replace NaN in eta_dis,eta_ch with BLANKS
            vars = {'eta_dis','eta_ch'};
            GenData_new_nondisp_fully.eta_dis = num2cell(GenData_new_nondisp_fully.eta_dis);    % for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenData_new_nondisp_fully.eta_ch = num2cell(GenData_new_nondisp_fully.eta_ch);      % for some reason I needed to convert these columns to cell arrays of double instead of just double
            ztemp = GenData_new_nondisp_fully{:,vars};
            ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
            GenData_new_nondisp_fully{:,vars} = ztemp; clear ztemp;
            
            % get the column names of the GenData table, needed to use
            % datainsert to put new data back into MySQL
            colNames = GenData_new_nondisp_fully.Properties.VariableNames;
            
            % push new generator data to add to the gendata MySQl table
            datainsert(conn,'gendata',colNames,GenData_new_nondisp_fully);
            
            % display info about newly created gens
            for d1=1:length(genNames_nondisp_fully)
                disp(['    Added Fully Built to gendata: idGen = ',num2str(GenData_new_nondisp_fully.idGen(d1)),', ','Pmax = ',num2str(nondisp_fully_Pmax(d1)),', ',GenData_new_nondisp_fully.GenName{d1}])
            end
            
            
            % Step B: add the new gens (Invested) to each genconfiguration
            %--------------------------------------------------------------
            % modify idGen,GenName,CandidateUnit
            % (no need to modify idProfile b/c full capacity is built)
            
            % define info for MySQL table to be updated
            tablename3 = 'genconfiguration';
            colnames3 = {'idGen','GenName','CandidateUnit'};
            
            % loop over all GenConfigs that need to be updated
            for c1=1:length(ToUpdate_GenConfigs)
                
                % loop over all nondispatchable fully built gens to update
                for g1=1:sum(idx_nondisp_fully)
                    
                    % define new data to insert for this gen
                    % (idGen & GenName & CandidateStatus=0)
                    newdata3 = {idGen_new(g1),Str_name{g1},0};
                    
                    % define WHERE clause for this update
                    whereclause3 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_nondisp_fully{g1},'")')};
                    
                    % send this update to the database
                    update(conn,tablename3,colnames3,newdata3,whereclause3)
                    
                    % don't repeat display
                    if c1==1
                        disp(['    Edit  Fully Built in genconfigs: idGen = ',num2str(idGen_new(g1)),', Pmax = ',num2str(nondisp_fully_Pmax(g1)),' MW, ',Str_name{g1}])
                    end

                end
                
            end
            
        else
            % no updates needed to NonDispatchable FULLY built generators
            disp(['    none'])
        end
        %------------------------------------------------------------------
        %}
        
        % SECOND: update for any PARTIALLY built NON-DISP gens 
        % (need to copy gendata info and update for both Inv and Cd)
        %   A) must add new generators to gendata with new idGen, GenName,
        %      StartYr, EndYr (both new partial candidate and new partial
        %      invested)
        %   B) Calc the Inv and Cd Pmax portions
        %   C) For Inv portion: must add gens in genconfiguration for all 
        %      appropriate configs
        %      1) will edit idGen, GenName, CandidateUnit=0, Pmax, InvCost; 
        %      2) must check and create new profiles (if profile is in MWh)
        %         for any unit with a profile, push new profiles for both
        %         Inv and Cd portion to profiledata table, update idProfile 
        %         in GenConfig for Inv portion
        %      3) insert new entries for this Inv gen's info to all
        %         genconfigurations
        %   D) For Cd portion: must update gens in genconfiguration for all
        %      appropriate configs; modify the idGen, GenName and Pmax; if
        %      any gen has a profile in MWh then edit the idProfile based
        %      on the new profiles created in step C2
        %------------------------------------------------------------------
        
        disp([' ->Invested NonDispatchable Gens (partially built):'])
        
        % test if any nondispatchable gens are PARTIALLY built
        if sum(idx_nondisp_partially) > 0
            
            % get gen names of these to update
            genNames_nondisp_partially = genNames_nondisp(idx_nondisp_partially);
            
            % Step A: add new gens (Invested & remaining Candidate) to 
            % gendata table
            %--------------------------------------------------------------
            % copy from candidate and only need to change idGen number, 
            % GenName, StYr, EndYr;
            % for each partially invested unit I must add TWO new gen to
            % the gendata table 
            
            % reset the original gendata table data
            GenData = fetch(conn,['SELECT * FROM gendata']);
            
            % reset the largest idGen (need to define two new generators 
            % for each partially built unit and must not use an existing 
            % idGen even if not in this genconfig)
            idGen_last = max(GenData.idGen);
            
            % define the idGen for all new generators that need to be created
            idGen_new = (idGen_last+1:idGen_last+2*sum(idx_nondisp_partially))';
            
            % initialize index for the Inv and Cd portion
            idx_nondisp_partially_inv = false(2*length(genNames_nondisp_partially),1);
            idx_nondisp_partially_cd  = false(2*length(genNames_nondisp_partially),1);
            
            % get the gendata for all these gens from the Cd's in gendata
            % no need to resort because it gets data one generator at a
            % time
            % must create two entries since I will define a new unit in
            % GenData for both the Inv and Cd portion
            for ig = 1:length(genNames_nondisp_partially)
                idx_sta = 2*ig - 1;
                idx_end = idx_sta + 1;
                GenData_new_nondisp_partially(idx_sta,:) = GenData( find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp_partially(ig)), GenData.GenName, 'UniformOutput', 0)),1,'first'),: );
                GenData_new_nondisp_partially(idx_end,:) = GenData( find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp_partially(ig)), GenData.GenName, 'UniformOutput', 0)),1,'first'),: );
                % also need an indicator for which of these newly defined
                % gens in GenData are the Inv or Cd portion
                idx_nondisp_partially_inv(idx_sta) = true;
                idx_nondisp_partially_cd(idx_end) = true;
            end
            % overwrite the idGen with the new ones
            GenData_new_nondisp_partially.idGen = idGen_new;
            % overwrite the StartYr to be the year it's built
            GenData_new_nondisp_partially.StartYr = repmat(Year,[length(idGen_new),1]);
            % overwrite EndYr to be 2100
            GenData_new_nondisp_partially.EndYr = repmat(2100,[length(idGen_new),1]);
            % also adjust the name of the built generator (change from Cd
            % to Inv)
            Str_beg_Inv = extractBefore(genNames_nondisp_partially,"Cd");       % get the gen name string before the 'Cd' (will use this exactly in the new name for both invested and candidate)
            Str_end_Inv = extractAfter(genNames_nondisp_partially,"Cd");        % get the gen name string after the 'Cd' (will use this exactly in the new name for the invested)
            Str_name_Inv = strcat(Str_beg_Inv,'Inv',Str_end_Inv,"_PtInv");      % create new name for the new Invested generator
            Str_beg_Cd = genNames_nondisp_partially;                            % use the full original name  
            Str_name_Cd = strcat(Str_beg_Cd,'_PtCd');                           % add a '_PtCd' for each time a portion remains as a candidate
            % need to get names in correct order
            for ig =  1:length(genNames_nondisp_partially)
                idx_sta = 2*ig - 1;
                idx_end = idx_sta + 1;
                Str_name_InvCd2(idx_sta,1) = Str_name_Inv(ig);
                Str_name_InvCd2(idx_end,1) = Str_name_Cd(ig);
            end
            
            % overwrite the GenNames with the new Invested ones
            GenData_new_nondisp_partially.GenName = Str_name_InvCd2;
            % replace NaN in eta_dis,eta_ch with BLANKS
            vars = {'eta_dis','eta_ch'};
            GenData_new_nondisp_partially.eta_dis = num2cell(GenData_new_nondisp_partially.eta_dis);    % for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenData_new_nondisp_partially.eta_ch = num2cell(GenData_new_nondisp_partially.eta_ch);      % for some reason I needed to convert these columns to cell arrays of double instead of just double
            ztemp = GenData_new_nondisp_partially{:,vars};
            ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
            GenData_new_nondisp_partially{:,vars} = ztemp; clear ztemp;
            
            % get the column names of the GenData table, needed to use
            % datainsert to put new data back into MySQL
            colNames = GenData_new_nondisp_partially.Properties.VariableNames;
            
            % push new generator data to add to the gendata MySQl table
            datainsert(conn,'gendata',colNames,GenData_new_nondisp_partially);
            %--------------------------------------------------------------
            
            
            % Step B: Calc the Inv and Cd portion power capacities
            %--------------------------------------------------------------
            
            % calculate the Pmax_inv and Pmax_cd
            Pinv_nondisp = CentIvToMySQL.Pmax(idx_nondisp);
            Pmax_inv_nondisp = Pinv_nondisp(idx_nondisp_partially);
            Pmax_cd_nondisp = GensInv_nondisp_Pmax(idx_nondisp_partially) - Pmax_inv_nondisp;
            Pmax_orig_nondisp = GensInv_nondisp_Pmax(idx_nondisp_partially);
            % FOR other parameters: Pmin, Emax, Emin...
            % OPTION 1: calculate the Pmin_inv and Pmin_cd (for any newly built
            % storages)
            % THIS DOESN'T WORK NOW because CentIv only sends 0's for Pmin
            % of partially built conventionals (GasCC) and DB has nonZero
            % as original Pmin for these candidates. Need to check if these
            % gas units are supposed to be continuous investment decisions
            % and if so set Pmin=0 in database
            %Pinv_nondisp_min = CentIvToMySQL.Pmin(idx_nondisp);
            %Pmin_orig_nondisp = GensInv_nondisp_Pmin(idx_nondisp_partially);
            %Emax_orig_nondisp = GensInv_nondisp_Emax(idx_nondisp_partially);
            %Emin_orig_nondisp = GensInv_nondisp_Emin(idx_nondisp_partially);
            %Pmin_inv_nondisp1 = Pinv_nondisp_min(idx_nondisp_partially);
            %Pmin_cd_nondisp1 = GensInv_nondisp_Pmin(idx_nondisp_partially) - Pmin_inv_nondisp1;
            % OPTION 2: assume the Pmax ratio of invested/original is same
            % for Pmin, round to 1 decimal
            PminInvestmentForNonDispatchables = CentIvToMySQL.Pmin(idx_nondisp);
            Pmin_orig_nondisp = GensInv_nondisp_Pmin(idx_nondisp_partially);
            Pmin_inv_nondisp2 = PminInvestmentForNonDispatchables(idx_nondisp_partially);
            Pmin_cd_nondisp2 = Pmin_orig_nondisp - Pmin_inv_nondisp2;
            % also make same calculation for Emax_inv / Emax_cd
            Emax_orig_nondisp = GensInv_nondisp_Emax(idx_nondisp_partially);
            Emax_inv_nondisp2 = round(Emax_orig_nondisp .* (Pmax_inv_nondisp ./ Pmax_orig_nondisp),1);
            Emax_cd_nondisp2 = round(Emax_orig_nondisp .* (Pmax_cd_nondisp ./ Pmax_orig_nondisp),1);
            % also make same calculation for Emin_inv / Emin_cd
            Emin_orig_nondisp = GensInv_nondisp_Emin(idx_nondisp_partially);
            Emin_inv_nondisp2 = round(Emin_orig_nondisp .* (Pmax_inv_nondisp ./ Pmax_orig_nondisp),1);
            Emin_cd_nondisp2 = round(Emin_orig_nondisp .* (Pmax_cd_nondisp ./ Pmax_orig_nondisp),1);
            
            % display info about newly created gens
            for d1=1:length(genNames_nondisp_partially)
                idx_sta = 2*d1 - 1;
                idx_end = idx_sta + 1;
                disp(['    Added Partial Newly Built to gendata: idGen = ',num2str(GenData_new_nondisp_partially.idGen(idx_sta)),', ','Pmax = ',num2str(Pmax_inv_nondisp(d1)),', ','Pmin = ',num2str(Pmin_inv_nondisp2(d1)),', ','Emax = ',num2str(Emax_inv_nondisp2(d1)),', ','Emin = ',num2str(Emin_inv_nondisp2(d1)),', ',GenData_new_nondisp_partially.GenName{idx_sta}])
                disp(['    Added Partial Candidate   to gendata: idGen = ',num2str(GenData_new_nondisp_partially.idGen(idx_end)),', ','Pmax = ',num2str(Pmax_cd_nondisp(d1)) ,', ','Pmin = ',num2str(Pmin_cd_nondisp2(d1)) ,', ','Emax = ',num2str(Emax_cd_nondisp2(d1)) ,', ','Emin = ',num2str(Emin_cd_nondisp2(d1)) ,', ',GenData_new_nondisp_partially.GenName{idx_end}])
                %disp(['    Added Partial Newly Built to gendata: idGen = ',num2str(GenData_new_nondisp_partially.idGen(idx_sta)),', ','Pmax = ',num2str(Pmax_inv_nondisp(d1)),', ',GenData_new_nondisp_partially.GenName{idx_sta}])
                %disp(['    Added Partial Candidate   to gendata: idGen = ',num2str(GenData_new_nondisp_partially.idGen(idx_end)),', ','Pmax = ',num2str(Pmax_cd_nondisp(d1)),', ',GenData_new_nondisp_partially.GenName{idx_end}])
            end
            %--------------------------------------------------------------
            
            
            % Step C: add the new gens (Invested) to each genconfiguration
            %--------------------------------------------------------------
            % copy candidate and modify idGen,GenName,CandidateUnit,Pmax
            % (maybe modify idProfile if profile is in MWh)
            
            % set idGen numbers for the newly built Inv portion
            idGen_new_nondisp_partially_inv = GenData_new_nondisp_partially.idGen(idx_nondisp_partially_inv);
            
            % get the column names of the GenConfiguration table, needed to
            % use datainsert to put new data back into MySQL
            colNames2 = GenConfigurationData.Properties.VariableNames;
            
            % get the genconfiguration data for the original candidate gens
            % (note this gets these gen's data ALL genconfigurations)
            GenConfigurationData_toupdate = GenConfigurationData( find(cell2mat(cellfun(@(x) ismember(x, genNames_nondisp_partially), GenConfigurationData.GenName, 'UniformOutput', 0))),: );
            % remove any genconfigs I shouldn't be updating
            GenConfigurationData_toupdate(~ismember(GenConfigurationData_toupdate.idGenConfig,ToUpdate_GenConfigs),:)=[];
            
            % Step C1: edit main data for Inv portion
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            % modify the idGen, GenName, CandidateUnit, Pmax, and Pmin for 
            % all the genconfigs I need to update
            % Note: I edit Pmin just incase but should generally be =0
            % Note: I also edit Emax, Emin in case such units are
            % defined in the future
            GenConfigurationData_new_nondisp_partially = GenConfigurationData_toupdate;
            GenConfigurationData_new_nondisp_partially.idGen = repmat( GenData_new_nondisp_partially.idGen(idx_nondisp_partially_inv),length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_nondisp_partially.GenName = repmat( GenData_new_nondisp_partially.GenName(idx_nondisp_partially_inv),length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_nondisp_partially.CandidateUnit = repmat( zeros(length(genNames_nondisp_partially),1),length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_nondisp_partially.Pmax = repmat( Pmax_inv_nondisp,length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_nondisp_partially.Pmin = repmat( Pmin_inv_nondisp2,length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_nondisp_partially.Emax = repmat( Emax_inv_nondisp2,length(ToUpdate_GenConfigs),1 );
            GenConfigurationData_new_nondisp_partially.Emin = repmat( Emin_inv_nondisp2,length(ToUpdate_GenConfigs),1 );
            
            % get the correct InvestCost to set for these built gens based
            % on the year it was built (get InvCost for current
            % idGenConfig, and repeat for all GenConfigs that need to be 
            % updated)
            GenConfigurationData_new_nondisp_partially.InvCost = repmat( GenConfigurationData_toupdate.InvCost(find(GenConfigurationData_toupdate.idGenConfig == idGenConfig)) ,length(ToUpdate_GenConfigs),1 );
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            % Step C2: calculate and create a new profile for the Inv and
            % Cd portion gens (only needed if profiles are in MWh)
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            % get the original profiles for these gens
            ProfileData = fetch(conn,['SELECT * FROM profiledata']);
            
            % get the largest ProfileID so I can define a new ProfileID
            idProfile_last = max(ProfileData.idProfile);
            
            % get the idProfile of all nondisp_partially built gens
            for g1=1:length(genNames_nondisp_partially)
                nondisp_partially_idProfile(g1,1) = GenConfigurationData_toupdate.idProfile( find(strcmp(GenConfigurationData_toupdate.GenName,genNames_nondisp_partially{g1}),1) );
            end
            
            % create a vector for tracking these profile IDs if they change
            % or not
            nondisp_partially_idProfile_new_inv = nondisp_partially_idProfile;
            nondisp_partially_idProfile_new_cand = nondisp_partially_idProfile;
            
            % only proceed if one or more of these partially built nondisp
            % gens has a profile
            if any(~isnan(nondisp_partially_idProfile))
                
                % create an index for only those partially built nondisp 
                % gens with a profile
                idx_nondisp_partially_profile = false(length(genNames_nondisp_partially),1);
                idx_nondisp_partially_profile(~isnan(nondisp_partially_idProfile)) = true;
                
                % test if any of these profiles are defined in MWh
                idx_nondisp_partially_editprofiles = strcmp( ProfileData.unit(nondisp_partially_idProfile(idx_nondisp_partially_profile) ), 'MWh');
                
                % only complete if a partially built nondisp uses MWh profile
                if any(idx_nondisp_partially_editprofiles)
                    
                    % create list of idProfiles that must be updated
                    nondisp_partially_editprofiles_idProfile = nondisp_partially_idProfile(idx_nondisp_partially_editprofiles);
                    
                    genNames_nondisp_partially_editprofiles_inv  = Str_name_Inv(idx_nondisp_partially_editprofiles);
                    genNames_nondisp_partially_editprofiles_cand = Str_name_Cd(idx_nondisp_partially_editprofiles);
                    
                    % for these that use MWh get the original profiles and 
                    % calculate the new profiles for both remaining 
                    % candidate and the invested gen
                    for g2=1:length(nondisp_partially_editprofiles_idProfile)
                        
                        % get the row index for these
                        nondisp_partially_editprofiles_idx(g2,1) = find(ProfileData.idProfile==nondisp_partially_editprofiles_idProfile(g2));
                        
                        % pull the profile data as a single row numeric vector
                        profileData_vals = getTimeSeriesFromDatabase(conn,nondisp_partially_editprofiles_idProfile(g2))';
                        
                        % now create two new profiles for this gen's remaining
                        % candidate and invested portions
                        profileData_vals_cand = profileData_vals * (Pmax_cd_nondisp(g2)/Pmax_orig_nondisp(g2));
                        profileData_vals_inv = profileData_vals * (Pmax_inv_nondisp(g2)/Pmax_orig_nondisp(g2));
                        
                        % now convert these new profiles into the string format for
                        % the database; new one keeps up to 15 digits after decimal
                        profileData_text_cand = strjoin(arrayfun(@(x) num2str(x,'%10.15f'),profileData_vals_cand,'UniformOutput',false),',');
                        profileData_text_inv = strjoin(arrayfun(@(x) num2str(x,'%10.15f'),profileData_vals_inv,'UniformOutput',false),',');
                        % add brackets to complete the JSON text for the profile
                        profileData_text_cand = {strcat('[',profileData_text_cand,']')};
                        profileData_text_inv = {strcat('[',profileData_text_inv,']')};
                        
                        % store the partially built 'inv' gen profiles for
                        % later
                        profileData_text_inv_all(g2,1) = profileData_text_inv;
                        profileData_text_cand_all(g2,1) = profileData_text_cand;
                        
                    end
                    
                    % create the new profile for the partially Inv/Cd gens
                    % get the profiledata for these profiles
                    ProfileData_new_inv = ProfileData(nondisp_partially_editprofiles_idx,:);
                    ProfileData_new_cand = ProfileData(nondisp_partially_editprofiles_idx,:);
                    % overwrite the idProfile with the new ones
                    ProfileData_new_inv.idProfile = idProfile_last + [1:length(nondisp_partially_editprofiles_idProfile)]';
                    ProfileData_new_cand.idProfile = idProfile_last + length(nondisp_partially_editprofiles_idProfile) + [1:length(nondisp_partially_editprofiles_idProfile)]';
                    nondisp_partially_idProfile_new_inv = ProfileData_new_inv.idProfile;
                    nondisp_partially_idProfile_new_cand = ProfileData_new_cand.idProfile;
                    % overwrite the timeSeries with the new ones
                    ProfileData_new_inv.timeSeries = profileData_text_inv_all;
                    ProfileData_new_cand.timeSeries = profileData_text_cand_all;
                    % get the column names of the GenData table, needed to use
                    % datainsert to put new data back into MySQL
                    colNames = ProfileData_new_inv.Properties.VariableNames;
                    
                    % push new profile data to add to the gendata MySQl table
                    datainsert(conn,'profiledata',colNames,ProfileData_new_inv);
                    datainsert(conn,'profiledata',colNames,ProfileData_new_cand);
                    
                    % display new profiles for Invested
                    for d1=1:length(nondisp_partially_editprofiles_idProfile)
                        disp(['    Added Partial Newly Built to profiledata: idProfile = ',num2str(ProfileData_new_inv.idProfile(d1)), ', Pblt/Pmax ratio = ',num2str(Pmax_inv_nondisp(d1)/Pmax_orig_nondisp(d1)),', Pbuilt = ',num2str(Pmax_inv_nondisp(d1)),' MW out of Pmax = ',num2str(Pmax_orig_nondisp(d1)),' MW, ',genNames_nondisp_partially_editprofiles_inv{d1}])
                        disp(['    Added Partial Candidate   to profiledata: idProfile = ',num2str(ProfileData_new_cand.idProfile(d1)),', Pblt/Pmax ratio = ',num2str(Pmax_cd_nondisp(d1)/Pmax_orig_nondisp(d1)), ', Pbuilt = ',num2str(Pmax_cd_nondisp(d1)), ' MW out of Pmax = ',num2str(Pmax_orig_nondisp(d1)),' MW, ',genNames_nondisp_partially_editprofiles_cand{d1}])
                    end
                    
                    % modify the idProfile associated with only the Inv
                    % portion gen for all the genconfigs I need to update
                    for d1=1:length(nondisp_partially_editprofiles_idProfile)
                        GenConfigurationData_new_nondisp_partially.idProfile( find(strcmp(GenConfigurationData_new_nondisp_partially.GenName,genNames_nondisp_partially_editprofiles_inv{d1,1})) ) = ProfileData_new_inv.idProfile(d1);
                    end
                    
                else
                    % no edits to profiles needed, none of the profiles is 
                    % in MWh
                end
                
            else
                % no edits to profiles needed, no partially built nondisp 
                % gens use a profile
            end
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            % Step C3: remove NaNs and insert new entries for Inv portion 
            % to genconfigurations
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            % replace NaN in Emin,Emax,E_ini,HedgeRatio with BLANKS
            vars = {'idProfile','Emin','Emax','E_ini','HedgeRatio'};
            GenConfigurationData_new_nondisp_partially.idProfile = num2cell(GenConfigurationData_new_nondisp_partially.idProfile);  	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_nondisp_partially.Emin = num2cell(GenConfigurationData_new_nondisp_partially.Emin);            	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_nondisp_partially.Emax = num2cell(GenConfigurationData_new_nondisp_partially.Emax);            	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_nondisp_partially.E_ini = num2cell(GenConfigurationData_new_nondisp_partially.E_ini);          	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            GenConfigurationData_new_nondisp_partially.HedgeRatio = num2cell(GenConfigurationData_new_nondisp_partially.HedgeRatio);	% for some reason I needed to convert these columns to cell arrays of double instead of just double
            ztemp = GenConfigurationData_new_nondisp_partially{:,vars};
            ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
            GenConfigurationData_new_nondisp_partially{:,vars} = ztemp; clear ztemp;
            
            % push new generator (invested) data to add to the 
            % genconfiguration MySQl table (push all configurations at
            % once)
            datainsert(conn,'genconfiguration',colNames2,GenConfigurationData_new_nondisp_partially);
            
            % display info genconfig update for newly created gens
            for d1=1:sum(idx_nondisp_partially_inv) 
                disp(['    Added Partial Newly Built to genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_inv(d1)),', ','Pmax = ',num2str(Pmax_inv_nondisp(d1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(d1)),'), ','Pmin = ',num2str(Pmin_inv_nondisp2(d1)),', ','Emax = ',num2str(Emax_inv_nondisp2(d1)) ,', ','Emin = ',num2str(Emin_inv_nondisp2(d1)) ,', ','with idProfile = ',num2str(nondisp_partially_idProfile_new_inv(d1)),', ',Str_name_Inv{d1}])
                %disp(['    Added Partial Newly Built to genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_inv(d1)),', Pmax = ',num2str(Pmax_inv_nondisp(d1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(d1)),'), with idProfile = ',num2str(nondisp_partially_idProfile_new_inv(d1)),', ',Str_name_Inv{d1}])
            end
            %>>>>>>>>>>>><<<<<<<<<<<<<<
            
            %-------------------------------------------------------------- 
            

            % Step D: edit remaining (Candidate) in each genconfiguration
            %--------------------------------------------------------------
            % modify the idGen, GenName, Pmax, Pmin, Emax, Emin 
            % (if these have a profile in MWh then edit the idProfile based  
            % on new profiles created above)
            
            % define info for MySQL table to be updated
            tablename4 = 'genconfiguration';
            %colnames2 = {'idGen','GenName','Pmax'};
            %newdata2 = {Str_name_Cd,Pmax_cd};
            
            % set idGen numbers for the remaining Cd portion
            idGen_new_nondisp_partially_cand = GenData_new_nondisp_partially.idGen(idx_nondisp_partially_cd);
            
            % loop over all GenConfigs that need to be updated
            for c1=1:length(ToUpdate_GenConfigs)
                
                % loop over all nondispatchable fully built gens to update
                for g1=1:sum(idx_nondisp_partially_cd)
                    
                    % test if this partially built gen had a profile
                    nondisp_partially_idProfile_cd = GenConfigurationData_toupdate.idProfile( find(strcmp(GenConfigurationData_toupdate.GenName,genNames_nondisp_partially{g1}),1) );
                    
                    % only proceed if this partially built nondisp gens has
                    % a profile
                    if ~isnan(nondisp_partially_idProfile_cd)
                        
                        % test if any of these profiles are defined in MWh
                        idx_nondisp_partially_editprofiles_cd = strcmp( ProfileData.unit(nondisp_partially_idProfile_cd), 'MWh');
                        
                        % only complete if a partially built nondisp uses 
                        % MWh profile
                        if idx_nondisp_partially_editprofiles_cd
                            % this partially built Cd portion has a profile
                            % in MWh
                            
                            % define info for MySQL table to be updated
                            colnames4 = {'idGen','GenName','idProfile','Pmax','Pmin','Emax','Emin'};
                            
                            % setup the changes to this candidate's GenName
                            % and Pmax
                            newdata4 = {idGen_new_nondisp_partially_cand(g1),Str_name_Cd{g1},nondisp_partially_idProfile_new_cand(g1),Pmax_cd_nondisp(g1),Pmin_cd_nondisp2(g1),Emax_cd_nondisp2(g1),Emin_cd_nondisp2(g1)};
                            % check for any NaN and replace with blank
                            newdata4(cellfun(@(x) any(isnan(x)),newdata4)) = {''};
                            
                            % define WHERE clause for this update
                            whereclause4 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_nondisp_partially{g1},'")')};
                            
                            % send this update to the database
                            update(conn,tablename4,colnames4,newdata4,whereclause4)
                            
                            % don't repeat display
                            if c1==1
                                %disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_cand(g1)),', Pmax = ',num2str(Pmax_cd_nondisp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(g1)),'), with idProfile = ',num2str(nondisp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                                disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_nondisp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(g1)),'), ','Pmin = ',num2str(Pmin_cd_nondisp2(g1)),', ','Emax = ',num2str(Emax_cd_nondisp2(g1)) ,', ','Emin = ',num2str(Emin_cd_nondisp2(g1)) ,', ','with idProfile = ',num2str(nondisp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                            end
                            
                        else
                            % this partially built Cd portion has a
                            % profile, but not in MWh, so leave idProfile
                            % alone
                            
                            % define info for MySQL table to be updated
                            colnames4 = {'idGen','GenName','Pmax','Pmin','Emax','Emin'};
                            
                            % setup the changes to this candidate's GenName
                            % and Pmax
                            newdata4 = {idGen_new_nondisp_partially_cand(g1),Str_name_Cd{g1},Pmax_cd_nondisp(g1),Pmin_cd_nondisp2(g1),Emax_cd_nondisp2(g1),Emin_cd_nondisp2(g1)};
                            % check for any NaN and replace with blank
                            newdata4(cellfun(@(x) any(isnan(x)),newdata4)) = {''};
                            
                            % define WHERE clause for this update
                            whereclause4 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_nondisp_partially{g1},'")')};
                            
                            % send this update to the database
                            update(conn,tablename4,colnames4,newdata4,whereclause4)
                            
                            % don't repeat display
                            if c1==1
                                %disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_cand(g1)),', Pmax = ',num2str(Pmax_cd_nondisp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(g1)),'), with idProfile = ',num2str(nondisp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                                disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_nondisp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(g1)),'), ','Pmin = ',num2str(Pmin_cd_nondisp2(g1)),', ','Emax = ',num2str(Emax_cd_nondisp2(g1)) ,', ','Emin = ',num2str(Emin_cd_nondisp2(g1)) ,', ','with idProfile = ',num2str(nondisp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                            end
                            
                        end
                        
                    else
                        % this partially built Cd portion does NOT have a
                        % profile
                        
                        % define info for MySQL table to be updated
                        colnames4 = {'idGen','GenName','Pmax','Pmin','Emax','Emin'};
                        
                        % setup the changes to this candidate's GenName and
                        % Pmax
                        newdata4 = {idGen_new_nondisp_partially_cand(g1),Str_name_Cd{g1},Pmax_cd_nondisp(g1),Pmin_cd_nondisp2(g1),Emax_cd_nondisp2(g1),Emin_cd_nondisp2(g1)};
                        % check for any NaN and replace with blank
                        newdata4(cellfun(@(x) any(isnan(x)),newdata4)) = {''};
                        
                        % define WHERE clause for this update
                        whereclause4 = {strcat('WHERE (idGenConfig = "',num2str(ToUpdate_GenConfigs(c1)),'") and (GenName = "',genNames_nondisp_partially{g1},'")')};
                        
                        % send this update to the database
                        update(conn,tablename4,colnames4,newdata4,whereclause4)
                        
                        % don't repeat display
                        if c1==1
                            %disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_cand(g1)),', Pmax = ',num2str(Pmax_cd_nondisp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(g1)),'), with idProfile = ',num2str(nondisp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                            disp(['    Edit  Partial Candidate   in genconfigs: idGen = ',num2str(idGen_new_nondisp_partially_cand(g1)),', ','Pmax = ',num2str(Pmax_cd_nondisp(g1)),' MW (Orig Pmax = ',num2str(Pmax_orig_nondisp(g1)),'), ','Pmin = ',num2str(Pmin_cd_nondisp2(g1)),', ','Emax = ',num2str(Emax_cd_nondisp2(g1)) ,', ','Emin = ',num2str(Emin_cd_nondisp2(g1)) ,', ','with idProfile = ',num2str(nondisp_partially_idProfile_new_cand(g1)),', ',Str_name_Cd{g1}])
                        end
                            
                    end
                    
                end
                
            end
            %--------------------------------------------------------------
            
            
        else
            % no updates needed to Non-Dispatchable PARTIALLY built generators
            disp(['    none'])
        end
        %------------------------------------------------------------------
        %}
    else
        % no updates needed to ANY Non-Dispatchable generators
        disp(['    none'])
    end
    
    
    %------------------------------------------------------------<<<<<<<<<<
    
    %% Update Reserves
    % assumes the names & order of the reserves coming from CentIv 
    % assumes the names in the database
    
    disp([' ->Tertiary Reserves:'])
    
    % setup the names of the reserve profiles in ProfileData (order matches
    % what comes from CentIv)
    reserves_editprofiles_name = {'CH_Reserve_Tertiary_UP';'CH_Reserve_Tertiary_DN'};
    
    % get the original profiles for these gens
    ProfileData = fetch(conn,['SELECT * FROM profiledata']);
    
    % get the idProfile for the reserves that could change
    reserves_editprofiles_idProfile = ProfileData.idProfile( find(cell2mat(cellfun(@(x) ismember(x, reserves_editprofiles_name), ProfileData.name, 'UniformOutput', 0))),: );
    
    % loop over each of the reserve profiles to modify
    for r1=1:length(reserves_editprofiles_name)
        
        % pull the profile data as a single row numeric vector (only 1)
        profileData_reserves_vals = getTimeSeriesFromDatabase(conn,reserves_editprofiles_idProfile(r1))';
        
        % get the amount to add from CentIv
        if r1==1
            % is for Tertiary UP
            reserves_add = CentIvToMySQL.TCR_UP_add;
        else
            % is for Tertiary DN
            reserves_add = CentIvToMySQL.TCR_DOWN_add;
        end
        
        % round the added amount to the nearest MW
        reserves_add = round(reserves_add,0);
        
        % edit this profile by adding the amount from CentIv
        profileData_reserves_vals_new = profileData_reserves_vals + reserves_add;
        
        % now convert these new profiles into the string format for
        % the database; new one keeps up to 15 digits after decimal
        profileData_reserves_text_new = strjoin(arrayfun(@(x) num2str(x,'%10.15f'),profileData_reserves_vals_new,'UniformOutput',false),',');
        % add brackets to complete the JSON text for the profile
        profileData_reserves_text_new = {strcat('[',profileData_reserves_text_new,']')};
        
        % update the candidate profile (ONLY need to reset the
        % profile itself)
        tablename5 = 'profiledata';
        colnames5 = {'timeSeries'};
        newdata5 = profileData_reserves_text_new;
        % define WHERE clause for this update
        whereclause5 = {strcat('WHERE (idProfile = "',num2str(reserves_editprofiles_idProfile(r1)),'")')};
        % send this update to the database
        update(conn,tablename5,colnames5,newdata5,whereclause5)
        
        % display update to candidate profiles
        disp(['    Edit ',reserves_editprofiles_name{r1},' in profiledata: Added = ',num2str(reserves_add),' MW every hour'])
        
    end
    
    
    %% Update DistIv Profiles
    
    disp([' ->DistIv Profiles:'])
    
    % get the original profiles for these gens
    DistIvProfileData = fetch(conn,['SELECT * FROM distprofiles']);
    
    % Elena sends me cell arrays with each column is a node:
    %  1st row = subregion
    %  2nd row = node name
    %  3rd-8762nd row = hourly DistIV Gen for that node
    [nrows1,nnodes1]=size(CentIvToMySQL.Generation_DistIv);     % include only CH nodes
    % take Elena's data out of the table (listing of node names and
    % hourly loads for these nodes)
    NodalDistIvGen_nodenames = CentIvToMySQL.Generation_DistIv{2,:};       % cell array of strings, listing of the node names for all DistIv data sent (should be all CH nodes)
    NodalDistIvGen_vals = str2double(CentIvToMySQL.Generation_DistIv{3:nrows1,1:nnodes1}); % numeric matrix, each column is a node's data, CentIv will send the DistIv gen supplied for all CH nodes as a table (any days that are reduced are assumed identical to the simulated day prior)
    
    % works when CentIv already unpacks the load to all 8760 hrs
    NodalDistIvGen_vals_FullYr = NodalDistIvGen_vals;
    
    % need to round any very small numbers to zero
    NodalDistIvGen_vals_FullYr(NodalDistIvGen_vals_FullYr <= 0.00001) = 0;
    
    % get idDistProfile for each node (correct order)
    distiv_editprofiles_idProfile = DistIvProfileData.idDistProfile( find(cell2mat(cellfun(@(x) ismember(x, NodalDistIvGen_nodenames), DistIvProfileData.name, 'UniformOutput', 0))),:  );
    
    % loop over each profile to modify
    for d1=1:size(NodalDistIvGen_vals_FullYr,2)
        
        % now convert these new profiles into the string format for
        % the database; new one keeps up to 4 digits after decimal
        distprofileData_vals_text_new = strjoin(arrayfun(@(x) num2str(x,'%10.4f'),NodalDistIvGen_vals_FullYr(:,d1),'UniformOutput',false),',');
        % add brackets to complete the JSON text for the profile
        distprofileData_vals_text_new = {strcat('[',distprofileData_vals_text_new,']')};
        
        % update the candidate profile (ONLY need to reset the
        % profile itself)
        tablename6 = 'distprofiles';
        colnames6 = {'timeSeries'};
        newdata6 = distprofileData_vals_text_new;
        % define WHERE clause for this update
        whereclause6 = {strcat('WHERE (idDistProfile = "',num2str(distiv_editprofiles_idProfile(d1)),'")')};
        % send this update to the database
        update(conn,tablename6,colnames6,newdata6,whereclause6)
        
    end
    
    
    % display update to candidate profiles
    disp(['    Edit DistIv Gen Injection profiles in distprofiles: Annual DistIv Gen = ',num2str(sum(sum(NodalDistIvGen_vals_FullYr))/1000000),' TWh'])
    
    
    
    %% Update Transmission Lines - identify
    
    %>>>>>>>>>>------------------------------------------------------------
    % Identify which NetworkConfigs should be updated based on the simulation
    % year
    %>>>>>>>>>>------------------------------------------------------------
    
    % Network Config Info
    % get any existing data from networkConfigInfo
    NetworkConfigInfo_Data = select(conn,['SELECT * FROM ',dbName,'.networkconfiginfo']);
    
    % determine all NetworkConfig to update (current and future years)
    ToUpdate_NetworkConfigs = NetworkConfigInfo_Data.idNetworkConfig( NetworkConfigInfo_Data.year >= Year );
    %------------------------------------------------------------<<<<<<<<<<
    
    disp([' ->Network Configurations to edit: '])
    for y1=1:length(ToUpdate_NetworkConfigs)
        disp(['    Year = ',num2str(NetworkConfigInfo_Data.year(ToUpdate_NetworkConfigs(y1))),', idNetworkConfig = ',num2str(ToUpdate_NetworkConfigs(y1))])
    end
    
    %>>>>>>>>>>------------------------------------------------------------
    % Determine which lines to update are branches or transformers
    %>>>>>>>>>>------------------------------------------------------------
    
    % get line/trafo data from SQl (use idNetworkConfig for current year)
    branchTable = getBranchData(conn,idNetworkConfig);
    trafoTable = getTransformerData(conn,idNetworkConfig);
    
    % identify and separate the branch/trafo investments
    [idx_branch, zdummy1] = ismember(CentIvToMySQL.LineBuiltName, branchTable.LineName);
    [idx_trafo, zdummy2] = ismember(CentIvToMySQL.LineBuiltName, trafoTable.TrafoName);
    LineBuiltName = CentIvToMySQL.LineBuiltName(idx_branch);
    TrafoBuiltName = CentIvToMySQL.LineBuiltName(idx_trafo);
    
    % reorder the data for new investments to be in proper order according
    % to the branchTable order
    [z_dummy3, order_invBranch_all] = ismember(branchTable.LineName, LineBuiltName);
    % remove all non-relevant entries
    order_invBranch_all(order_invBranch_all==0) = [];
    % reorder all appropriate Invested data provided over this interface
    % this is currently only the .LineBuiltName
    LineBuiltName = LineBuiltName(order_invBranch_all);
    
    % reorder the data for new investments to be in proper order according
    % to the trafoTable order
    [z_dummy4, order_invTrafo_all] = ismember(trafoTable.TrafoName, TrafoBuiltName);
    % remove all non-relevant entries
    order_invTrafo_all(order_invTrafo_all==0) = [];
    % reorder all appropriate Invested data provided over this interface
    % this is currently only the .LineBuiltName
    TrafoBuiltName = TrafoBuiltName(order_invTrafo_all);
    %}
    %------------------------------------------------------------------
    
    
    %% Update Transmission Lines - fully built
    %>>>>>>>>>>------------------------------------------------------------
    % Update all Branches - only discrete version
    %>>>>>>>>>>------------------------------------------------------------
    
    disp([' ->Invested Transmission Lines (fully built):'])
    
    % test if any branches need to be updated
    if sum(idx_branch) > 0
        
        % UPDATE for any FULLY built line
        %   A) must add new line to linedata with new idLine, LineName,
        %      StartYr, EndYr
        %   B) must edit lines in lineconfiguration for all appropriate
        %      configs; will edit idLine, LineName, CandidateUnit=0;
        %------------------------------------------------------------------
        
        % adjust the name of the built line
        % (change from Cand to Inv)
        Str_beg_line = extractBefore(LineBuiltName,"cand");         % get the line name string before the 'Cand' (will use this exactly in the new name for the invested)
        Str_end_line = extractAfter(LineBuiltName,"cand");          % get the line name string after  the 'Cand' (will use this exactly in the new name for the invested)
        Str_name_line = strcat(Str_beg_line,'Inv',Str_end_line);  	% create new name for the new Invested line (replace 'Cand' with 'Inv')
        
        % Step A: add new lines (Invested) to linedata table
        %--------------------------------------------------------------
        % copy from candidate and only need to change idLine number,
        % LineName, StYr, EndYr,
        
        % get the original lineconfiguraiton table data
        LineConfigurationData = fetch(conn,['SELECT * FROM lineconfiguration']);
        % get the original linedata table data
        LineData = fetch(conn,['SELECT * FROM linedata']);
        % get the largest idLine (in case I need to define a new line and
        % must not use an existing idLine even if not in this lineconfig)
        idLine_last = max(LineData.idLine);
        
        % define the idLine for all new lines that need to be created
        idLine_new = (idLine_last+1:idLine_last+sum(idx_branch))';
        
        % get the linedata for all these lines from the Cd's in linedata,
        % no need to resort because it gets data one line at a time
        for iline = 1:length(LineBuiltName)
            LineData_new_disp_fully(iline,:) = LineData( find(cell2mat(cellfun(@(x) ismember(x, LineBuiltName(iline)), LineData.LineName, 'UniformOutput', 0)),1,'first'),: );
        end
        % overwrite the idLine with the new ones
        LineData_new_disp_fully.idLine = idLine_new;
        % overwrite the StartYr to be the year it's built
        LineData_new_disp_fully.StartYr = repmat(Year,[length(idLine_new),1]);
        % overwrite EndYr to be 2100
        LineData_new_disp_fully.EndYr = repmat(2100,[length(idLine_new),1]);
        % overwrite the LineNames with the new Invested ones
        LineData_new_disp_fully.LineName = Str_name_line;
        % just store MVA ratings of these lines
        disp_fully_MVA = LineData_new_disp_fully.rateA;
        % replace NaN in ?? with BLANKS
        %vars = {'eta_dis','eta_ch'};
        %LineData_new_disp_fully.eta_dis = num2cell(LineData_new_disp_fully.eta_dis);    % for some reason I needed to convert these columns to cell arrays of double instead of just double
        %LineData_new_disp_fully.eta_ch = num2cell(LineData_new_disp_fully.eta_ch);      % for some reason I needed to convert these columns to cell arrays of double instead of just double
        %ztemp = LineData_new_disp_fully{:,vars};
        %ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
        %LineData_new_disp_fully{:,vars} = ztemp; clear ztemp;
        
        % get the column names of the LineData table, needed to use
        % datainsert to put new data back into MySQL
        colNames = LineData_new_disp_fully.Properties.VariableNames;
        
        % push new line data to add to the gendata MySQl table
        datainsert(conn,'linedata',colNames,LineData_new_disp_fully);
        
        % display info about newly created lines
        for d1=1:length(LineBuiltName)
            disp(['    Added Fully Built to linedata: idLine = ',num2str(LineData_new_disp_fully.idLine(d1)),', ','MVA = ',num2str(disp_fully_MVA(d1)),', ',LineData_new_disp_fully.LineName{d1}])
        end
        
        % Step B: edit the new lines (Invested) to each lineconfiguration
        %--------------------------------------------------------------
        % modify idLine,LineName,CandidateUnit
        
        % define info for MySQL table to be updated
        tablename7 = 'lineconfiguration';
        colnames7 = {'idLine','LineName','Candidate'};
        
        % loop over all NetworkConfigs that need to be updated
        for c1=1:length(ToUpdate_NetworkConfigs)
            
            % loop over all fully built lines to update
            for l1=1:sum(idx_branch)
                
                % define new data to insert for this line
                % (idLine & LineName & CandidateStatus=0)
                newdata7 = {idLine_new(l1),Str_name_line{l1},0};
                
                % define WHERE clause for this update
                whereclause7 = {strcat('WHERE (idNetworkConfig = "',num2str(ToUpdate_NetworkConfigs(c1)),'") and (LineName = "',LineBuiltName{l1},'")')};
                
                % send this update to the database
                update(conn,tablename7,colnames7,newdata7,whereclause7)
                
                % don't repeat display
                if c1==1
                    disp(['    Edit  Fully Built in lineconfigs: idLine = ',num2str(idLine_new(l1)),', MVA = ',num2str(disp_fully_MVA(l1)),' MW, ',LineData_new_disp_fully.LineName{l1}])
                end
                
            end
            
        end
        
    else
        % no updates needed to branches
        disp(['    none'])
    end
    %}
    %------------------------------------------------------------------
    
    
    %% Update Transformers - fully built
    %>>>>>>>>>>------------------------------------------------------------
    % Update all Transformers - only discrete version
    %>>>>>>>>>>------------------------------------------------------------
    
    disp([' ->Invested Transformers (fully built):'])
    
    % test if any trafos need to be updated
    if sum(idx_trafo) > 0
        
         % UPDATE for any FULLY built trafos
        %   A) must add new trafo to transformerdata with new
        %      idTransformer, TrafoName, StartYr, EndYr
        %   B) must edit trafos in transformerconfiguration for all 
        %      appropriate configs; will edit idTransformer, TrafoName, 
        %      CandidateUnit=0;
        %------------------------------------------------------------------
        
        % adjust the name of the built trafo
        % (change from Cand to Inv)
        Str_beg_trafo = extractBefore(TrafoBuiltName,"cand");           % get the trafo name string before the 'Cand' (will use this exactly in the new name for the invested)
        Str_end_trafo = extractAfter(TrafoBuiltName,"cand");            % get the trafo name string after  the 'Cand' (will use this exactly in the new name for the invested)
        Str_name_trafo = strcat(Str_beg_trafo,'Inv',Str_end_trafo);     % create new name for the new Invested trafo (replace 'Cand' with 'Inv')
        
        % Step A: add new trafos (Invested) to transformerdata table
        %--------------------------------------------------------------
        % copy from candidate and only need to change idTransformer number,
        % TrafoName, StYr, EndYr,
        
        % get the original transformerconfiguraiton table data
        TrafoConfigurationData = fetch(conn,['SELECT * FROM transformerconfiguration']);
        % get the original transformerdata table data
        TrafoData = fetch(conn,['SELECT * FROM transformerdata']);
        % get the largest idTransformer (in case I need to define a new transformer and
        % must not use an existing idTransformer even if not in this genconfig)
        idTrafo_last = max(TrafoData.idTransformer);
        
        % define the idTransformer for all new trafos that need to be created
        idTrafo_new = (idTrafo_last+1:idTrafo_last+sum(idx_trafo))';
        
        % get the transformerdata for all these trafos from the Cd's in transformer data,
        % no need to resort because it gets data one trafo at a time
        for itrafo = 1:length(TrafoBuiltName)
            TrafoData_new_disp_fully(itrafo,:) = TrafoData( find(cell2mat(cellfun(@(x) ismember(x, TrafoBuiltName(itrafo)), TrafoData.TrafoName, 'UniformOutput', 0)),1,'first'),: );
        end
        % overwrite the idTransformer with the new ones
        TrafoData_new_disp_fully.idTransformer = idTrafo_new;
        % overwrite the StartYr to be the year it's built
        TrafoData_new_disp_fully.StartYr = repmat(Year,[length(idTrafo_new),1]);
        % overwrite EndYr to be 2100
        TrafoData_new_disp_fully.EndYr = repmat(2100,[length(idTrafo_new),1]);
        % overwrite the TrafoNames with the new Invested ones
        TrafoData_new_disp_fully.TrafoName = Str_name_trafo;
        % just store MVA ratings of these trafos
        disp_fully_MVA_trafo = TrafoData_new_disp_fully.rateA;
        % replace NaN in ?? with BLANKS
        %vars = {'eta_dis','eta_ch'};
        %LineData_new_disp_fully.eta_dis = num2cell(LineData_new_disp_fully.eta_dis);    % for some reason I needed to convert these columns to cell arrays of double instead of just double
        %LineData_new_disp_fully.eta_ch = num2cell(LineData_new_disp_fully.eta_ch);      % for some reason I needed to convert these columns to cell arrays of double instead of just double
        %ztemp = LineData_new_disp_fully{:,vars};
        %ztemp(cellfun(@(ztemp) any(isnan(ztemp(:))), ztemp)) = java.lang.String('');
        %LineData_new_disp_fully{:,vars} = ztemp; clear ztemp;
        
        % get the column names of the TrafoData table, needed to use
        % datainsert to put new data back into MySQL
        colNames = TrafoData_new_disp_fully.Properties.VariableNames;
        
        % push new trafo data to add to the gendata MySQl table
        datainsert(conn,'transformerdata',colNames,TrafoData_new_disp_fully);
        
        % display info about newly created trafos
        for d1=1:length(TrafoBuiltName)
            disp(['    Added Fully Built to transformerdata: idTransformer = ',num2str(TrafoData_new_disp_fully.idTransformer(d1)),', ','MVA = ',num2str(disp_fully_MVA_trafo(d1)),', ',TrafoData_new_disp_fully.TrafoName{d1}])
        end
        
        % Step B: edit the new trafos (Invested) to each transformerconfiguration
        %--------------------------------------------------------------
        % modify idTransformer,TrafoName,CandidateUnit
        
        % define info for MySQL table to be updated
        tablename8 = 'transformerconfiguration';
        colnames8 = {'idTransformer','TrafoName','Candidate'};
        
        % loop over all NetworkConfigs that need to be updated
        for c1=1:length(ToUpdate_NetworkConfigs)
            
            % loop over all fully built trafos to update
            for l1=1:sum(idx_trafo)
                
                % define new data to insert for this trafo
                % (idTransformer & TrafoName & CandidateStatus=0)
                newdata8 = {idTrafo_new(l1),Str_name_trafo{l1},0};
                
                % define WHERE clause for this update
                whereclause8 = {strcat('WHERE (idNetworkConfig = "',num2str(ToUpdate_NetworkConfigs(c1)),'") and (TrafoName = "',TrafoBuiltName{l1},'")')};
                
                % send this update to the database
                update(conn,tablename8,colnames8,newdata8,whereclause8)
                
                % don't repeat display
                if c1==1
                    disp(['    Edit  Fully Built in transformerconfigs: idTransformer = ',num2str(idTrafo_new(l1)),', MVA = ',num2str(disp_fully_MVA_trafo(l1)),' MW, ',TrafoData_new_disp_fully.TrafoName{l1}])
                end
                
            end
            
        end
        
    else
        % no updates needed to trafos
        disp(['    none'])
    end
    %}
    %------------------------------------------------------------------    
    
    
    %% SEND ALL TO DATABASE
    
    %conn.commit;
    %conn.AutoCommit = 'on';
    
    pause = 1;
    
end

