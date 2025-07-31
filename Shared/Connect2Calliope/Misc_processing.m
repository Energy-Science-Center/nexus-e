% Some code for testing Calliope data input

techs = unique(table_data{29}.techs);
locs = unique(table_data{29}.locs);
carriers = unique(table_data{29}.carriers);
subsectors = unique(table_data{8}.subsector);

% get the unique entries of techs-carriers (use other version below)
data_flow_in_subset = table(table_data{6}.techs,table_data{6}.carriers,'VariableNames',{'techs','carriers'});
[data_flow_in_subset_unique,ia] = unique(data_flow_in_subset,'rows','stable');
data_flow_in_unique = table_data{6}(ia,:);

% get the unique entries -----------------------------------------
% flow_in (techs - carriers)
[data_flow_in_subset_unique,ia1] = unique(table_data{6}(:,[2,4]),'rows','stable');
data_flow_in_unique = table_data{6}(ia1,:);

% net_import (exporting_region - importing_region)
[data_net_import_subset_unique,ia2] = unique(table_data{2}(:,[2,3]),'rows','stable');
data_net_import_unique = table_data{2}(ia2,:);

% nameplate_capacity (techs - carriers)
[data_nameplate_capacity_subset_unique,ia3] = unique(table_data{3}(:,[2,4]));
data_nameplate_capacity_unique = table_data{3}(ia3,:);

% storage_capacity (techs - carriers)
[data_storage_capacity_subset_unique,ia4] = unique(table_data{4}(:,[2,4]));
data_storage_capacity_unique = table_data{4}(ia4,:);

% flow_out (techs - carriers)
[data_flow_out_subset_unique,ia5] = unique(table_data{5}(:,[2,4]));
data_flow_out_unique = table_data{5}(ia5,:);

% net_transfer_capacity (techs - exporting_region - importing_region)
[data_net_transfer_capacity_subset_unique,ia6] = unique(table_data{1}(:,[2,3,6]));
data_net_transfer_capacity_unique = table_data{1}(ia6,:);

% duals (techs - carriers)
[data_duals_subset_unique,ia7] = unique(table_data{9}(:,[3]));
data_duals_unique = table_data{9}(ia7,:);
% only CHE
table_data_CH = table_data{9}(find(strcmp(table_data{9}.locs,'CHE')),:);
[data_duals_subset_unique_CH,ia8] = unique(table_data_CH(:,[3]));
data_duals_unique_CH = table_data_CH(ia8,:);
% ----------------------------------------------------------------

% detect which entry in table_data
idx_table_flowin = find(strcmp(table_names,'flow_in'));
% get unique timesteps (will sort by smallest)
timesteps_num = unique(table_data{idx_table_flowin}.timesteps);
timesteps_vec = datevec(timesteps_num);

% ----------------------------------------------------------------
% get CH imports/exports/net

% save results to other temp structure
%s1_new = CalliopeToNexuse;

% get idx of CH borders
CH_XB_export_idx = strcmp(CalliopeToNexuse.ImpExp_Borders_all.exporting_region,'CHE');
CH_XB_import_idx = strcmp(CalliopeToNexuse.ImpExp_Borders_all.importing_region,'CHE');

% get the names of these borders
CH_XB_export_names = CalliopeToNexuse.ImpExp_Borders_all(CH_XB_export_idx,:);
CH_XB_import_names = CalliopeToNexuse.ImpExp_Borders_all(CH_XB_import_idx,:);

% get XB flows
CH_XB_export_flows = CalliopeToNexuse.ImpExp_YrlyTot_all.ExpRegion2ImpRegion(CH_XB_export_idx,:); % are (+) values
CH_XB_import_flows = CalliopeToNexuse.ImpExp_YrlyTot_all.ExpRegion2ImpRegion(CH_XB_import_idx,:); % are (+) values

% Calc sum of all exports and all imports and net imports
CH_XB_exports_sum = sum(CH_XB_export_flows);
CH_XB_imports_sum = sum(CH_XB_import_flows);
CH_XB_NetImport = CH_XB_imports_sum - CH_XB_exports_sum;

% ----------------------------------------------------------------
% get non-CH demands

% totals
Demand_DE_s4 = data.BaseElecDemand_yrly.DE + data.DacElecDemand_yrly.DE + data.ImpExp_YrlyTot_need.XX2EU(2) + data.ImpExp_YrlyTot_need.EU2XX(2) + data.ElectrifiedDemands_yrly.DE_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.DE_emobility_flex_Calliope + data.ElectrifiedDemands_yrly.DE_heatpump;
Demand_DE_s3 = s3.BaseElecDemand_yrly.DE + s3.DacElecDemand_yrly.DE + s3.ImpExp_YrlyTot_need.XX2EU(2) + s3.ImpExp_YrlyTot_need.EU2XX(2) + s3.ElectrifiedDemands_yrly.DE_emobility_noflex_Calliope + s3.ElectrifiedDemands_yrly.DE_emobility_flex_Calliope + s3.ElectrifiedDemands_yrly.DE_heatpump;

%
data = s4_v4;

% separate by demand type (DE)
Demands_DE(1,1) = {'Base'};
Demands_DE(2,1) = {'DAC'};
Demands_DE(3,1) = {'NetExp'};
Demands_DE(4,1) = {'Export'};
Demands_DE(5,1) = {'Import'};
Demands_DE(6,1) = {'EV_Tot'};
Demands_DE(7,1) = {'EV_NoFlex'};
Demands_DE(8,1) = {'EV_Flex'};
Demands_DE(9,1) = {'HP'};
Demands_DE(10,1) = {'Total'};
Demands_DE(1,2) = num2cell(data.BaseElecDemand_yrly.DE);
Demands_DE(2,2) = num2cell(data.DacElecDemand_yrly.DE);
Demands_DE(3,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(2) + data.ImpExp_YrlyTot_need.EU2XX(2));
Demands_DE(4,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(2));
Demands_DE(5,2) = num2cell(data.ImpExp_YrlyTot_need.EU2XX(2));
Demands_DE(6,2) = num2cell(data.ElectrifiedDemands_yrly.DE_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.DE_emobility_flex_Calliope);
Demands_DE(7,2) = num2cell(data.ElectrifiedDemands_yrly.DE_emobility_noflex_Calliope);
Demands_DE(8,2) = num2cell(data.ElectrifiedDemands_yrly.DE_emobility_flex_Calliope);
Demands_DE(9,2) = num2cell(data.ElectrifiedDemands_yrly.DE_heatpump);
Demands_DE(10,2) = num2cell(data.BaseElecDemand_yrly.DE + data.DacElecDemand_yrly.DE + data.ImpExp_YrlyTot_need.XX2EU(2) + data.ImpExp_YrlyTot_need.EU2XX(2) + data.ElectrifiedDemands_yrly.DE_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.DE_emobility_flex_Calliope + data.ElectrifiedDemands_yrly.DE_heatpump);

% separate by demand type (FR)
Demands_FR(1,1) = {'Base'};
Demands_FR(2,1) = {'DAC'};
Demands_FR(3,1) = {'NetExp'};
Demands_FR(4,1) = {'Export'};
Demands_FR(5,1) = {'Import'};
Demands_FR(6,1) = {'EV_Tot'};
Demands_FR(7,1) = {'EV_NoFlex'};
Demands_FR(8,1) = {'EV_Flex'};
Demands_FR(9,1) = {'HP'};
Demands_FR(10,1) = {'Total'};
Demands_FR(1,2) = num2cell(data.BaseElecDemand_yrly.FR);
Demands_FR(2,2) = num2cell(data.DacElecDemand_yrly.FR);
Demands_FR(3,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(3) + data.ImpExp_YrlyTot_need.EU2XX(3));
Demands_FR(4,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(3));
Demands_FR(5,2) = num2cell(data.ImpExp_YrlyTot_need.EU2XX(3));
Demands_FR(6,2) = num2cell(data.ElectrifiedDemands_yrly.FR_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.FR_emobility_flex_Calliope);
Demands_FR(7,2) = num2cell(data.ElectrifiedDemands_yrly.FR_emobility_noflex_Calliope);
Demands_FR(8,2) = num2cell(data.ElectrifiedDemands_yrly.FR_emobility_flex_Calliope);
Demands_FR(9,2) = num2cell(data.ElectrifiedDemands_yrly.FR_heatpump);
Demands_FR(10,2) = num2cell(data.BaseElecDemand_yrly.FR + data.DacElecDemand_yrly.FR + data.ImpExp_YrlyTot_need.XX2EU(3) + data.ImpExp_YrlyTot_need.EU2XX(3) + data.ElectrifiedDemands_yrly.FR_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.FR_emobility_flex_Calliope + data.ElectrifiedDemands_yrly.FR_heatpump);

% separate by demand type (IT)
Demands_IT(1,1) = {'Base'};
Demands_IT(2,1) = {'DAC'};
Demands_IT(3,1) = {'NetExp'};
Demands_IT(4,1) = {'Export'};
Demands_IT(5,1) = {'Import'};
Demands_IT(6,1) = {'EV_Tot'};
Demands_IT(7,1) = {'EV_NoFlex'};
Demands_IT(8,1) = {'EV_Flex'};
Demands_IT(9,1) = {'HP'};
Demands_IT(10,1) = {'Total'};
Demands_IT(1,2) = num2cell(data.BaseElecDemand_yrly.IT);
Demands_IT(2,2) = num2cell(data.DacElecDemand_yrly.IT);
Demands_IT(3,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(4) + data.ImpExp_YrlyTot_need.EU2XX(4));
Demands_IT(4,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(4));
Demands_IT(5,2) = num2cell(data.ImpExp_YrlyTot_need.EU2XX(4));
Demands_IT(6,2) = num2cell(data.ElectrifiedDemands_yrly.IT_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.IT_emobility_flex_Calliope);
Demands_IT(7,2) = num2cell(data.ElectrifiedDemands_yrly.IT_emobility_noflex_Calliope);
Demands_IT(8,2) = num2cell(data.ElectrifiedDemands_yrly.IT_emobility_flex_Calliope);
Demands_IT(9,2) = num2cell(data.ElectrifiedDemands_yrly.IT_heatpump);
Demands_IT(10,2) = num2cell(data.BaseElecDemand_yrly.IT + data.DacElecDemand_yrly.IT + data.ImpExp_YrlyTot_need.XX2EU(4) + data.ImpExp_YrlyTot_need.EU2XX(4) + data.ElectrifiedDemands_yrly.IT_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.IT_emobility_flex_Calliope + data.ElectrifiedDemands_yrly.IT_heatpump);

% separate by demand type (AT)
Demands_AT(1,1) = {'Base'};
Demands_AT(2,1) = {'DAC'};
Demands_AT(3,1) = {'NetExp'};
Demands_AT(4,1) = {'Export'};
Demands_AT(5,1) = {'Import'};
Demands_AT(6,1) = {'EV_Tot'};
Demands_AT(7,1) = {'EV_NoFlex'};
Demands_AT(8,1) = {'EV_Flex'};
Demands_AT(9,1) = {'HP'};
Demands_AT(10,1) = {'Total'};
Demands_AT(1,2) = num2cell(data.BaseElecDemand_yrly.AT);
Demands_AT(2,2) = num2cell(data.DacElecDemand_yrly.AT);
Demands_AT(3,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(1) + data.ImpExp_YrlyTot_need.EU2XX(1));
Demands_AT(4,2) = num2cell(data.ImpExp_YrlyTot_need.XX2EU(1));
Demands_AT(5,2) = num2cell(data.ImpExp_YrlyTot_need.EU2XX(1));
Demands_AT(6,2) = num2cell(data.ElectrifiedDemands_yrly.AT_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.AT_emobility_flex_Calliope);
Demands_AT(7,2) = num2cell(data.ElectrifiedDemands_yrly.AT_emobility_noflex_Calliope);
Demands_AT(8,2) = num2cell(data.ElectrifiedDemands_yrly.AT_emobility_flex_Calliope);
Demands_AT(9,2) = num2cell(data.ElectrifiedDemands_yrly.AT_heatpump);
Demands_AT(10,2) = num2cell(data.BaseElecDemand_yrly.AT + data.DacElecDemand_yrly.AT + data.ImpExp_YrlyTot_need.XX2EU(1) + data.ImpExp_YrlyTot_need.EU2XX(1) + data.ElectrifiedDemands_yrly.AT_emobility_noflex_Calliope + data.ElectrifiedDemands_yrly.AT_emobility_flex_Calliope + data.ElectrifiedDemands_yrly.AT_heatpump);


