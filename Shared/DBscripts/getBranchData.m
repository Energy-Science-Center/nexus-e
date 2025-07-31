% =========================================================================
%>
%> (c)ETH Z�rich 2022
%>
%> 
%> 
%> author : Jared Garrison
%> email: garrison@fen.ethz.ch
%>
%> project : Nexus-e
%> 
%> 
%> ========================================================================
%> 
%> @brief get transmission line configuration from database wrapped 
%<        nexus.getBranchData procedure
%> 
%> @in  conn:      conn = database('MySQL-psl','philipp','');
%>      idNetworkConfig: DB config Identifier 
%> @out infoTable: generatorData table
% =========================================================================
function infoTable = getBranchData(conn,idNetworkConfig)

setdbprefs('DataReturnFormat','table')  % ensures the fetch commands return data as Tables (not Cell Arrays)

infoTable = fetch(conn,['call getBranchData(',int2str(idNetworkConfig),')']);


end