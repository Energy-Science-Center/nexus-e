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
%> @brief get transmission transformer configuration from database wrapped 
%<        nexus.getTransformerData procedure
%> 
%> @in  conn:      conn = database('MySQL-psl','philipp','');
%>      idNetworkConfig: DB config Identifier 
%> @out infoTable: generatorData table
% =========================================================================
function infoTable = getTransformerData(conn,idNetworkConfig)

setdbprefs('DataReturnFormat','table')  % ensures the fetch commands return data as Tables (not Cell Arrays)

infoTable = fetch(conn,['call getTransformerData(',int2str(idNetworkConfig),')']);


end