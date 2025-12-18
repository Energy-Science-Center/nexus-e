% =========================================================================
%>
%> (c)ETH Z�rich 2018
%>
%> 
%> 
%> author : Philipp Fortenbacher
%> email: fortenbacher@fen.ethz.ch
%>
%> project : Nexus
%> 
%> 
%> ========================================================================
%> 
%> @brief get generator configuration from database wrapped 
%<        nexus.getGeneratorData procedure
%> 
%> @in  conn:      conn = database('MySQL-psl','philipp','');
%>      idGenConfig: DB config Identifier 
%> @out infoTable: generatorData table
% =========================================================================
function infoTable = getGenData(conn,idGenConfig)

setdbprefs('DataReturnFormat','table')  % ensures the fetch commands return data as Tables (not Cell Arrays)

infoTable = fetch(conn,['call getGeneratorData(',int2str(idGenConfig),')']);


end
