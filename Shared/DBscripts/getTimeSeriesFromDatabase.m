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
%> @brief gets profile time series data
%> 
%> @in  conn:      conn = database('MySQL-psl','philipp','');
%>      profileId: Identifier from database table nexus.profiledata
%> @out TimeSeries: profile series vector
% =========================================================================

function TimeSeries =  getTimeSeriesFromDatabase(conn,profileID)

setdbprefs('DataReturnFormat','table')  % ensures the fetch commands return data as Tables (not Cell Arrays)

Data = fetch(conn,['select timeSeriesData from profiledata,JSON_TABLE(timeSeries,"$[*]" COLUMNS (timeSeriesData double PATH ''$'')) as timeSeries WHERE idProfile=',int2str(profileID),';']);
TimeSeries = Data.timeSeriesData;

end