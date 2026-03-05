---
title: Input data analysis
---
* demand profiles from the Excel file are now overwritten by the nodal files
* columns that are not uploaded (deprecated, used for visualisation,...):
  * (possible) gens.GenNum
  * probably only one among them is actually used: gens.AfemNodeNum gens.NodeCode gens.NodeName gens.NodeId
  * gens.Country gens.Canton gens.CantonCode
  * "white columns are not used" according to Ambra citing Jared
  * gens.Pmin or gens.Pmin_database?
  * CentFlexPotential where flex_type == emobility or Load_heatpump
* columns we don't know the prupose of
  * gens.Status
  * gens.Mean_Error_Forecast_24 gens.Sigma_Error_Forecast_24