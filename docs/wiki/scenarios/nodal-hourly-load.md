## Input nodal-hourly load data and flexibility parameters

The electricity demands (Conventional, eMobility, HeatPump, and Electrolysis) are now setup and put into the MySQL database using nodal-hourly profiles defined in CSV files for each year. Similarly, the CentIv model now also allows for the EV and HP demand to shift within defined limits and the flexibility parameters are also defined using similar nodal-hourly input CSV files. All these files are contained within one folder located in the `/nexus-e-framework/scenario_data/source_excel` folder alongside the main input excel files. Right now, there are example datasets for everyone to use already prepared and tested; they are given in [TYNDP24](../scenarios/tyndp24.md) and use the TYNDP2024 data. The two versions setup are identical except one ‘turns off’ EV flexibility.

The data in these two examples are described in a short ‘ReadMe.txt’ file within each folder. To summarize, the CH demands are scaled to match the annual totals for 2030-2050 based on the EP2050+ ZeroBASIS scenario while the non-CH demands are set based on the data from TYNDP2024. EV and HP demand profile shapes are unique per node and based on the data provided by Maria Parajeles Herrera (EVs) and Yi Guo (HPs). The Conventional and Electrolysis demand profiles are based on the same population-based split we have always used until now for the electricity demands (every node has the same shape).

The EV flexibility bounds are currently defined such that the EV flexibility should only be ‘turned on’ for simulations of 2-day and 1-day resolution. So, for testing using 8-day resolution, please create databases with EV flexibility ‘turned off’. This limitation can be overcome moving forward, but a correction is not planned in the near term.

If scaling the EV or HP demands, you must also scale the flexibility parameters. It is fine to use the same scale factor for the flexibility parameters as for the demand when doing this. Avoid any rounding when scaling the flexibility parameters because it could lead to infeasibility if the bounds cross over each other.

To turn off the flexibility of EV and HP demand do the following:

### EV demand

- set as zeros all the daily flexible energy parameters (`XXXX_eMobility_Flex_FE_NodalDaily_MWh.csv`) in the input CSV files and make a new database with these.
- This is already prepared in the CSVs located in the folder `Nexuse_DB-Input_v48_TYNDP24-DE09_NoEVflex_DemandProfiles`.
- There is currently no way in the code or the run script to turn off EV flexibility.

### HP demand

- In the CentIv module script `create_scenario_fast.py` modify the parameter named `self.HPFlexiblePercentage` to equal 0.
- The default for this parameter is 0.33 (1/3 of all Heat Pumps allow direct load control and operate with the flexibility bounds defined in the input data). This 33% limit is based on other references for the annual heat pump shift (Jan Linder’s master’s thesis) and the annual EV shift (from results using Maria’s EV flexibility data), which both yield around 20-25% of the total HP and EV demand shifting. This assumption is taken so that both the EV flexibility and HP flexibility have a similarly conservative estimate (since the EV flex is also limited with no V2G and even the V1G does not impact the SOC at the end of each parking event).