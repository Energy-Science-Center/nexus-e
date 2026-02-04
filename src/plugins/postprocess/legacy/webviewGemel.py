######################
# This script is "translated" from webviewGemel.R.
#######################

import pandas as pd
import os
import argparse

current_directory = os.path.dirname(os.path.abspath(__file__))

## Definition of all arguments passed through the command line
parser = argparse.ArgumentParser(description='Process GemEl results')
# Source file: By default, it is left empty, then the script will look for and read from the lsf.o* file.
parser.add_argument("--simu-name", type=str, help="Simulation name (i.e., result folder's name)",
                    default='nexus_disagg_nuc50_Jun27_T1020_NoResTarg_27-Jun-2020_10-20')
args = parser.parse_args()

simu_name = args.simu_name


def prepare_data(data, column_names):
    data = pd.read_csv(f"{current_directory}/Outputs/{simu_name}/GemEl/{data}")
    # rename the columns in place
    data.columns = column_names
    # remove non-informative columns in place
    data.drop(["Indicator", "Scenario"], axis=1, inplace=True)
    # keep only data where Year > 2019
    data = data[data['Year'] > 2019]
    return data


# Macroeconomic
column_names = ["Scenario", "Year", "Indicator", "Parameter", "Value [billion CHF]"]
g_d_macro = prepare_data("results_agg.csv", column_names)
# Select parameters and store them in another data frame
desired_param = ["GDPe (nv)", "GDPp (nv)", "Exports (nv)", "Imports (nv)", "Investment (nv)"]
selected_macro = g_d_macro[g_d_macro['Parameter'].isin(desired_param)]
# rename some parameters to be more understandable and the new names will be shown
# in the webviewer.
final_macro = selected_macro.replace({'Parameter': {
    "GDPe (nv)": "GDP (expenditure approach)",
    "GDPp (nv)": "GDP (production approach)",
    "Exports (nv)": "Exports",
    "Imports (nv)": "Imports",
    "Investment (nv)": "Investment"
}})

# CO2
column_names = ["Scenario", "Year", "Indicator", "Sector", "Value"]
g_d_co2 = prepare_data("results_co2.csv", column_names)
renamed_co2 = g_d_co2.replace({'Sector': {
    "Energy sectors": "Energy",
    "Agriculture sector": "Agriculture",
}})
final_co2 = renamed_co2.pivot(index="Year", columns="Sector", values="Value")

# Sectors
column_names = ["Scenario", "Year", "Indicator", "Sector", "Parameter", "Value"]
g_d_sec = prepare_data("results_sec.csv", column_names)
# Change all sector abbreviations to uppercase, so it can be joined with the sector_names dataframe below.
g_d_sec["Sector"] = g_d_sec["Sector"].str.upper()
# "EDT" stands for the "services of electricity distribution and trade" sector. We remove this row because detailed
# information of the electricity sector can be found from other results, and also the unit of EDT sector's price is
# not aligned with other sectors.
g_d_sec = g_d_sec[g_d_sec['Sector'] != "EDT"]
# Use the description of the sectors (not the acronyms)
sector_names = pd.read_csv(f"{current_directory}/Dependencies/economic_sector_names.csv")
sector_names["Full_name"] = sector_names["Abbreviation"] + ": " + [x.split(":")[1] for x in sector_names["Full_name"]]
# how="left" means to remove rows in sector_names if it doesn't exist in g_d_sec.
g_d_sec = g_d_sec.merge(sector_names, left_on="Sector", right_on="Abbreviation", how="left")
# Substitute the "Sector" column with the "Full_name" column
g_d_sec = g_d_sec.drop("Sector", axis=1)
g_d_sec = g_d_sec.rename(columns={"Full_name": "Sector"})
# Select certain parameters to show based on Florian's advice
desired_param = ["Domestic user price", "Imports", "Exports", "Output Level"]
selected_sec = g_d_sec[g_d_sec['Parameter'].isin(desired_param)]
# rename some parameters to be more understandable and the new names will be shown
# in the webviewer.
final_sec = selected_sec.replace({'Parameter': {
    "Domestic user price": "Domestic user prices [-]",
    "Imports": "Imports [billion CHF]",
    "Exports": "Exports [billion CHF]",
    "Output Level": "Output level [billion CHF]",
}})

# Save results to nexus-e/Shared/postProcess/Outputs/<simu_name>/GemEl/
if not os.path.exists(f'Outputs/{simu_name}/GemEl'):
    os.makedirs(f"Outputs/{simu_name}/GemEl", exist_ok=True)
final_sec.to_csv(f'Outputs/{simu_name}/GemEl/national_sectors.csv', index=False)
final_macro.to_csv(f'Outputs/{simu_name}/GemEl/national_macroeconomics.csv', index=False)
final_co2.to_csv(f'Outputs/{simu_name}/GemEl/national_co2_million_ton.csv')
