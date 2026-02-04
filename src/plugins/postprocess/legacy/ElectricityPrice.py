import os
from pathlib import Path
import pandas as pd

from ..results_context import get_years_simulated_by_centiv

annual_elprices_dfs = {
        "c": pd.DataFrame([]),
        "e": pd.DataFrame([])
    }

annual_elprices_dfs = {
        "c": pd.DataFrame([]),
        "e": pd.DataFrame([])
    }

def create_elprices(
    y,
    CentIvDirectory,
    flexeco_dir,
    simulation_postprocess_path,
    simu_name,
    single_electric_node,
    elprices_dfs
):
    country_names = {
        "CH": "Switzerland",
        "DE": "Germany",
        "IT": "Italy",
        "AT": "Austria",
        "FR": "France"
    }
    
    # create electricity price files for CentIV and FlexEco
    # CentIV
    # electricity Prices - excel path
    fn18 = os.path.join(CentIvDirectory, "ElPrice_hourly_adjustedDistIvABM_CH.xlsx")
    fn19 = os.path.join(CentIvDirectory, "ElPrice_hourly_adjustedDistIvABM_Neighbours.xlsx")

    # Demand - excel path
    fn20 = os.path.join(CentIvDirectory, "Demand_hourly_CH.xlsx")
    fn21 = os.path.join(CentIvDirectory, "Demand_hourly_DE.xlsx")
    fn22 = os.path.join(CentIvDirectory, "Demand_hourly_IT.xlsx")
    fn23 = os.path.join(CentIvDirectory, "Demand_hourly_FR.xlsx")
    fn24 = os.path.join(CentIvDirectory, "Demand_hourly_AT.xlsx")

    # load nodal electricity prices for each country
    elecprice_nodal_ch = pd.read_excel(fn18).drop(["Unnamed: 0"], axis=1)
    elecprice_nodal_neig = pd.read_excel(fn19).drop(["Unnamed: 0"], axis=1)

    # get one df with all nodal electricity prices per country
    elecprice_nodal_all = pd.concat([elecprice_nodal_ch, elecprice_nodal_neig], axis=1)

    # load nodal electricity demand for each country
    demand_nodal_ch = pd.read_excel(fn20).drop(["Unnamed: 0"], axis=1)
    demand_nodal_de = pd.read_excel(fn21).drop(["Unnamed: 0"], axis=1)
    demand_nodal_it = pd.read_excel(fn22).drop(["Unnamed: 0"], axis=1)
    demand_nodal_fr = pd.read_excel(fn23).drop(["Unnamed: 0"], axis=1)
    demand_nodal_at = pd.read_excel(fn24).drop(["Unnamed: 0"], axis=1)

    # get one df with all demand per country
    demand_nodal_all = pd.concat([demand_nodal_ch, demand_nodal_de, demand_nodal_it, demand_nodal_fr, demand_nodal_at], axis=1)
    # to avoid negative weighting we set all negative demands to 0
    demand_nodal_all = demand_nodal_all.clip(lower=0)

    # identify the nodes in the neighboring countries
    nodes_ch = list(demand_nodal_ch.columns)
    nodes_de = list(demand_nodal_de.columns)
    nodes_it = list(demand_nodal_it.columns)
    nodes_fr = list(demand_nodal_fr.columns)
    nodes_at = list(demand_nodal_at.columns)
    nodes_dict = {"Switzerland": nodes_ch, "Germany": nodes_de, "Italy": nodes_it, "France": nodes_fr, "Austria": nodes_at}

    # initialize dictionary
    elecprice_country_hourly_nodal_weighted_avg_dict = {}
    demand_country_hourly_dict = {}

    # get load-weighted nodal hourly electricity prices
    for country in nodes_dict.keys():
        nodes_list = nodes_dict[country]
        demand_country_hourly_dict[country] = demand_nodal_all[nodes_list].sum(axis=1)
        if single_electric_node:         
            # If single electric node is activated, only use the prices, no weighting is needed because there is only one node
            if country=="Switzerland":
                elecprice_country_hourly_nodal_weighted_avg_dict[country] = elecprice_nodal_all["CH00"]
            else:
                # find nodes that are in columns of elecprice_nodal_all and nodes_list (their intersection)
                nodes_with_price_country = list(set(elecprice_nodal_all.columns).intersection(nodes_list))  #NOTE: this is not acceptable. Given current defintion of the nodes and countries, there are two nodes for each country, but only one has a price, but the demand (demand_nodal_all[nodes_with_price_country].sum(axis=1)) has values in two nodes.
                elecprice_country_hourly_nodal_weighted_avg_dict[country] = elecprice_nodal_all.loc[:,nodes_with_price_country[0]]
        else: # normal case, i.e., with multiple nodes in CH
            demand_country_yearly = demand_nodal_all[nodes_list].sum(axis=1)
            demand_nodal_weights = demand_nodal_all[nodes_list].div(demand_country_yearly, axis=0)
            elecprice_weighted = elecprice_nodal_all[nodes_list] * demand_nodal_weights
            elecprice_weighted_avg = elecprice_weighted.sum(axis=1)
            elecprice_country_hourly_nodal_weighted_avg_dict[country] = elecprice_weighted_avg

    # hourly elprice per country from dict to df
    elprices_dfs['c'] = pd.DataFrame(data=elecprice_country_hourly_nodal_weighted_avg_dict)
    elprices_dfs['c'].index.name = "Hour"

    # hourly demand per country from dict to df
    demand_country_hourly_df = pd.DataFrame(data=demand_country_hourly_dict)
    demand_country_hourly_df.index.name = "Hour"

    # write hourly electricity price
    elprices_dfs['c'].to_csv(
        os.path.join(
            simulation_postprocess_path,
            "national_generation_and_capacity",
            f"national_elecprice_hourly_c_{y}.csv"
        )
    )



    # FlexEco
    # check if FlexEco result exist
    if os.path.isdir(flexeco_dir):
        elprices_dfs['e'] = pd.DataFrame([])
        for c in country_names:
            file_path = os.path.join(flexeco_dir, f"Prices_hourly_{c}_EuroPerMWh.csv")
            elprice = pd.read_csv(file_path)

            elprices_dfs['e'][country_names[c]] = elprice['electricity']

        # write hourly electricity price
        elprices_dfs['e'].index.name = "Hour"
        elprices_dfs['e'].to_csv(
            os.path.join(
                simulation_postprocess_path,
                "national_generation_and_capacity",
                f"national_elecprice_hourly_e_{y}.csv"
            )
        )

    # write monthly and annual data
    for model in elprices_dfs:
        if model == "c":
            #get hourly prices
            elprice_country_hourly_df = elprices_dfs['c']
            #add timestamp to hourly data
            elprice_country_hourly_df['date'] = pd.date_range(start='1/1/2018', periods=len(elprice_country_hourly_df), freq='H')
            # Resample to get the monthly sum
            elprice_weighted_country_monthly_df = elprice_country_hourly_df.resample('M', on='date').sum()
            # Calculate the number of hours in each month (number of days in the month * 24 hours)
            hours_in_month = elprice_country_hourly_df.resample('M', on='date').size()
            # Divide the monthly sum by the number of hours in each month
            elprice_weighted_country_monthly_df = elprice_weighted_country_monthly_df.div(hours_in_month, axis=0)
            # reset index
            elprice_weighted_country_monthly_df.index = range(1, 1 + len(elprice_weighted_country_monthly_df)) # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
            elprice_weighted_country_monthly_df.index.name = "Month"

            # save to csv
            elprice_weighted_country_monthly_df.to_csv(
                os.path.join(
                    simulation_postprocess_path,
                    "national_generation_and_capacity",
                    f"national_elecprice_monthly_{model}_{y}.csv"
                )
            )

            # annual electricity price
            total_yearly_load = demand_country_hourly_df[["Switzerland", "Germany", "Italy", "France", "Austria"]].sum(axis=0)
            demand_hourly_weights_year = demand_country_hourly_df[["Switzerland", "Germany", "Italy", "France", "Austria"]].div(total_yearly_load, axis=1)
            elecprice_weighted_country_yearly_df = demand_hourly_weights_year * elprices_dfs['c']
            annual_elprices_dfs[model][y] = elecprice_weighted_country_yearly_df.sum(axis=0)

        else:
            # no changes made for the flexeco case
            elprice_m = elprices_dfs[model]
            elprice_m['date'] = pd.date_range(start='1/1/2018', periods=len(elprice_m), freq='H')
            elprice_m = elprice_m.resample('M', on='date').mean()
            elprice_m.reset_index(inplace=True)
            elprice_m["Month"] = range(1, 1 + len(elprice_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
            elprice_m.drop(["date"], axis=1, inplace=True)
            elprice_m = elprice_m.set_index("Month")
            elprice_m.to_csv(
                os.path.join(
                    simulation_postprocess_path,
                    "national_generation_and_capacity",
                    f"national_elecprice_monthly_{model}_{y}.csv"
                )
            )

            # annual electricity price
            # elprices_dfs[model].drop('date', axis=1, inplace=True)
            annual_elprices_dfs[model][y] = elprices_dfs[model].mean(axis=0, numeric_only=False)


    return

def write_annual_elprices(
    annual_elprices_dfs,
    simulation_postprocess_path,
    simu_name
):
    for model in annual_elprices_dfs:
        # annual electricity price
        annual_elprices_dfs[model].index.name = "Row"
        annual_elprices_dfs[model].to_csv(
            os.path.join(
                simulation_postprocess_path,
                "national_generation_and_capacity",
                f"national_elecprice_annual_{model}.csv"
            )
        )


def main(simulation: str, single_electric_node: bool):
    simulation_postprocess_path = "postprocess"
    centiv_years = get_years_simulated_by_centiv(Path())
    elprices_dfs = {}

    for year in centiv_years:
        global CentIvDirectory

        CentIvDirectory = f"CentIv_{year}"

        global flexeco_dir
        flexeco_dir = f"FlexEco_{year}"
  
        create_elprices(year,CentIvDirectory,flexeco_dir,simulation_postprocess_path,simulation,single_electric_node,elprices_dfs)
        write_annual_elprices(annual_elprices_dfs, simulation_postprocess_path, simulation)