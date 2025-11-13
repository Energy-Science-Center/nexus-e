import os
from pathlib import Path
import pandas as pd
import numpy as np
import pymysql
import argparse

from .Generation import group_n_rename
from ..results_context import get_years_simulated_by_centiv

from nexus_e import config as config

def create_revenue(
    years,
    model,
    simulation_postprocess_path,
    database,
    host: str,
    user :str,
    password: str
):
    # create revenue data by multiplying price with hourly generation
    country_names = {
        'CH': 'Switzerland',
        'FR': 'France',
        'IT': 'Italy',
        'AT': 'Austria',
        'DE': 'Germany'
    }

    year_dic = {
        2018: 1,
        2020: 2,
        2030: 3,
        2040: 4,
        2050: 5
    }

    output_directory = os.path.join(
        simulation_postprocess_path,
        "national_generation_and_capacity"
    )
    # read annual prices as reference for the plot
    annual_elprices_filepath = os.path.join(
        output_directory,
        f"/national_elecprice_annual_{model}.csv"
    )

    if os.path.isfile(annual_elprices_filepath):
        elec_prices = pd.read_csv(annual_elprices_filepath, low_memory=False)
        elec_prices = elec_prices.T
        elec_prices.columns = elec_prices.iloc[0]
        elec_prices.drop('Row', inplace=True)

        for country in country_names:
            # read grouped capacity file
            capacity_grouped = pd.read_csv(
                os.path.join(
                    output_directory,
                    f"national_capacity_gw_{country.lower()}.csv"
                ),
                index_col=0, 
                low_memory=False
            )
            capacity_grouped.columns = [int(x) for x in capacity_grouped.columns]
            revenue_all_years = pd.DataFrame()
            revenue_per_gen_all_years = pd.DataFrame()
            revenue_per_cap_all_years = pd.DataFrame()

            profit_all_years = pd.DataFrame()
            profit_per_gen_all_years = pd.DataFrame()
            profit_per_cap_all_years = pd.DataFrame()

            # list of possible models for loop
            years_list = []
            for year in years:
                # check if and electricity price file exist
                price_file = os.path.join(
                    output_directory,
                    f"national_elecprice_hourly_{model}_{year}.csv"
                )
                if os.path.exists(price_file):
                    # read price file
                    price_h = pd.read_csv(price_file, low_memory=False)
                    gen_file = os.path.join(
                        simulation_postprocess_path,
                        f"Gen_{country}_{year}_{model}.csv"
                    )
                    # check if generation file exists
                    if os.path.exists(gen_file):
                        # REVENUE
                        # ignore
                        tech_ignored = []
                        if country == 'CH':
                            tech_ignored = ['Load (Total)', 'Imports', 'Exports', 'Load_Shed', 'Load_Shed-Ind']

                        gen_annual_h = pd.read_csv(gen_file, low_memory=False)

                        # ignore technologies with less than 1 GWh production per year
                        gen_a = gen_annual_h.sum(axis=0)
                        gen_a.drop('Hour')
                        neg_tech = gen_a.loc[lambda x: abs(x) < 1000].index.tolist()
                        for col in neg_tech:
                            gen_annual_h[col].values[:] = 0

                        # price array to series
                        s_price_h = price_h[country_names[country]]

                        # multiply all rows with electricity price
                        # MWh * Euro / MWh
                        revenue_h = gen_annual_h.apply(lambda x: np.asarray(x) * np.asarray(s_price_h))

                        # hourly revenue
                        # write to csv file
                        revenue_h.drop(columns=['Hour'] + tech_ignored, inplace=True, errors='ignore')
                        revenue_h.index.rename('Hour', inplace=True)
                        group_n_rename(revenue_h,index_name="Hour").to_csv(
                            os.path.join(
                                output_directory,
                                f'generation_revenue_hourly_{model}_{country.lower()}_{year}.csv'
                            )
                        )

                      
                        # monthly revenue
                        revenue_m = revenue_h.copy()
                        revenue_m['date'] = pd.date_range(start='1/1/2018', periods=len(revenue_m), freq='H')
                        revenue_m = revenue_m.resample('M', on='date').sum()
                        revenue_m = revenue_m.reset_index()
                        revenue_m["Month"] = range(1, 1 + len(revenue_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
                        revenue_m = revenue_m.drop(["date"], axis=1)
                        revenue_m = revenue_m.set_index("Month")
                        # write to csv file
                        group_n_rename(revenue_m,index_name="Month").to_csv(
                            os.path.join(
                                output_directory,
                                f'generation_revenue_monthly_{model}_{country.lower()}_{year}.csv'
                            )
                        )

                        # annual revenue
                        revenue_a = revenue_h.sum(axis=0)
                        revenue_a.index.rename('Row', inplace=True)
                        revenue_a.columns = [year]

                        # combine all annual columns to a dataframe
                        revenue_all_years = pd.concat([revenue_all_years, revenue_a], axis=1)
                        years_list.append(year)

                        # REVENUE PER GEN
                        revenue_per_gen_a = pd.DataFrame([])
                        revenue_a_grouped = group_n_rename(revenue_a.to_frame(), transposed=True).transpose()
                        gen_a_grouped = group_n_rename(gen_a.to_frame(), transposed=True).transpose()

                        for tech in revenue_a_grouped.columns:
                            revenue_per_gen_a[tech] = np.divide(revenue_a_grouped[tech], abs(gen_a_grouped[tech]),
                                                                out=np.zeros_like(revenue_a_grouped[tech]),
                                                                where=gen_a_grouped[tech] != 0)

                        revenue_per_gen_all_years = pd.concat([revenue_per_gen_all_years, revenue_per_gen_a], axis=0)

                        # REVENUE PER CAPACITY
                        revenue_per_cap_a = pd.DataFrame([])

                        for gentype in revenue_a_grouped.columns:
                            if ' (Gen)' in gentype:
                                gentype_cap = gentype.replace(' (Gen)', '')
                            elif ' (Load)' in gentype:
                                gentype_cap = gentype.replace(' (Load)', '')
                            else:
                                gentype_cap = gentype

                            if gentype_cap in capacity_grouped.index:
                                revenue_per_cap_a[gentype] = np.divide(revenue_a_grouped[gentype],
                                                                       capacity_grouped.loc[gentype_cap, year],
                                                                       out=np.zeros_like(revenue_a_grouped[gentype]),
                                                                       where=capacity_grouped.loc[
                                                                                 gentype_cap, year] != 0)
                            else:
                                revenue_per_cap_a[gentype] = 0

                        revenue_per_cap_all_years = pd.concat([revenue_per_cap_all_years, revenue_per_cap_a], axis=0)

                        # PROFIT
                        # connect to database for variable costs
                        conn = pymysql.connect(host=host, database=database, user=user, password=password)
                        cursor = conn.cursor()
                        cursor.execute(f'call {database}.getGeneratorData({year_dic[year]})')
                        results = cursor.fetchall()
                        cursor.execute(f'call {database}.getDistGeneratorData({year_dic[year]})')
                        results2 = cursor.fetchall()

                        cursor.close()
                        conn.close()

                        # load into dataframe
                        df = pd.DataFrame(results)
                        cost_df = df[[4, 6, 17]]
                        cost_df.columns = ['Country', 'Technology', 'TotVarCost']

                        # replace wrong technology names
                        cost_df.loc[cost_df['Technology'] == 'BattTSO', 'Technology'] = 'Battery-TSO'
                        cost_df.loc[cost_df['Technology'] == 'BattDSO', 'Technology'] = 'Battery-DSO'

                        # load DistIv data into df
                        df_distiv = pd.DataFrame(results2)
                        pvcost_df = df_distiv[[1, 34]]
                        pvcost_df.columns = ['GenName', 'VOM_Cost']

                        # create profit data
                        gen_annual_h_copy = gen_annual_h.copy()
                        gen_annual_h_copy.drop(columns=['Hour'] + tech_ignored, inplace=True)
                        gen_annual_h_copy.index.rename('Hour', inplace=True)
                        # TODO
                        totvarcost_h = gen_annual_h_copy.copy()
                        cost_not_available = []
                        for technology in gen_annual_h_copy.columns:
                            # 0 VOM cost for load of storage technologies
                            cost = 0
                            if 'Pump' in technology and 'Load' not in technology:
                                cost_name = 'Pump'
                            else:
                                cost_name = technology

                            if year != 2020 and country == 'CH' and technology == 'PV-roof':
                                cost = pvcost_df[pvcost_df['GenName'] == 'PV 10-30 kW']
                                cost = cost.loc[:, 'VOM_Cost'].mean(numeric_only=False) * 10  # conversion from cents / kWh -> € / MWh
                            elif cost_name in cost_df['Technology'].unique():
                                cost = cost_df[
                                    (cost_df['Country'] == country) & (cost_df['Technology'] == cost_name)]
                                cost = cost.loc[:, 'TotVarCost'].mean(numeric_only=False)

                            if cost_name not in cost_df['Technology'].unique() or cost == np.nan:
                                if gen_annual_h_copy[technology].sum != 0:
                                    # for debugging
                                    cost_not_available.append(technology)

                            # hourly cost
                            totvarcost_h[technology] = gen_annual_h_copy[technology].apply(lambda x: x * cost)

                        profit_h = revenue_h.copy()
                        for technology in revenue_h.columns:
                            if ' (Load)' not in technology:
                                # subtract operating cost from revenue
                                profit_h[technology] = revenue_h[technology] - totvarcost_h[technology]
                            else:
                                profit_h[technology] = revenue_h[technology]

                        profit_h.fillna(0, inplace=True)
                        group_n_rename(profit_h,index_name="Hour").to_csv(
                            os.path.join(
                                output_directory,
                                f'generation_profit_hourly_{model}_{country.lower()}_{year}.csv'
                            )
                        )

                        # monthly profit
                        profit_m = profit_h.copy()
                        profit_m['date'] = pd.date_range(start='1/1/2018', periods=len(profit_m), freq='H')

                        profit_m = profit_m.resample('M', on='date').sum()

                        profit_m = profit_m.reset_index()
                        profit_m["Month"] = range(1, 1 + len(profit_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
                        profit_m = profit_m.drop(["date"], axis=1)

                        profit_m = profit_m.set_index("Month")
                        # write to csv file
                        group_n_rename(profit_m,index_name="Month").to_csv(
                            os.path.join(
                                output_directory,
                                f'generation_profit_monthly_{model}_{country.lower()}_{year}.csv'
                            )
                        )

                        # annual revenue
                        profit_a = profit_h.sum(axis=0)
                        profit_a.index.rename('Row', inplace=True)
                        profit_a.columns = [year]

                        # combine all annual columns to a dataframe
                        profit_all_years = pd.concat([profit_all_years, profit_a], axis=1)

                        # PROFIT PER GEN
                        profit_per_gen_a = pd.DataFrame([])
                        profit_a_grouped = group_n_rename(profit_a.to_frame(), transposed=True).transpose()

                        # sum profit of storage technologies
                        profit_a_grouped['Pump'] = profit_a_grouped['Pump'] + profit_a_grouped[
                            'Pump (Load)']
                        profit_a_grouped['Battery'] = profit_a_grouped['Battery'] + profit_a_grouped[
                            'Battery (Load)']
                        profit_a_grouped.drop(['Pump (Load)', 'Battery (Load)'], axis=1, inplace=True)

                        for tech in profit_a_grouped.columns:
                            if ' (Gen)' in tech:
                                tech_disp = tech.replace(' (Gen)', '')
                            else:
                                tech_disp = tech
                            profit_per_gen_a[tech_disp] = np.divide(profit_a_grouped[tech], gen_a_grouped[tech],
                                                                    out=np.zeros_like(profit_a_grouped[tech]),
                                                                    where=gen_a_grouped[tech] != 0)

                        profit_per_gen_all_years = pd.concat([profit_per_gen_all_years, profit_per_gen_a], axis=0)

                        # PROFIT PER CAP
                        profit_per_cap_a = pd.DataFrame([])

                        for tech in profit_a_grouped.columns:
                            if ' (Gen)' in tech:
                                tech_cap = tech.replace(' (Gen)', '')
                            else:
                                tech_cap = tech
                            if tech_cap in capacity_grouped.index:
                                profit_per_cap_a[tech_cap] = np.divide(profit_a_grouped[tech],
                                                                       capacity_grouped.loc[tech_cap, year],
                                                                       out=np.zeros_like(profit_a_grouped[tech]),
                                                                       where=capacity_grouped.loc[tech_cap, year] != 0)
                            else:
                                profit_per_cap_a[tech] = 0

                        profit_per_cap_all_years = pd.concat([profit_per_cap_all_years, profit_per_cap_a], axis=0)

            # write annual revenue
            revenue_all_years.columns = years_list
            group_n_rename(
                revenue_all_years,
                transposed=True,
                index_name='Row'
            ).to_csv(
                os.path.join(
                    output_directory,
                    f'generation_revenue_annual_{model}_{country.lower()}.csv'
                )
            )

            # write annual revenue per gen
            revenue_per_gen_all_years = revenue_per_gen_all_years.T
            revenue_per_gen_all_years.index.rename('Row', inplace=True)
            revenue_per_gen_all_years.columns = years_list
            revenue_per_gen_all_years.to_csv(
                os.path.join(
                    output_directory,
                    f'generation_revenuepergen_annual_{model}_{country.lower()}.csv'
                )
            )

            # write annual revenue per cap
            revenue_per_cap_all_years = revenue_per_cap_all_years.T
            revenue_per_cap_all_years.index.rename('Row', inplace=True)
            revenue_per_cap_all_years.columns = years_list
            revenue_per_cap_all_years.to_csv(
                os.path.join(
                    output_directory,
                    f'generation_revenuepercap_annual_{model}_{country.lower()}.csv'
                )
            )

            # write annual profit
            profit_all_years.columns = years_list
            if country == 'CH' and model == 'e':
                profit_all_years.to_csv('profit_ungrouped.csv')
            group_n_rename(
                profit_all_years,
                transposed=True,
                index_name='Row'
            ).to_csv(
                os.path.join(
                    output_directory,
                    f'generation_profit_annual_{model}_{country.lower()}.csv'
                )
            )

            # write annual profit per gen
            # add wholesale electricity price as reference
            profit_per_gen_all_years['Reference'] = elec_prices[country_names[country]].values.tolist()
            profit_per_gen_all_years = profit_per_gen_all_years.T
            profit_per_gen_all_years.index.rename('Row', inplace=True)
            profit_per_gen_all_years.columns = years_list
            profit_per_gen_all_years.to_csv(
                os.path.join(
                    output_directory,
                    f'generation_profitpergen_annual_{model}_{country.lower()}.csv'
                )
            )

            # write annual profit per cap
            # add wholesale electricity price as reference
            profit_per_cap_all_years['Reference'] = elec_prices[country_names[country]].values.tolist()
            profit_per_cap_all_years = profit_per_cap_all_years.T
            profit_per_cap_all_years.index.rename('Row', inplace=True)
            profit_per_cap_all_years.columns = years_list
            profit_per_cap_all_years.to_csv(
                os.path.join(
                    output_directory,
                    f'generation_profitpercap_annual_{model}_{country.lower()}.csv'
                )
            )

    return

def main(simulation: str, database: str, host: str, user: str, password: str):
    os.path.abspath(os.curdir)
    simulation_postprocess_path = os.getcwd()

    # get CentIV years
    centIv_listyears = get_years_simulated_by_centiv(Path())

    # active models
    # CentIV: c
    # FlexEco: e
    models = ['c']

    # check if FlexEco results exist:
    for i in centIv_listyears:
        if 'e' not in models:
            flexeco_file = os.path.join(
                simulation_postprocess_path,
                f"Gen_CH_{i}_e.csv"
            )
            if os.path.isfile(flexeco_file):
                models.append('e')

    # compute generation revenues by type for each model
    # loop over active models
    for model in models:
        create_revenue(
            centIv_listyears,
            model,
            simulation_postprocess_path,
            database,
            host=host,
            user=user,
            password=password
        )

if __name__ == "__main__":
    config_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.toml')
    settings = config.load(config.TomlFile(config_file_path))
    argp = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    argp.add_argument(
        "--simuname",
        type=str,
        help="Name of MySQL database results",
        default='evflex_ntc70_33pvt_ct_te',
    )
    argp.add_argument(
        "--DBname",
        type=str,
        help="Name of the input MySQL database",
        default='evflex_ntc70_33pvt_ct_te',
    )
    args = argp.parse_args()
    main(
        simulation=args.simuname,
        database=args.DBname,
        host=settings.input_database_server.host,
        user=settings.input_database_server.user,
        password=settings.input_database_server.password
    )
