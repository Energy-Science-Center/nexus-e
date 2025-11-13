import os
from pathlib import Path
import pandas as pd
import numpy as np
import argparse
import pymysql
import h5py

from .Generation import group_n_rename, year_dic
from ..results_context import get_years_simulated_by_centiv

from nexus_e import config as config


def read_generator_file():
    """
    read generator file to list all relevant technologies
    """
   
    generator_list_file = "Generator_List.xlsx"
    generators = pd.read_excel(generator_list_file)
    # We drop these duplicated lines to avoid counting values twice
    if any(generators.duplicated(subset=["Technology", "PlotGroup", "OutputType"])):
        print(
            "Found duplicated combinations of Technology, PlotGroup, and "
            + f"OutputType in {generator_list_file}. Duplications are dropped."
        )
        duplicates_mask = generators.duplicated(
            subset=["Technology", "PlotGroup", "OutputType"],
            keep=False
        )
        print(
            generators[["Technology", "PlotGroup", "OutputType"]][duplicates_mask]
        )
        generators.drop_duplicates(
            subset=["Technology", "PlotGroup", "OutputType"],
            inplace=True
        )
    generators.set_index('Technology', inplace=True)
    return generators


def calculate_annuity(investment):
    # return annuity

    # lifetime of PV and battery systems
    system_lifetime = 30
    # rent
    r = 0.05

    annuity = (1 - 1 / (1 + r) ** system_lifetime) / r
    return investment / [annuity]


def imports_and_exports_calculation(dir_path, year, investment_interval, output_path):
    """
       This function calculates Switzerland's electricity import and export costs,
       including the revenues from exports, costs from imports flows and congestion rent, ignoring tranist flows
       related to cross-border electricity trading. It updates the class data storage
       (DfStorage) with the import/export balance over the given investment interval.
       """

    # Functions
    def get_import_costs(border_flows, elec_prices, national_generation_hourly_CH, neighbor_countries, GW_to_MW, year):
        """
        NetImport values are splitted by country using the original cross boarder flows, 
        and then multiplied by the electricity prices of each country to obtain the import costs.
        And ignoring transit flows
        """
        border_flows_values = border_flows.values
        elec_prices_neighbors = elec_prices[neighbor_countries]
        elec_prices_neighbors_values = elec_prices_neighbors.values
        # Identify hours with electricity imports to Switzerland (negative flows)
        mask_border_flows_negative = border_flows_values < 0
        # 1. Calculate import prices for hours when Switzerland is importing electricity
        import_prices = np.zeros_like(elec_prices_neighbors_values)  # Initialize with zeros
        import_prices[mask_border_flows_negative] = elec_prices_neighbors_values[
            mask_border_flows_negative]  # Apply neighbor prices during imports
        # 2. Calculate Switzerland's net exports (positive for export, negative for import)
        net_export_CH = -national_generation_hourly_CH[["Imports (Net)"]].sum(axis=1).values * GW_to_MW
        # Separate net exports and net imports
        net_import_CH_positive = np.clip(-net_export_CH, 0, None)  # Only positive values for imports
        # 3. Separate exports/imports to/from each neighboring country
        imports_to_CH = np.where(border_flows_values < 0, -border_flows_values, 0)
        # Compute the share of imports for each neighboring country relative to total flows
        denominator = imports_to_CH.sum(axis=1, keepdims=True)
        denominator[denominator == 0] = np.nan  # Replace zeros with NaN to avoid division by zero
        imports_to_CH_shares = imports_to_CH / denominator
        imports_to_CH_shares = np.nan_to_num(imports_to_CH_shares)
        # 4. Stack net imports for each country
        net_import_CH_positive_stacked = np.tile(net_import_CH_positive[:, np.newaxis], (1, 4))
        # Filter import shares for time instances with actual positive imports
        import_shares_filtered = np.where(net_import_CH_positive_stacked > 0, imports_to_CH_shares, 0)
        # 5. Calculate import costs
        import_costs = pd.DataFrame(import_shares_filtered * net_import_CH_positive_stacked
                                    * elec_prices_neighbors_values, columns=neighbor_countries)
        # 6. Save hourly import data and aggregate import costs
        border_flows_negative = pd.DataFrame(np.where(border_flows_values < 0, border_flows_values, 0),
                                             columns=neighbor_countries)
        DfStorage.df_imports[year] = border_flows_negative.sum(axis=1)  # Store hourly imports

        import_costs_total = import_costs.sum().sum()  # Total import cost
        return import_costs_total
    
    def get_export_revenues(border_flows, elec_prices, national_generation_hourly_CH, neighbor_countries, GW_to_MW, year):
        """
        NetExport values are splitted by country using the original cross boarder flows, 
        and then multiplied by the electricity prices of CH to obtain the export revenues.
        And ignoring transit flows
        """
        border_flows_values = border_flows.values
        elec_prices_neighbors = elec_prices[neighbor_countries]
        elec_prices_neighbors_values = elec_prices_neighbors.values
        # Identify hours with electricity imports to Switzerland (negative flows)
        mask_border_flows_negative = border_flows_values < 0
        # 2. Calculate Switzerland's net exports (positive for export, negative for import)
        net_export_CH = -national_generation_hourly_CH[["Imports (Net)"]].sum(axis=1).values * GW_to_MW
        # Separate positive net exports and negative net imports
        net_export_CH_positive = np.clip(net_export_CH, 0, None)  # Only positive values for exports
        # 3. Separate exports/imports to/from each neighboring country
        exports_from_CH = np.where(border_flows_values > 0, border_flows_values, 0)
        # Compute the share of exports for each neighboring country relative to total flows
        np.seterr(divide='ignore', invalid='ignore')
        exports_from_CH_shares = exports_from_CH / exports_from_CH.sum(axis=1, keepdims=True)
        # 4. Stack net exports for each country
        net_export_CH_positive_stacked = np.tile(net_export_CH_positive[:, np.newaxis], (1, 4))
        # Filter export shares for time instances with actual positive exports
        export_shares_filtered = np.where(net_export_CH_positive_stacked > 0, exports_from_CH_shares, 0)
        # 5. Calculate export revenues
        export_revenues = pd.DataFrame(export_shares_filtered * net_export_CH_positive_stacked
                                       * elec_prices_CH_per_country, columns=neighbor_countries)
        #6. Save hourly export data and aggregate export profits
        DfStorage.df_exports[year] = national_generation_hourly_CH['Exports'] * GW_to_MW

        export_profits_total = export_revenues.sum().sum()  # Total export profit
        return export_profits_total
    def get_congestion_rent(border_flows, elec_prices, neighbor_countries, elec_prices_CH_per_country):
        """"
        Congestion rent is the additional revenue earned in a system due to capacity constraints,
        that create price differences between locations or time periods.
        The congestion rent is calculated as price difference times boarderflow

        """
        border_flows_values = border_flows.values
        elec_prices_neighbors = elec_prices[neighbor_countries]
        elec_prices_neighbors_values = elec_prices_neighbors.values
        # 11. Calculate congestion rent (arbitrage potential between price differences across borders)
        congestion_rent = 0.5 * abs(
            border_flows_values * (elec_prices_neighbors_values - elec_prices_CH_per_country))
        congestion_rent_total = congestion_rent.sum()
        return congestion_rent_total

    # 0. define conversion coefficients
    GW_to_MW = 1000

    # 1. Read hourly cross-border electricity flows between Switzerland and neighbors
    border_flows = pd.read_excel(os.path.join(dir_path, 'CrossBorderBranchFlows_hourly_CH_LP.xlsx'),
                                 index_col=0)
    neighbor_countries = ['Germany', 'France', 'Italy', 'Austria']
    border_flows.columns = neighbor_countries

    # 2. Read hourly electricity prices for Switzerland and neighboring countries
    elec_prices = pd.read_csv(os.path.join(output_path, f'national_elecprice_hourly_c_{year}.csv'))
    elec_prices_CH = elec_prices['Switzerland'].values
    elec_prices_CH_per_country = np.tile(elec_prices_CH[:, np.newaxis],
                                         (1, 4))  # Replicate Swiss prices for each country

    # 3. Get hourly Swiss electricity generation and net imports (negative if net export)
    national_generation_hourly_CH = pd.read_csv(
        os.path.join(output_path, f'national_generation_hourly_gwh_c_ch_{year}.csv'))

    # 4. Calculate import costs
    import_costs_total = get_import_costs(border_flows, elec_prices, national_generation_hourly_CH, neighbor_countries, GW_to_MW, year)

    # 5. calculate export revenues
    export_profits_total = get_export_revenues(border_flows, elec_prices, national_generation_hourly_CH, neighbor_countries, GW_to_MW, year)

    # 6. calculate congestion rent
    congestion_rent_total = get_congestion_rent(border_flows, elec_prices, neighbor_countries, elec_prices_CH_per_country)

    # 12. Prepare a cashflow summary for the investment interval
    year_list = range(year - investment_interval + 1, year + 1)
    yearly_imports_exports_cashflow = pd.DataFrame(columns=year_list)

    # Store total import/export costs and transfer capacity payments over the interval
    yearly_imports_exports_cashflow.loc['Imports'] = [import_costs_total] * investment_interval
    yearly_imports_exports_cashflow.loc['Exports'] = [-export_profits_total] * investment_interval
    yearly_imports_exports_cashflow.loc['Transfer capacity payment'] = [-congestion_rent_total] * investment_interval

    # 13. Merge the yearly cashflow data into the final imports/exports balance dataframe
    DfStorage.df_imports_exports_balance = pd.concat(
        [DfStorage.df_imports_exports_balance, yearly_imports_exports_cashflow], axis=1)

    return


class DfStorage:
    # store all resuls here
    df_investment_costs = pd.DataFrame()
    df_fixedop_costs = pd.DataFrame()
    df_varop_costs = pd.DataFrame()
    df_costs_per_costtype = pd.DataFrame()
    df_costs__per_gentype = pd.DataFrame()
    df_discount_costs = pd.DataFrame()

    df_imports_exports_balance = pd.DataFrame()
    df_imports = pd.DataFrame()
    df_exports = pd.DataFrame()
    df_total_output = pd.DataFrame()


class AnnualDataCollectionAnnualized:
    def __init__(
        self,
        year,
        simulation: str,
        database: str,
        output_path: str,
        centiv_only: bool,
        host: str,
        user: str,
        password: str,
        previous_object=None,
    ):
        self.year = year
        self.simulation = simulation
        self.database = database
        self.centiv_only = centiv_only
        self.host = host
        self.user = user
        self.password = password

        # define previous_object for earlier investments
        if previous_object is not None:
            self.previous_year = previous_object.year
        else:
            self.previous_year = None

        # discounting is calculated every year
        self.investment_interval = 1

        # discount rates used to calculate the discounted system costs
        self.discount_rates = [0, 3, 5, 7]

        # reference year used to calculate the discounted costs
        self.reference_year_discounting = 2020

        # define path to the results directory of a specific year
        self.centiv_results_path = f"CentIv_{year}"

        # get new investments
        self.__get_costs_centIV()

        # calculate import costs and export revenue
        imports_and_exports_calculation(self.centiv_results_path, self.year, self.investment_interval, output_path)

    def __get_costs_centIV(self):
        """
        This method gets the three different cost types investments costs,
        variable costs, fixed costs for all technologies from CentIv results.
        """
       
        # get costs
        centiv_gencon_ch = pd.read_excel(
            os.path.join(
                self.centiv_results_path,
                "GenerationConsumption_total_CH_LP.xlsx"
            ),
            index_col="Technology"
        )

        year_list = [self.year]
        yearly_inv_cost = pd.DataFrame(columns=year_list)
        yearly_fix_cost = pd.DataFrame(columns=year_list)
        yearly_var_cost = pd.DataFrame(columns=year_list)

        gen_technologies = centiv_gencon_ch.index.tolist()

        if 'PV-roof' not in gen_technologies:
            gen_technologies.append('PV-roof')
        if 'Battery-TSO' not in gen_technologies:
            gen_technologies.append('Battery-TSO')

        # append
        for technology in gen_technologies:
            if technology in centiv_gencon_ch.index:

                yearly_inv_cost.loc[technology] = [centiv_gencon_ch.loc[
                                                       technology, 'Tot_InvCost_CHF']]
                yearly_fix_cost.loc[technology] = [centiv_gencon_ch.loc[
                                                       technology, 'Tot_FOpCost_CHF']]
                yearly_var_cost.loc[technology] = [centiv_gencon_ch.loc[
                                                       technology, 'Tot_OpCost_CHF']]
            else:
                yearly_inv_cost.loc[technology] = [0]
                yearly_fix_cost.loc[technology] = [0]
                yearly_var_cost.loc[technology] = [0]

        # add transmission costs to investments
        yearly_inv_cost.loc['Transmission'] = self.__transmission_costs_calculation()

        # call method to collect DistIv results
        distiv_pv, distiv_bat = self.__get_costs_distiv()

        # calculate annuity for residential PV and battery systems
        eac_pv = calculate_annuity(distiv_pv['yearly_inv_cost'][0])
        eac_bat = calculate_annuity(distiv_bat['yearly_inv_cost'][0])

        # add distiv costs to centiv costs
        yearly_inv_cost.loc['PV-roof'] = yearly_inv_cost.loc['PV-roof'] + eac_pv
        yearly_fix_cost.loc['PV-roof'] = yearly_fix_cost.loc['PV-roof'] + distiv_pv['yearly_fix_cost']
        yearly_var_cost.loc['PV-roof'] = yearly_var_cost.loc['PV-roof'] + distiv_pv['yearly_var_cost']

        yearly_inv_cost.loc['Battery-TSO'] = yearly_inv_cost.loc['Battery-TSO'] + eac_bat
        yearly_fix_cost.loc['Battery-TSO'] = yearly_fix_cost.loc['Battery-TSO'] + distiv_bat['yearly_fix_cost']
        yearly_var_cost.loc['Battery-TSO'] = yearly_var_cost.loc['Battery-TSO'] + distiv_bat['yearly_var_cost']

        # add previous investments to new investments
        DfStorage.df_investment_costs = pd.concat([DfStorage.df_investment_costs, yearly_inv_cost], axis=1)

        DfStorage.df_fixedop_costs = pd.concat([DfStorage.df_fixedop_costs, yearly_fix_cost], axis=1)
        DfStorage.df_varop_costs = pd.concat([DfStorage.df_varop_costs, yearly_var_cost], axis=1)
        return



    def __transmission_costs_calculation(self):
        new_lines = pd.read_excel(
            os.path.join(
                self.centiv_results_path,
                "NewLinesOnlyOneStatus.xlsx"
            )
        )
        new_lines = new_lines['LineName'].tolist()

        new_lines_costs = 0

        # get conversion efficiency for fuel cells
        connection = pymysql.connect(host=self.host, database=self.database, user=self.user, password=self.password)
        connection_cursor = connection.cursor()
        connection_cursor.execute(f'call {self.database}.getBranchData({year_dic[self.year]})')
        results = connection_cursor.fetchall()
        df = pd.DataFrame(results)

        df.columns = [desc[0] for desc in connection_cursor.description]
        df = df[['LineName',  'CandCost', 'length']]

        df.set_index('LineName', drop=True, inplace=True)

        for line_name in new_lines:
            updated_line_name = line_name.replace('_cand', '_Inv')

            # for debugging purpose
            if updated_line_name not in df.index:
                costs_per_km = df.at[line_name, 'CandCost']
                line_length = df.at[line_name, 'length']
            else:
                costs_per_km = df.at[updated_line_name, 'CandCost']
                line_length = df.at[updated_line_name, 'length']

            new_lines_costs += costs_per_km * line_length

        return [new_lines_costs]

    def __get_costs_distiv(self):
        """
        This method gets the three different cost types investments costs,
        variable costs, fixed costs for all technologies from DistIv results.
        """
        # for 2020 and centIV only: all data is 0
        # Define the data and row names
        data = {'PV-roof': [0, 0, 0],
                'Solar Battery': [0, 0, 0]}
        row_names = ['Investment Costs', 'VarOp Costs', 'FixedOp Costs']

        # Create the DataFrame with row names
        result_df = pd.DataFrame(data, index=row_names)

        # overwrite results_df
        if self.year != 2020 and not self.centiv_only:
            results_distiv = h5py.File(f"DistIv_{self.year}.mat")

            # hardcoded
            gen_names_distIv = ['PV 2-10 kW', 'PV 10-30 kW', 'PV 30-100 kW', 'PV >100 kW', 'Biomass wood',
                                'Biomass manure', 'Solar Battery', 'CHP gas', 'Grid Battery']
            df_results_distIv = pd.DataFrame(columns=gen_names_distIv)

            investment_cost = results_distiv['resDistIv']['cost']['invest']['Investcost_EUR']
            investment_cost = investment_cost[()]

            df_results_distIv.loc['Investment Costs'] = investment_cost[0]

            var_op_cost = results_distiv['resDistIv']['cost']['all']['VarOpcost_tot_EUR']
            var_op_cost = var_op_cost[()]
            df_results_distIv.loc['VarOp Costs'] = var_op_cost[0]

            fixed_op_cost = results_distiv['resDistIv']['cost']['all']['FixedOpcost_tot_EUR']
            fixed_op_cost = fixed_op_cost[()]
            df_results_distIv.loc['FixedOp Costs'] = fixed_op_cost[0]

            # Sum up the values in the first 4 columns (PV only)
            sum_of_first_4_columns = df_results_distIv.iloc[:, :4].sum(axis=1)

            # Create a new DataFrame with only the sum of the first 4 columns (drop all other columns)
            result_df = pd.DataFrame(sum_of_first_4_columns, columns=['PV-roof'])
            # add solar battery data
            result_df['Solar Battery'] = df_results_distIv['Solar Battery']

        # assign costs to series
        # PV
        pv_distiv = {
            'yearly_inv_cost': [result_df.loc[
                                    'Investment Costs', 'PV-roof'] / self.investment_interval] * self.investment_interval,

            'yearly_fix_cost': [result_df.loc['FixedOp Costs', 'PV-roof']] * self.investment_interval,

            'yearly_var_cost': [result_df.loc['VarOp Costs', 'PV-roof']] * self.investment_interval
        }
        # Battery
        bat_distiv = {
            'yearly_inv_cost': [result_df.loc[
                                    'Investment Costs', 'Solar Battery'] / self.investment_interval] * self.investment_interval,

            'yearly_fix_cost': [result_df.loc['FixedOp Costs', 'Solar Battery']] * self.investment_interval,

            'yearly_var_cost': [result_df.loc['VarOp Costs', 'Solar Battery']] * self.investment_interval
        }
        return pv_distiv, bat_distiv


def return_extend_dataframe(df, cumulative=False):
    # fill columns such that the dataframe is complete
    df.fillna(0, inplace=True)
    # if cumulative is True, add value of last simulated year on top
    df_out = pd.DataFrame()
    simulated_years = df.columns

    for i in range(len(simulated_years)):
        if simulated_years[i] == simulated_years[-1]:
            new_years = list(range(simulated_years[i], 2060))
        else:
            new_years = list(range(simulated_years[i], simulated_years[i + 1]))
        for year in new_years:
            if cumulative and i != 0:
                df_out[year] = df[simulated_years[i]] + df_out[simulated_years[i - 1]]
            else:
                df_out[year] = df[simulated_years[i]]

    df_out.fillna(0, inplace=True)
    return df_out


def discount_df(df, rate, ignore_columns=None):
    """
    Discount the cost based on the difference between year and the reference year (2020)
    """
    df_out = df.copy()

    for column in df_out.columns:
        if ignore_columns is not None:
            if column not in ignore_columns:
                timeperiod = max(0, column - 2020)
                df_out[column] = df[column] / (1 + rate / 100) ** timeperiod
        else:
            timeperiod = max(0, column - 2020)
            df_out[column] = df[column] / (1 + rate / 100) ** timeperiod
    return df_out


class OutputPreparerAnnualized:
    def __init__(self, output_path: str, simulation: str):
        """
        This class is used to create the output files
        """
        
        # output directory
        self.output_path = output_path
        # discount rates
        self.discount_rates = [0, 3, 5, 7]

        # simulated years
        self.simulated_years = list(DfStorage.df_investment_costs.columns)

        # get data
        # investment costs
        # induvidual annualized investment costs per technology
        self.df_investment_costs_ind = return_extend_dataframe(DfStorage.df_investment_costs)
        # combined annualized investment costs per technology
        self.df_investment_costs_com = return_extend_dataframe(DfStorage.df_investment_costs, cumulative=True)
        # total annualized investment cost per simulated year
        self.df_investment_costs_per_simyear = self.__return_investment_per_simyear(DfStorage.df_investment_costs)
        self.df_transmission_investment_costs_per_simyear = \
            self.__return_investment_per_simyear(DfStorage.df_investment_costs, transmission=True)

        # VarOP costs
        self.df_varop_costs = return_extend_dataframe(DfStorage.df_varop_costs)
        # FixedOP costs
        self.df_fixedop_costs = return_extend_dataframe(DfStorage.df_fixedop_costs)
        # Imports & Exports
        self.df_imports_exports_balance = return_extend_dataframe(DfStorage.df_imports_exports_balance)

        # create output files for each cost type per technology type
        self.__prepare_outputs(self.df_investment_costs_ind, 'investments_ind', add_columns_to_regroup=['Transmission'])
        self.__prepare_outputs(self.df_investment_costs_com, 'investments_com', add_columns_to_regroup=['Transmission'])
        self.__prepare_outputs(self.df_varop_costs, 'varop')
        self.__prepare_outputs(self.df_fixedop_costs, 'fixedop')
        self.__prepare_outputs(self.df_imports_exports_balance, 'trading', technologies=False)

        # create costs per cost type
        self.__per_costtype()

        # total costs per technology
        self.__costs_per_technology()

        # cost types for each technology
        self.__cost_types_per_technology()

    def __prepare_outputs(self, df, name, technologies=True, add_columns_to_regroup=None, accumulated=True, add_columns_to_accumulation=None):
        # prepare all output files

        # group and rename
        if technologies:
            df = group_n_rename(df, transposed=True, index_name='Row', add_columns=add_columns_to_regroup)
        else:
            df.index.name = 'Row'
        # calculate discounting
        for rate in self.discount_rates:
            df_discounted = discount_df(df, rate, ignore_columns=add_columns_to_accumulation)
            # annualized
            self.__accumulate_costs(df_discounted, add_columns=add_columns_to_accumulation)\
                .to_csv(os.path.join(self.output_path, f'systemcosts_{name}_{rate}_an.csv'))
            if accumulated:
                # accumulated
                self.__accumulate_costs(df_discounted, sum_type='combined', add_columns=add_columns_to_accumulation)\
                    .to_csv(os.path.join(self.output_path, f'systemcosts_{name}_{rate}_cum.csv'))

        return

    def __return_investment_per_simyear(self, df, transmission=False):
        """
        This method splits investments by their year of creation
        """
        df_out = pd.DataFrame(columns=range(2020, 2060))
        if df.empty:
            return df_out
        if transmission:
            df_copy = df.loc[['Transmission']].copy()
        else:
            df_copy = df.copy()
            if "Transmission" in df_copy.index:
                df_copy.drop('Transmission', inplace=True)
        for year in self.simulated_years:
            df_out.loc[year] = 0
            df_out.loc[year, year:2060] = df_copy[year].sum()
        return df_out

    def __accumulate_costs(self, df, sum_type='annualized', add_columns=None):
        # accumulate costs by simulated years or calculate total costs
        df_out = df.copy()
        if sum_type == 'annualized':
            if add_columns is not None:
                return df_out[add_columns + self.simulated_years]
            else:
                return df_out[self.simulated_years]

        elif sum_type == 'combined':
            df_out_2 = pd.DataFrame()
            if add_columns is not None:
                df_add_columns = df_out[add_columns]
                df_out.drop(add_columns, axis=1, inplace=True)

            df_out_2['Costs'] = df_out.sum(axis=1)
            if add_columns is not None:
                df_out_2 = pd.concat([df_out_2, df_add_columns], axis=1)
            return df_out_2

    def __per_costtype(self):
        # create a new dataframe
        df_per_costtype = pd.DataFrame(columns=self.df_investment_costs_ind.columns)
        print(self.df_investment_costs_ind.columns)

        # split investments by the year when the investment was done
        for year in self.simulated_years:
            df_per_costtype.loc[f'Investments {year}'] = self.df_investment_costs_per_simyear.loc[year]
            df_per_costtype.loc[f'Transmission {year}'] = self.df_transmission_investment_costs_per_simyear.loc[year]
        df_per_costtype.loc['Variable Operation'] = self.df_varop_costs.sum()
        df_per_costtype.loc['Fixed Operation'] = self.df_fixedop_costs.sum()
        df_per_costtype.loc['Trading'] = self.df_imports_exports_balance.loc[["Imports", "Exports"]].sum()

        # total costs per cost type
        self.__prepare_outputs(df_per_costtype, 'percosttype', technologies=False)

        return

    def __costs_per_technology(self):
        # sum up all costs for each technology
        df = self.df_investment_costs_com.add(self.df_fixedop_costs, axis='index', fill_value=0)\
            .add(self.df_varop_costs, axis='index', fill_value=0)

        # append imports and exports
        df = pd.concat([df, self.df_imports_exports_balance])

        # total cost per gentype
        self.__prepare_outputs(df, 'pergentype', add_columns_to_regroup=['Imports', 'Exports', 'Transmission'])
        return

    def __cost_types_per_technology(self):

        df_costtyper_for_technologies = pd.DataFrame()
        # investments
        for year in self.simulated_years:
            df_year = pd.DataFrame(columns=range(2020, 2060))
            for idx, row in DfStorage.df_investment_costs.iterrows():
                df_year.loc[idx] = 0
                df_year.loc[idx, year:2060] = row[year]
            df_year = group_n_rename(df_year, transposed=True)

            df_year['Cost Type'] = f'Investments {year}'
            df_year = df_year.reset_index(level=0).rename(columns={'index': 'Technology'})
            df_year.rename(columns={'PlotGroup': 'Technology'}, inplace=True)

            # concat all dataframes
            df_costtyper_for_technologies = pd.concat([df_costtyper_for_technologies, df_year])
        # Write to xlsx
       
        # VarOp 
        df_varop = self.df_varop_costs.copy()
        df_varop = group_n_rename(df_varop, transposed=True)

        df_varop['Cost Type'] = 'Variable Operation'
        df_varop = df_varop.reset_index(level=0).rename(columns={'index': 'Technology'})
        df_varop.rename(columns={'PlotGroup': 'Technology'}, inplace=True)


        df_costtyper_for_technologies = pd.concat([df_costtyper_for_technologies, df_varop])
       
        # Fixed Operation
        df_fixop = self.df_fixedop_costs.copy()
        df_fixop = group_n_rename(df_fixop, transposed=True)

        df_fixop['Cost Type'] = 'Fixed Operation'
        df_fixop = df_fixop.reset_index(level=0).rename(columns={'index': 'Technology'})
        df_fixop.rename(columns={'PlotGroup': 'Technology'}, inplace=True)

        df_costtyper_for_technologies = pd.concat([df_costtyper_for_technologies, df_fixop])


        self.__prepare_outputs(df_costtyper_for_technologies, 'costtypes_per_technologies', technologies=False, add_columns_to_accumulation=['Technology', 'Cost Type'])
        return

def main(simulation: str, database: str, host: str, user: str, password: str):
    parent_directory = os.getcwd()

    # output path
    output_path = os.path.join(
        "postprocess",
        "national_generation_and_capacity"
    )

    # search for DistIv results
    has_distiv_results = any(
        os.path.exists(f"DistIv_{year}.mat")
        for year in [2030, 2040, 2050]
    )
    
    centiv_years = get_years_simulated_by_centiv(Path())
    obj_list = []
    previous_object = None
    # append costs for every simulated year
    for year in centiv_years:
        annualized_cost = AnnualDataCollectionAnnualized(
            year=year,
            simulation=simulation,
            database=database,
            centiv_only= not has_distiv_results,
            output_path=output_path,
            host=host,
            user=user,
            password=password,
            previous_object=previous_object,
        )
        obj_list.append(annualized_cost)
        previous_object = obj_list[-1]

    OutputPreparerAnnualized(
        output_path=output_path,
        simulation=simulation
    )

if __name__ == '__main__':
    config_file_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "config.toml"
    )
    settings = config.load(config.TomlFile(config_file_path))
    argp = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    argp.add_argument("--simuname", type=str, help="Name of MySQL database results",
                    default='pathfndr_s8_241119_cpv_s8')
    argp.add_argument("--DBname", type=str, help="Name of MySQL database",
                    default='pathfndr_s8_241119_cpv_s8')  # nexuse_schema2_disagg_ch2040
    args = argp.parse_args()
    main(
        simulation=args.simuname,
        database=args.DBname,
        host=settings.input_database_server.host,
        user=settings.input_database_server.user,
        password=settings.input_database_server.password
    )
