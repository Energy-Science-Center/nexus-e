import argparse
from dataclasses import dataclass
import numpy as np
import os
import pandas as pd
import pymysql

from .Generation import get_folders_with_prefix

from nexus_e import config

year_dic = {
    2018: 1,
    2020: 2,
    2030: 3,
    2040: 4,
    2050: 5
}

@dataclass
class LoginCredentials:
    host: str
    user: str
    password: str


class DataframeManager:
    def __init__(self, output_directory: str, filename: str, model: str):
        self.result_dataframes = {}

        self.output_directory = output_directory
        self.filename = filename
        self.model = model

    def append_results_of_key_from_year(self, year: int, key: str, results: pd.DataFrame):
        if key not in self.result_dataframes:
            self.result_dataframes[key] = {year: results}
        else:
            self.result_dataframes[key][year] = results

    def export_results_in_all_resolutions(self):
        for key in self.result_dataframes:

            for year in self.result_dataframes[key]:
                result = self.result_dataframes[key][year]
                self.__export_hourly_resolution(result, f'{self.filename}_hourly_{self.model}_{key.lower()}_{year}.csv')
                self.__export_monthly_resolution(result, f'{self.filename}_monthly_{self.model}_{key.lower()}_{year}.csv')

            self.__export_annual_resolution(key, f'{self.filename}_annual_{self.model}_{key.lower()}.csv')

    def __export_hourly_resolution(self, results, filename_hourly):
        df = results.copy()
        df.index.name = 'Hour'
        df.to_csv(os.path.join(self.output_directory, filename_hourly))

    def __export_monthly_resolution(self, results, filename_monthly):
        df_m = results.copy()
        df_m['date'] = pd.date_range(start='1/1/2018', periods=len(df_m), freq='H')
        df_m = df_m.resample('M', on='date').sum(numeric_only=False)
        df_m = df_m.reset_index()

        df_m["Month"] = range(1, 1 + len(df_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
        df_m = df_m.drop(["date"], axis=1)
        df_m = df_m.set_index("Month")
        df_m.to_csv(os.path.join(self.output_directory, filename_monthly))

    def __export_annual_resolution(self, key, filename_annual):
        GWh_to_TWh = 1000
        annual_df = None

        for year in self.result_dataframes[key]:
            result = self.result_dataframes[key][year]
            df_a = result.sum(axis=0, numeric_only=False)
            df_a.name = year
            df_a = df_a / GWh_to_TWh

            if annual_df is None:
                annual_df = df_a
            else:
                annual_df = pd.concat([annual_df, df_a], axis=1)

        annual_df.index.name = 'Row'
        annual_df.to_csv(os.path.join(self.output_directory, filename_annual))

class BorderFlows:
    def __init__(
        self,
        database: str,
        simulation: str,
        parent_directory: str,
        database_credentials: LoginCredentials
    ):
        self.__database = database
        self.__simulation = simulation
        self.__parent_directory = parent_directory
        self.__database_credentials = database_credentials

    def load(self, year: int):
        self.__year = year
        self.__get_linenames_from_DB()
        self.__read_border_flows_and_set_columns()

    def __get_linenames_from_DB(self):
        conn = pymysql.connect(
            host=self.__database_credentials.host,
            database=self.__database,
            user=self.__database_credentials.user,
            password=self.__database_credentials.password
        )
        cursor = conn.cursor()
        cursor.execute(f'call {self.__database}.getBranchData({year_dic[self.__year]})')
        results = cursor.fetchall()

        self.branch_data = pd.DataFrame(results)
        self.branch_data.columns = [desc[0] for desc in cursor.description]

    def __read_border_flows_and_set_columns(self):
        CentIvDirectory = f"{self.__parent_directory}/../../Results/{self.__simulation}/CentIv_{self.__year}"

        file_path = os.path.join(CentIvDirectory, 'BranchFlows_hourly_ALL_LP.xlsx')

        border_flows = pd.read_excel(file_path, index_col=0)

        # Remove Metadata
        border_flows = border_flows.iloc[1:]

        # Set line names as column names
        border_flows.columns = border_flows.iloc[0]
        self.border_flows = border_flows.iloc[1:]

    def get_annual_flow_from_to_country(self, from_country: str, to_country: str):
        kWh_to_GWh = 1000000

        # CH-to-neighbor lines are defined as neighbor-to-neighbor lines in the database
        if from_country == 'CH':
            from_country = to_country
        elif to_country == 'CH':
            to_country = from_country

        power_lines = self.branch_data[(self.branch_data['FromCountry']==from_country) & (self.branch_data['ToCountry']==to_country)]

        # depending on the definition of the power flow
        if not power_lines.empty:
            line_name = power_lines['LineName'].values[0]

            exports = self.border_flows[self.border_flows[line_name] > 0][line_name].sum() / kWh_to_GWh
            imports = self.border_flows[self.border_flows[line_name] < 0][line_name].sum() / kWh_to_GWh
        else:
            line_name = self.branch_data[
                (self.branch_data['FromCountry'] == to_country) & (self.branch_data['ToCountry'] == from_country)][
                'LineName'].values[0]

            exports = self.border_flows[self.border_flows[line_name] < 0][line_name].sum() / kWh_to_GWh
            imports = self.border_flows[self.border_flows[line_name] > 0][line_name].sum() / kWh_to_GWh

        return abs(exports), abs(imports)
    
    def get_hourly_flow_from_to_country(self, from_country: str, to_country: str):

        if from_country == 'CH':
            from_country = to_country
        elif to_country == 'CH':
            to_country = from_country

        power_lines = self.branch_data[(self.branch_data['FromCountry']==from_country) & (self.branch_data['ToCountry']==to_country)]

        # depending on the definition of the power flow
        if len(power_lines.index) > 0:
            line_name = power_lines['LineName'].values[0]

            exports = self.border_flows[line_name].copy()
            exports[exports < 0] = 0

            imports = self.border_flows[line_name].copy()
            imports[imports > 0] = 0

        else:
            line_name = self.branch_data[
                (self.branch_data['FromCountry'] == to_country) & (self.branch_data['ToCountry'] == from_country)][
                'LineName'].values[0]

            exports = self.border_flows[line_name].copy()
            exports[exports > 0] = 0

            imports = self.border_flows[line_name].copy()
            imports[imports < 0] = 0
        return abs(exports) * -1, abs(imports)


class CrossCountryFlows:
    def __init__(self, simulated_year: list[int], dataframe_manager: DataframeManager):
        self.__dataframe_manager = dataframe_manager
        self.__simulated_years = simulated_year
        self.__country_neighbors = {
            "CH": ["DE", "FR", "IT", "AT"],
            "DE": ["CH", "FR", "AT"],
            "FR": ["CH", "DE", "IT"],
            "IT": ["CH", "FR", "AT"],
            "AT": ["CH", "DE", "IT"],
        }

    def load_flows_for_all_years(self, border_flows: BorderFlows):
        for year in self.__simulated_years:
            border_flows.load(year)
            for country in ["CH", "DE", "FR", "IT", "AT"]:
                country_results_hourly = pd.DataFrame()
                for neighbor in self.__country_neighbors[country]:
                    exports, imports = border_flows.get_hourly_flow_from_to_country(
                        country, neighbor
                    )
                    country_results_hourly[neighbor + " Exports"] = exports
                    country_results_hourly[neighbor + " Imports"] = imports
                MWh_to_GWh = 1000
                country_results_hourly = country_results_hourly / MWh_to_GWh
                country_results_hourly = self.__add_net_balance_to_crossborderflow(
                    country_results_hourly
                )
                country_results_hourly = self.__add_transit_to_crossborderflow(
                    country_results_hourly
                )
                
                self.__dataframe_manager.append_results_of_key_from_year(
                    year, country, country_results_hourly
                )

    def __add_net_balance_to_crossborderflow(
        self, crossborderflow_df: pd.DataFrame, transposed=False
    ):
        if transposed:
            crossborderflow_df.loc["Imports (Net)"] = crossborderflow_df.sum()
        else:
            crossborderflow_df["Imports (Net)"] = crossborderflow_df.sum(axis=1)

        return crossborderflow_df

    def __calculate_transit_and_non_transit_parts(self, crossborderflow_df: pd.DataFrame, flow_type: str, total_flow: np.ndarray):
        # Calculation of the transit and the nontransit part of the imports and the exports
        # The smaller part of import and export in each our is the transit part of the other 
        # and is distributed proportionally to each country according to the imports or exports

        if flow_type not in ['Imports', 'Exports']:
            raise ValueError("flow_type must be either 'Imports' or 'Exports'")
        if flow_type == 'Exports':
            flow_type = 'Imports'
        elif flow_type == 'Imports':
            flow_type = 'Exports'

        flow_columns = [col for col in crossborderflow_df.columns if flow_type in col and col != f'{flow_type} (Net)']
        flow_portions = np.zeros_like(crossborderflow_df[flow_columns].values)
        non_zero_total_flow = total_flow != 0
        if non_zero_total_flow.any():
            flow_portions[non_zero_total_flow] = np.abs(crossborderflow_df[flow_columns].values[non_zero_total_flow] / total_flow[non_zero_total_flow][:, None])

        new_columns = pd.DataFrame(index=crossborderflow_df.index)

        for countries in flow_columns:
            new_columns[f"{countries}_Transit"] = 0
            new_columns[f"{countries}_Non_Transit"] = 0
            col_idx = flow_columns.index(countries)
            transit_col = f"{countries}_Transit"
            non_transit_col = f"{countries}_Non_Transit"
            if flow_type == 'Exports':
                new_columns[transit_col] = flow_portions[:, col_idx] * np.abs(crossborderflow_df['Transit_Flow'].values)
                new_columns[non_transit_col] = (np.abs(crossborderflow_df[countries].values) - new_columns[transit_col].values)
                new_columns[transit_col] = new_columns[transit_col]*-1
                new_columns[non_transit_col] = new_columns[non_transit_col] * -1

            else:
                new_columns[transit_col] = flow_portions[:, col_idx] * np.abs(crossborderflow_df['Transit_Flow'].values)
                new_columns[non_transit_col] = np.abs(crossborderflow_df[countries].values) - new_columns[transit_col].values

        new_columns.index = new_columns.index.astype(crossborderflow_df.index.dtype)
        crossborderflow_df = pd.concat([crossborderflow_df, new_columns], axis=1)

        return crossborderflow_df



    def __add_transit_to_crossborderflow(self, crossborderflow_df: pd.DataFrame, transposed=False):
        if transposed:
            data_to_sum = crossborderflow_df.iloc[:-1, :]
        else:
            data_to_sum = crossborderflow_df.iloc[:, :-1]

        crossborderflow_df['Transit_Flow'] = 0

        Imports_Total = data_to_sum.where(data_to_sum < 0).sum(axis=1)
        Exports_Total = data_to_sum.where(data_to_sum > 0).sum(axis=1)

        larger_imports_mask = abs(Imports_Total) > abs(Exports_Total)
        crossborderflow_df['Transit_Flow'] = np.where(larger_imports_mask, abs(Exports_Total), abs(Imports_Total))

        crossborderflow_df = self.__calculate_transit_and_non_transit_parts(crossborderflow_df, 'Exports',
                                                                            Exports_Total.values)
        crossborderflow_df = self.__calculate_transit_and_non_transit_parts(crossborderflow_df, 'Imports',
                                                                            Imports_Total.values)

        return crossborderflow_df

    def save_flows_to_files(self):
        self.__dataframe_manager.export_results_in_all_resolutions()



def main(simulation: str, database: str, host: str, user: str, password: str):
    output_directory = os.path.join(
        os.getcwd(), f"Outputs/{simulation}/national_generation_and_capacity"
    )
    # CentIV only
    model = "c"
    dataframe_manager = DataframeManager(output_directory, "cross_country_flows", model)
    simulated_years = get_folders_with_prefix(
        f"{os.getcwd()}/../../Results/{simulation}", "CentIv"
    )
    cross_country_flows = CrossCountryFlows(simulated_years, dataframe_manager)
    border_flows = BorderFlows(
        database=database,
        simulation=simulation,
        parent_directory=os.getcwd(),
        database_credentials=LoginCredentials(
            host=host,
            user=user,
            password=password
        )
    )
    cross_country_flows.load_flows_for_all_years(border_flows=border_flows)
    cross_country_flows.save_flows_to_files()


if __name__ == "__main__":
    config_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.toml')
    settings = config.load(config.TomlFile(config_file_path))
    argp = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
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
