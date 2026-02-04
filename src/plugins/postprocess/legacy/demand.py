import os
from pathlib import Path
import pandas as pd
import pymysql
from .Generation import year_dic, create_dir
from ..results_context import get_years_simulated_by_centiv

class DatabaseSchema:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str
    ):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database

    def __execute_query(self, query: str) -> pd.DataFrame:
        conn = pymysql.connect(
            host=self.__host,
            database=self.__database,
            user=self.__user,
            password=self.__password
        )
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        if results:
            df.columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return df

    def table_exists(self, table_name: str) -> bool:
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self.__execute_query(query)
        return len(result) > 0

    def get_load_profiles(self, year: int):
        query = f"SELECT * FROM load_profiles WHERE year = {year}"
        return self.__execute_query(query)

    def get_demand(self, year: int):
        query = f'call {self.__database}.getLoadData_v2({year_dic[year]})'
        return self.__execute_query(query)

    def get_profiles(self):
        query = "SELECT * FROM profiledata"
        return self.__execute_query(query)

def get_profiles(country, demand_data, profiles):
    hourly_demand_profiles = pd.DataFrame()
    for profile_type in ['idProfile', 'idProfile_eMobility', 'idProfile_eHeatPump', 'idProfile_eHydrogen']:
        country_demand = demand_data[demand_data['Country'] == country]
        profile_id = country_demand[profile_type].iloc[0]
        id_column = 0
        profile_with_metadata = profiles[profiles.iloc[:, id_column] == profile_id]
        data_index = 6
        profile = eval(profile_with_metadata.iloc[:, data_index].iloc[0])
        result = pd.DataFrame({profile_type: profile})
        hourly_demand_profiles = pd.concat([hourly_demand_profiles, result], axis=1)
    return hourly_demand_profiles

def get_profiles_flex(country, demand_data):
    hourly_demand_profiles = pd.DataFrame()
    for profile_type in ['Conventional', 'eMobility', 'HeatPump', 'Electrolysis']:
        country_demand = demand_data[demand_data['BusName'].str.startswith(country)]
        profile_data = country_demand[country_demand['LoadType'] == profile_type]
        profile_data = profile_data.copy()  # Ensure we are working with a copy
        profile_data.loc[:, 'timeSeries'] = profile_data['timeSeries'].apply(eval)
        summed_profile = profile_data['timeSeries'].apply(pd.Series).sum(axis=0).tolist()
        result = pd.DataFrame({profile_type: summed_profile})
        hourly_demand_profiles = pd.concat([hourly_demand_profiles, result], axis=1)
    hourly_demand_profiles.columns = ['idProfile', 'idProfile_eMobility', 'idProfile_eHeatPump', 'idProfile_eHydrogen']
    return hourly_demand_profiles

def main(database: str, host: str, user: str, password: str):
    database_schema = DatabaseSchema(
        host=host,
        user=user,
        password=password,
        database=database
    )

    centiv_years = get_years_simulated_by_centiv(Path())

    output_path = os.path.join(
        "postprocess",
        "national_generation_and_capacity"
    )
    create_dir(output_path)
    df_container = {}
    for year in centiv_years:
        if database_schema.table_exists('load_profiles'):
            demand_data = database_schema.get_load_profiles(year)
        else:
            demand_data = database_schema.get_demand(year)

        profiles = database_schema.get_profiles()

        for country in ['CH', 'DE', 'FR', 'IT', 'AT']:
            if database_schema.table_exists('load_profiles'):
                hourly_demand_profiles = get_profiles_flex(country, demand_data)
            else:
                hourly_demand_profiles = get_profiles(country, demand_data, profiles)

            country_lower = country.lower()
            hourly_demand_profiles.index.name = 'Hour'
            filename = f'demand_hourly_c_{country_lower}_{year}.csv'
            hourly_demand_profiles.to_csv(os.path.join(output_path, filename))

            hourly_demand_profile_time = pd.DataFrame({
                'DateTime': pd.date_range(start='2024-01-01', end='2024-12-31', freq='H'),
            })
            monthly_demand_profiles = pd.concat([hourly_demand_profile_time, hourly_demand_profiles], axis=1)
            monthly_demand_profiles.set_index('DateTime', inplace=True)
            monthly_demand_profiles = monthly_demand_profiles.astype(float)
            monthly_data_sum = monthly_demand_profiles.resample('M').sum()
            yearly_data_sum = monthly_data_sum.resample('Y').sum()
            yearly_data_sum.reset_index(inplace=True)
            yearly_data_sum.index.name = 'Row'
            monthly_data_sum.index = range(1, 1 + len(monthly_data_sum)) # to be able to run the model partially, e.g. for 168 hours, it is needed to be defined this flexibly
            filename = f'demand_monthly_c_{country_lower}_{year}.csv'
            monthly_data_sum.index.name = 'Month'
            monthly_data_sum.to_csv(os.path.join(output_path, filename))
            filename = f'demand_annual_c_{country_lower}.csv'
            df_name = f'demand_annual_c_{country_lower}_{year}'
            # Remove the first column that contains the timestamp
            yearly_data_sum_without_timestamp = yearly_data_sum.iloc[:, 1:]
            yearly_data_sum_without_timestamp = yearly_data_sum_without_timestamp.T
            yearly_data_sum_without_timestamp.columns = [year]
            df_container[df_name] = yearly_data_sum_without_timestamp

    for country in ['CH', 'DE', 'FR', 'IT', 'AT']:
        country_lower = country.lower()
        yearly_data = pd.DataFrame()
        for year in centiv_years:
            df_name = f'demand_annual_c_{country_lower}_{year}'
            desired_df = df_container[df_name]
            yearly_data = pd.concat([yearly_data, desired_df], axis=1)

        filename = f'demand_annual_c_{country_lower}.csv'
        yearly_data.index.name = 'Row'
        yearly_data.to_csv(os.path.join(output_path, filename))