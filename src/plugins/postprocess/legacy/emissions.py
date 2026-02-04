import os
from pathlib import Path

import pandas as pd
import pymysql

from .Generation import group_n_rename, read_generator_file
from ..results_context import get_years_simulated_by_centiv

def return_GeneratorData_from_InputDB(
    year,
    database: str,
    host: str,
    user: str,
    password: str
):
    # call MySQL procedure to get relevant generator data
    conn = pymysql.connect(host=host, database=database, user=user, password=password)
    cursor = conn.cursor()

    sql_query = f"SELECT gendata.GenName, gendata.Technology, gendata.CO2Rate, gendata.StartYr, gendata.EndYr FROM " \
                f"gendata WHERE StartYr <= {year} AND EndYr >= {year}"
    cursor.execute(sql_query)
    results = cursor.fetchall()

    df = pd.DataFrame(results)
    df.columns = [desc[0] for desc in cursor.description]
    conn.close()
    return df


def filter_df_by_country(df, country):
    df = df.transpose()
    df = df[df[0] == country]
    df.drop(0, axis=1, inplace=True)
    return df.transpose()


def group_by_month(dataframe: pd.DataFrame):
    # group an hourly dataframe by months
    df_m = dataframe.copy()
    df_m['date'] = pd.date_range(start='1/1/2018', periods=len(df_m), freq='H')
    df_m = df_m.resample('M', on='date').sum()
    df_m = df_m.reset_index()
    df_m["Month"] = range(1, 1 + len(df_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    df_m = df_m.drop(["date"], axis=1)
    df_m = df_m.set_index("Month")
    return df_m


class GenerationPerUnit:
    def __init__(self, year: int, simulation: str):
        self.__original_generation_per_unit = pd.read_excel(
            os.path.join(
                f"CentIv_{year}",
                "GenerationConsumptionPerGenGenNames_hourly_ALL_LP.xlsx"
            )
        )
        self.__generation_per_unit = self.__original_generation_per_unit.copy()

    def reset(self):
        self.__generation_per_unit = self.__original_generation_per_unit.copy()
        return self

    def where_country(self, country: str):
        self.__generation_per_unit = filter_df_by_country(
            self.__generation_per_unit,
            country
        )
        return self
    
    def get_table(self):
        return self.__generation_per_unit

    def get_formatted_table(self) -> pd.DataFrame:
        # remove 0 columns
        generation_per_unit = self.__generation_per_unit.copy()
        generation_per_unit = generation_per_unit.loc[:, ~generation_per_unit.columns.isin(
            generation_per_unit.columns[(generation_per_unit == 0).all()])]

        generation_per_unit.columns = generation_per_unit.iloc[1]
        generation_per_unit = generation_per_unit.iloc[2:]
        generation_per_unit.reset_index(drop=True, inplace=True)
        generation_per_unit.index.name = 'Row'

        # sum up all units with the same name
        generation_per_unit = generation_per_unit.groupby(level=0, axis=1).sum()

        return generation_per_unit
    
    def get_technology_of_unit(self, unit: str):
        transposed_table = self.__generation_per_unit.transpose()
        return transposed_table[transposed_table[2] == unit][1].values[0]


class EmissionCreator:
    # this class calculates the CO2 emissions for all technologies
    # for all years, for all countries
    def __init__(
        self,
        simulation: str,
        database: str,
        host: str,
        user: str,
        password: str
    ):
        self.simulation = simulation
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.generator_data = None

        # read generator file
        self.__read_generator_list()

        # get all CentIv years
        self.centiv_years = get_years_simulated_by_centiv(Path())

        # create a dataframe for each country to store the emissions of each year
        self.annual_emission_countries = {}

    def __read_generator_list(self):
        self.generator_list = read_generator_file()

    def export_emissions(self):

        # iterate over all simulated years
        for year in self.centiv_years:

            generation_reader = GenerationPerUnit(
                year=year,
                simulation=self.simulation
            )
            # get CO2 rate form input DB
            self.generator_data = return_GeneratorData_from_InputDB(
                year,
                database=self.database,
                host=self.host,
                user=self.user,
                password=self.password
            )

            # neighbour countries
            for country in ['DE', 'FR', 'IT', 'AT']:
                emissions = self.__return_emissions_per_unit(
                    generation_reader.reset().where_country(country)
                )

                self.__export_monthly_and_hourly_emissions(emissions, year, country)

            # CH
            emissions_CH = self.__return_emissions_per_unit(
                generation_reader.reset().where_country('CH')
            )

            self.__export_monthly_and_hourly_emissions(emissions_CH, year, 'CH')

        self.__export_annual_emissions()

    def __return_emissions_per_unit(self, generation_reader: GenerationPerUnit):
        generation_per_unit = generation_reader.get_formatted_table()
        # create an empty df
        emissions_hourly = pd.DataFrame(index=generation_per_unit.index)

        # iterate over all units
        for unit in generation_per_unit.columns:
            # Uncoment to use for debugging
            #print(unit)
            co2_rate = self.__get_CO2_rate(unit, indicator='GenName')
            # Uncoment to use for debugging
            #print(co2_rate)
            technology = generation_reader.get_technology_of_unit(unit)
            # Uncoment to use for debugging
            #print(technology)
            # add emissions of the unit to technology
            if technology in self.generator_list[self.generator_list['GeneratorType'] == 'Consumption'].index:
                # consumption technologies
                if technology + ' (Load)' not in emissions_hourly.columns:
                    emissions_hourly[technology + ' (Load)'] = abs(generation_per_unit[unit]) * co2_rate
                else:
                    emissions_hourly[technology + ' (Load)'] += abs(generation_per_unit[unit]) * co2_rate

            elif technology in self.generator_list[(self.generator_list['GeneratorType'] == 'Storage') & (self.generator_list['OutputType'] == 1)].index:
                # only consider production of storage technologies
                if technology not in emissions_hourly.columns:
                    emissions_hourly[technology] = generation_per_unit[unit].apply(lambda x: 0 if x < 0 else x) * co2_rate
                else:
                    emissions_hourly[technology] += generation_per_unit[unit].apply(lambda x: 0 if x < 0 else x) * co2_rate

            else:
                if technology not in emissions_hourly.columns:
                    emissions_hourly[technology] = generation_per_unit[unit] * co2_rate
                else:
                    emissions_hourly[technology] += generation_per_unit[unit] * co2_rate

        emissions_hourly = emissions_hourly.astype(float)

        return emissions_hourly

    def __get_CO2_rate(self, technology:str, indicator='Technology'):

        with_technology = (self.generator_data[indicator] == technology)
        # check if technology daa exists
        if len(self.generator_data[with_technology]) == 0:
            print(f'CO2 Rate for {technology} not found in DB!')
            return 0
        else:
            # Uncoment to use for debugging
            #print(self.generator_data[is_in_country_and_has_technology])
            return self.generator_data[with_technology]['CO2Rate'].mean()

    def __export_monthly_and_hourly_emissions(self, emissions, year, country):
        self.__export_hourly_file(emissions, year, country)

        self.__export_monthly_file(emissions, year, country)

        # write annual values to dataframe
        if country not in self.annual_emission_countries:
            self.annual_emission_countries[country] = pd.DataFrame(emissions.sum())
        else:
            self.annual_emission_countries[country] = pd.concat(
                [self.annual_emission_countries[country], emissions.sum()], axis=1)

    def __export_hourly_file(self, emissions: pd.DataFrame, year: int, country: str):
        self.__export_file(group_n_rename(emissions, index_name='Hour'),
                           f'emissions_hourly_c_{country.lower()}_{year}.csv')

    def __export_monthly_file(self, emissions: pd.DataFrame, year: int, country: str):
        self.__export_file(group_n_rename(group_by_month(emissions), index_name='Month'),
                           f'emissions_monthly_c_{country.lower()}_{year}.csv')

    def __export_annual_emissions(self):
        for country in self.annual_emission_countries:
            # reset column names
            self.annual_emission_countries[country].columns = self.centiv_years
            self.annual_emission_countries[country].fillna(0, inplace=True)
            self.__export_file(group_n_rename(self.annual_emission_countries[country], transposed=True, index_name='Row'), f'emissions_annual_c_{country.lower()}.csv')

    def __export_file(self, df, filename):
        # write dataframe to csv file
        output_path = os.path.join(
            "postprocess",
            "national_generation_and_capacity"
        )
        df.to_csv(os.path.join(output_path, filename))

def main(
    simulation: str,
    database: str,
    host: str,
    user: str,
    password: str,
):
    emissions_creator = EmissionCreator(
        simulation=simulation,
        database=database,
        host=host,
        user=user,
        password=password
    )
    emissions_creator.export_emissions()