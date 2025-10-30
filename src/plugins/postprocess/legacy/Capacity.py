import os
import pandas as pd
import numpy as np
import argparse
import pymysql

from .Generation import group_n_rename, get_data, get_folders_with_prefix, read_generator_file

import sys
from nexus_e import config

year_dic = {
    2018: 1,
    2020: 2,
    2030: 3,
    2040: 4,
    2050: 5
}
Cap_CH_centiv = pd.DataFrame()

class Capacity:
    def __init__(
        self,
        parent_directory,
        simu_name,
        centIv_listyears,
        pv_inv,
        bat_inv,
        generators,
        e_max,
        p_max,
        host,
        user,
        password
    ):
        self.parent_directory = parent_directory
        self.simu_name = simu_name
        self.centIv_listyears = centIv_listyears
        self.pv_inv = pv_inv
        self.bat_inv = bat_inv
        self.generators = generators
        self.e_max = e_max
        self.p_max = p_max
        self.host = host
        self.user = user
        self.password = password

    def get_cap(self,database):
        # get capacity for each technology
        # Capacity of Switzerland

        # store Swiss capacities
        df_cap_gen = pd.DataFrame()
        df_cap_con = pd.DataFrame()

        for y in self.centIv_listyears:
            centivpath = f"{self.parent_directory}/../../Results/{self.simu_name}/CentIv_{y}"

            fn7 = os.path.join(centivpath, "Generators_EM_agg.csv")
            cap_ch = pd.read_csv(fn7, index_col=0)

            # generation capacity
            cap_ch_gen = cap_ch.drop(["idGen", "Pmin", "TotVarCost"], axis=1)
            cap_ch_gen = cap_ch_gen.rename(columns={'Pmax': y})

            # consumption capacity
            cap_ch_con = cap_ch.drop(["idGen", "Pmax", "TotVarCost"], axis=1)
            cap_ch_con = cap_ch_con.rename(columns={'Pmin': y})

            # add P2G2P capacity if file exists:
            fn_cap_p2g2p = os.path.join(centivpath, "NewUnits_P2G2P.xlsx")
            if os.path.exists(fn_cap_p2g2p):
                # read p2g2p capacity file
                cap_p2g2p = pd.read_excel(fn_cap_p2g2p, index_col=1)
                # drop columns
                cap_p2g2p.drop(['Emax_h2stor', 'Technology', 'NewInvestment'], axis=1, inplace=True)
                cap_p2g2p = cap_p2g2p.astype(float)

                # check if Pmax_methdac is nonzero
                if not (cap_p2g2p['Pmax_methdac'] == 0).all():
                    # get conversion rate for methanation from input DB
                    conn = pymysql.connect(
                        host=self.host,
                        database=database,
                        user=self.user,
                        password=self.password
                    )
                    cursor = conn.cursor()
                    cursor.execute(f'call {database}.getGeneratorData_Extra({year_dic[y]})')
                    results = cursor.fetchall()

                    df = pd.DataFrame(results)
                    df.columns = [desc[0] for desc in cursor.description]

                    df = df[['GenName', 'Conv_methdac_el']]

                    df = df.T
                    df.columns = df.loc['GenName']

                    for unit in cap_p2g2p.index:
                        # get conversion efficiency
                        conv_methdac_el = df.loc['Conv_methdac_el', unit]

                        # divide MH_H2_th by conversion efficiency to get MW_el
                        cap_p2g2p.loc[unit, 'Pmax_methdac'] = cap_p2g2p.loc[unit, 'Pmax_methdac'] / conv_methdac_el * -1

                cap_p2g2p = cap_p2g2p.sum()

                # append fuel cell capacity
                cap_ch_gen.loc['P2G2P'] = [cap_p2g2p.loc['Pmax']]

                # append electrolyzer capacity and methanation capacity
                cap_ch_con.loc['P2G2P'] = [cap_p2g2p.loc['Pmin']]
                cap_ch_con.loc['Methanation'] = [cap_p2g2p.loc['Pmax_methdac']]

                # merge
                df_cap_gen = pd.concat([df_cap_gen, cap_ch_gen], axis=1)
                df_cap_con = pd.concat([df_cap_con, cap_ch_con], axis=1)

        # remove positive values from load capacity file
        df_cap_con = df_cap_con.applymap(lambda x: 0 if x > 0 else x)

        # export files
        df_cap_gen = df_cap_gen.fillna(0)
        df_cap_con = df_cap_con.fillna(0)
        # Remove columns with all zeros
        df_cap_con = df_cap_con[(df_cap_con != 0).any(axis=1)]

        # put the two dataframes together
        # add 'Load' to all technologies
        indices = df_cap_con.index
        new_indices = []
        for index in indices:
            new_indices.append(index + ' (Load)')
        df_cap_con.index = new_indices

        df_cap = pd.concat([df_cap_gen, df_cap_con], axis=0)
        Cap_CH_centiv = df_cap

        # Capacity Generation of neighbouring countries from PSL database
        # write dataframes with annual data to csv
        # store dataframes for each country
        df_dic_gen = {
            'DE': pd.DataFrame(),
            'AT': pd.DataFrame(),
            'IT': pd.DataFrame(),
            'FR': pd.DataFrame()
        }

        df_dic_con = {
            'DE': pd.DataFrame(),
            'AT': pd.DataFrame(),
            'IT': pd.DataFrame(),
            'FR': pd.DataFrame()
        }

        for year in self.centIv_listyears:
            # get max storage capacities from input DB
            conn = pymysql.connect(
                host=self.host,
                database=database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            cursor.execute(f'call {database}.getGeneratorData({year_dic[year]})')
            results = cursor.fetchall()

            df = pd.DataFrame(results)
            df.columns = [desc[0] for desc in cursor.description]
            df = df[['Country', 'Technology', 'Pmax', 'Pmin']]

            df.set_index('Technology', inplace=True)

            for country in ['DE', 'AT', 'IT', 'FR']:
                # filter by country
                df_country = df[df['Country'] == country]
                df_country = df_country.drop('Country', axis=1)

                # generating capacity
                df_country_gen = df_country.drop('Pmin', axis=1)
                df_country_gen.columns = [year]

                # merge
                df_country_gen = df_country_gen.groupby(df_country_gen.index).sum()
                df_dic_gen[country] = pd.concat([df_dic_gen[country], df_country_gen], axis=1)

                # generating capacity
                df_country_con = df_country.drop('Pmax', axis=1)
                df_country_con.columns = [year]

                # merge
                df_country_con = df_country_con.groupby(df_country_con.index).sum()  # Ensure unique index
                df_dic_con[country] = pd.concat([df_dic_con[country], df_country_con], axis=1)

        for country in ['DE', 'AT', 'IT', 'FR']:
            # prepare dataframe for export
            df_dic_gen[country] = df_dic_gen[country].fillna(0)
            df_dic_con[country] = df_dic_con[country].fillna(0)

            # put the two dataframes together
            # Remove columns with all zeros
            df_dic_con[country] = df_dic_con[country][(df_dic_con[country] != 0).any(axis=1)]

            # add 'Load' to all load technologies
            indices = df_dic_con[country].index
            new_indices = []
            for index in indices:
                new_indices.append(index + ' (Load)')
            df_dic_con[country].index = new_indices

            df_cap = pd.concat([df_dic_gen[country], df_dic_con[country]], axis=0)
            # transform to GW
            df_cap = df_cap / 1000  # in GW

            # write to csv
            df_cap = group_n_rename(df_cap, transposed=True, index_name='Row').sort_index()
            df_cap.to_csv(
                f"{self.parent_directory}/Outputs/{self.simu_name}/national_generation_and_capacity/national_capacity_gw_{country.lower()}.csv")

        return Cap_CH_centiv
    def get_cap_CH(self,Cap_CH_centiv):
        # add power capacity of the DistIv technologies
        annual_cap_CH = Cap_CH_centiv
        annual_cap_CH = annual_cap_CH.T
        annual_cap_CH.index = annual_cap_CH.index.astype(str)

        # get all capacities from technology_list
        # generating capacities
        condition = ((self.generators['GeneratorType'] != 'LoadShed') & (self.generators['GeneratorType'] != 'LoadShift') & (self.generators['OutputType'] == 1))
        capacities = self.generators[condition].index.tolist()
        # load technologies
        condition = (self.generators['OutputType'] == 0)
        # add load capacities to capacities
        for load_technology in self.generators[condition].index:
            capacities.append(load_technology + ' (Load)')


        annual_cap_CH = get_data(annual_cap_CH, annual_cap_CH, capacities)
        annual_cap_CH = annual_cap_CH.T
        # loop through all simulated years to add PV and battery investments
        for year in self.centIv_listyears:
            # add up all investments upon current year
            pv_new = 0
            bat_new = 0
            for n in self.centIv_listyears[:self.centIv_listyears.index(year)+1]:
                pv_new = pv_new + self.pv_inv[n]
                bat_new = bat_new + self.bat_inv[n]

            # add PV-roof and battery investments
            # DistIv batteries are added to Battery-TSO

            # generating capacity
            annual_cap_CH.at['PV-roof', str(year)] = annual_cap_CH.at['PV-roof', str(year)] + pv_new
            annual_cap_CH.at['Battery-TSO', str(year)] = annual_cap_CH.at['Battery-TSO', str(year)] + bat_new

            # consuming capacity
            annual_cap_CH.at['Battery-TSO (Load)', str(year)] = annual_cap_CH.at['Battery-TSO (Load)', str(year)] - bat_new

        #annual_cap_CH.to_csv('Cap_CH_centiv_distiv.csv')

        # prepare output
        annual_cap_CH = annual_cap_CH / 1000  # in GW
        # group technologies and write csv
        annual_cap_CH = group_n_rename(annual_cap_CH, transposed=True, index_name='Row').sort_index()

        annual_cap_CH.to_csv(
            f"{self.parent_directory}/Outputs/{self.simu_name}/national_generation_and_capacity/national_capacity_gw_ch.csv")

    def get_storage_cap_CH(self):
        # add storage capacity of the DistIv technologies

        # read capacities from CentIV
        annual_storage_cap_cap_CH = pd.read_csv(os.path.join(self.parent_directory, f"Storage_Cap_CH_centiv.csv"), index_col=0)

        # only batteries are added from DistIv

        # loop through all simulated years to battery investments
        for year in self.centIv_listyears:
            # add up all investments upon current year
            bat_new = 0
            for n in self.centIv_listyears[:self.centIv_listyears.index(year)+1]:
                # convert invested power capacity to energy capacity
                bat_new = bat_new + self.bat_inv[n] * self.e_max / self.p_max

            # DistIv batteries are added to Battery-TSO

            # storage capacity
            if 'Battery-TSO' not in annual_storage_cap_cap_CH.index:
                annual_storage_cap_cap_CH.at['Battery-TSO', str(year)] = bat_new
            else:
                annual_storage_cap_cap_CH.at['Battery-TSO', str(year)] = annual_storage_cap_cap_CH.at['Battery-TSO', str(year)] + bat_new

        # group technologies and write csv
        annual_cap_CH = group_n_rename(annual_storage_cap_cap_CH, transposed=True, index_name='Row').sort_index()

        annual_cap_CH.to_csv(
            f"{self.parent_directory}/Outputs/{self.simu_name}/national_generation_and_capacity/national_capacity_storage_ch.csv")

def main(simulation: str, database: str, host: str, user: str, password: str):
    parentDirectory = os.getcwd()

    # read generator file
    generators = read_generator_file()

    centIv_listyears = get_folders_with_prefix(f"{parentDirectory}/../../Results/{simulation}", 'CentIv')

    # Example values for pv_inv, bat_inv, e_max, and p_max
    pv_inv = {year: 0 for year in centIv_listyears}
    bat_inv = {year: 0 for year in centIv_listyears}
    e_max = 13.5  # Example value, replace with actual value
    p_max = 1.0   # Example value, replace with actual value

    capacity = Capacity(
        parentDirectory,
        simulation,
        centIv_listyears,
        pv_inv,
        bat_inv,
        generators,
        e_max,
        p_max,
        host=host,
        user=user,
        password=password
    )
    Cap_CH_centiv = capacity.get_cap(database)
    capacity.get_cap_CH(Cap_CH_centiv)
    #capacity.get_storage_cap_CH()

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
        default='pathfndr_s8_241119_cpv_s8',
    )
    argp.add_argument(
        "--DBname",
        type=str,
        help="Name of the input MySQL database",
        default='pathfndr_s8_241119_cpv_s8',
    )
    args = argp.parse_args()
    main(
        simulation=args.simuname,
        database=args.DBname,
        host=settings.input_database_server.host,
        user=settings.input_database_server.user,
        password=settings.input_database_server.password
    )
