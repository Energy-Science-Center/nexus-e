import os
from pathlib import Path
import pandas as pd
import argparse
import pymysql

from .Generation import group_n_rename,filter_by_country_and_format,read_generator_file,get_data
from ..results_context import get_years_simulated_by_centiv

from nexus_e import config

year_dic = {
    2018: 1,
    2020: 2,
    2030: 3,
    2040: 4,
    2050: 5
}

def export_h2_balance(
    year,
    CentIvDirectory,
    simulation_postprocess_path,
) -> None:
        # get the H2 balance of Switzerland
    # read all sheets into a dictionary with dataframes
    # TODO: add other countries if required

    # sheet names
    sheet_names = {
        'H2byEL_hourly_all': 'P2G2P (Load)',
        'H2meth_hourly_all': 'Methanation (Load)',
        'H2market_hourly_all': 'Imports',
        'H2recon_hourly_all': 'P2G2P'
    }

    # prepare output file
    h2balance_file = pd.DataFrame()
    # filepath
    fn_h2balance = os.path.join(CentIvDirectory, 'P2G2P_H2vars_hourly_LP.xlsx')
    if os.path.exists(fn_h2balance):
        all_sheets = pd.read_excel(fn_h2balance, sheet_name=None, index_col=0)
        # read file for market exports
        fn_h2export = os.path.join(CentIvDirectory, 'P2G2P_H2Market_hourly_CH_LP.xlsx')
        h2exports = filter_by_country_and_format(pd.read_excel(fn_h2export), 'CH')

        for sheet in all_sheets:
            # sum of all units
            h2balance_file[sheet_names[sheet]] = all_sheets[sheet].sum(axis=1)
        if 'P2G2P' in h2exports.columns:
            h2balance_file['Exports'] = h2exports['P2G2P'] * -1
        else:
            h2balance_file['Exports'] = 0
    else:
        # empty dataframe
        for sheet in sheet_names:
            h2balance_file[sheet_names[sheet]] = [0] * 8760
        h2balance_file['Exports'] = [0] * 8760

    # H2 PP hydrogen consumption & Methanation is negative
    h2balance_file['P2G2P'] = h2balance_file['P2G2P'] * -1
    h2balance_file['Methanation (Load)'] = h2balance_file['Methanation (Load)'] * -1
    h2balance_file.index.name = 'Hour'

    h2balance_file.to_csv(
        os.path.join(
            simulation_postprocess_path,
            "national_generation_and_capacity",
            f"h2_balance_ch_{year}.csv"
        )
    )

def get_storage_levels(
    country,
    country_data,
    year,
    CentIvDirectory,
    database,
    host: str,
    user: str,
    password: str
):
    # this method generates a file with the SOC of all storage technologies
    # TODO: H2 for neighbour countries, adapt if file format changes
    if not country_data.empty:
        # get discharge efficiency for all units from input DB
        conn = pymysql.connect(host=host, database=database, user=user, password=password)
        cursor = conn.cursor()
        cursor.execute(f'call {database}.getGeneratorData({year_dic[year]})')
        results = cursor.fetchall()

        df = pd.DataFrame(results)
        df.columns = [desc[0] for desc in cursor.description]
        df = df[['idGen', 'GenName', 'Technology', 'eta_dis']]
        df = df.T
        df.columns = df.loc['GenName']

        # store technolgies
        technologies = []

        # TODO remove this condition once the file is integrated
        for column in country_data:
            # multiply SOC with discharge efficiency
            # ignore technologies without SOC

            # TODO remove when testing is over
            if column not in df.columns:
                technology = 'Unknown'
                efficiency = 1
                if country_data[column].sum() != 0:
                    print(f'No efficiency for the following Unit: {column}')
            else:
                technology = df.loc['Technology', column]
                efficiency = df.loc['eta_dis', column]
                if efficiency is None:
                    print(f'Nan efficiency value for: {column}')

            country_data[column] = country_data[column] * efficiency
            technologies.append(technology)

        # add technologies to dataframe
        country_data.loc['Technology'] = technologies

        # group by technology
        country_data = country_data.T
        country_data = country_data.groupby(by='Technology').sum()
        country_data = country_data.T

    # get storage levels for all countries and technologies
    df_storage_lvl = pd.DataFrame()
    # get all generators
    generators = read_generator_file()

    generator_list = generators.loc[generators['OutputType'] == 1].index.tolist()
    df_storage_lvl = get_data(country_data, df_storage_lvl, generator_list)

    # TODO: fix for all countries
    if country == 'CH':
        fn_h2_soc = os.path.join(CentIvDirectory, 'P2G2P_H2SoC_hourly_ALL_LP.xlsx')
        # check if file exists
        if os.path.exists(fn_h2_soc):
            # read file
            h2_soc = pd.read_excel(fn_h2_soc, index_col=0)

            # check if file is empty (if yes, DB is also empty)
            if not (h2_soc == 0).all().all():
                # read consumption file for unit names
                h2_con = pd.read_excel(os.path.join(CentIvDirectory,'P2G2P_CH4DACconsumption_hourly_CH_LP.xlsx'), nrows=3)

                # get conversion efficiency for fuel cells
                conn = pymysql.connect(host=host, database=database, user=user, password=password)
                cursor = conn.cursor()
                cursor.execute(f'call {database}.getGeneratorData_Extra({year_dic[year]})')
                results = cursor.fetchall()
                df = pd.DataFrame(results)

                if not df.empty:
                    df.columns = [desc[0] for desc in cursor.description]
                    cursor.close()
                    conn.close()
                    df = df[['GenName', 'Conv_fc']]
                    df.set_index('GenName', inplace=True)
                    df = df.T

                    for column in h2_soc:
                        unit_name = h2_con.loc[2, column]
                        if unit_name not in df.columns:
                            print(f'No efficiency for the following P2G2P unit: {unit_name}')
                            efficiency = 23.32
                        else:
                            efficiency = df.loc['Conv_fc', unit_name]

                        # multiply mass with conversion efficiency (tonnes * MWh_el / tonne)
                        h2_soc[column] = h2_soc[column] * efficiency

                    # sum up all units
                    df_storage_lvl['H2'] = h2_soc.sum(axis=1)
                else:
                    print(f'Empty DB for {year} even if H2-SOC exists!')
                    df_storage_lvl['H2'] = h2_soc.sum(axis=1) * 23.31
            else:
                df_storage_lvl['H2'] = [0] * 8760
        else:
            df_storage_lvl['H2'] = [0] * 8760
    else:
        # H2 SOC for neighbour countries is 0 at the moment
        df_storage_lvl['H2'] = [0] * 8760
    df_storage_lvl.fillna(0, inplace=True)
    return df_storage_lvl


def get_storage_capacities(
    centiv_years,
    simulation_postprocess_path,
    simulation,
    database,
    year_dic,
    host: str,
    user: str,
    password: str
) -> pd.DataFrame:
        # get the maximum storage capacity for country for every year

        # store dataframes for each country
        df_dic = {
            'CH': pd.DataFrame(),
            'DE': pd.DataFrame(),
            'AT': pd.DataFrame(),
            'IT': pd.DataFrame(),
            'FR': pd.DataFrame()
        }

        for year in centiv_years:
            CentIvDirectory = f"CentIv_{year}"

            # get max storage capacities from input DB
            conn = pymysql.connect(host=host, database=database, user=user, password=password)
            cursor = conn.cursor()
            cursor.execute(f'call {database}.getGeneratorData({year_dic[year]})')
            results = cursor.fetchall()

            df = pd.DataFrame(results)
            df.columns = [desc[0] for desc in cursor.description]
            df = df[['GenName', 'Country', 'Technology', 'Emax', 'eta_dis', 'CandidateUnit']]

            df.set_index('GenName', inplace=True)

            # get H2 storage capacities from the Input DB
            # get conversion efficiency for fuel cells
            conn = pymysql.connect(host=host, database=database, user=user, password=password)
            cursor = conn.cursor()
            cursor.execute(f'call {database}.getGeneratorData_Extra({year_dic[year]})')
            results = cursor.fetchall()

            df_H2 = pd.DataFrame(results)

            if not df_H2.empty:
                df_H2.columns = [desc[0] for desc in cursor.description]

                df_H2 = df_H2[['GenName', 'Country', 'Technology', 'Emax_h2stor', 'Conv_fc']]
            else:
                # check if units were built
                # in Switzerland not all units listed in the Input DB are actually built
                fn_cap_p2g2p = os.path.join(CentIvDirectory, "NewUnits_P2G2P.xlsx")
                if os.path.exists(fn_cap_p2g2p):
                    newunits_p2g2p = pd.read_excel(fn_cap_p2g2p, index_col=1)
                    if not newunits_p2g2p.empty:
                        # error message if new units are built but there's no conversion efficiency
                        print(f'Missing conversion efficiency for H2 data in Input DB in {year}!')

            for country in df_dic:
                # filter by country
                df_country = df[(df['Country'] == country) & (df['CandidateUnit'] == 0)]
                df_country = df_country.drop('Country', axis=1)

                # multiply E_max with conversion efficiency
                df_country['El_potential'] = df_country['Emax'] * df_country['eta_dis']

                df_country = df_country.groupby(by='Technology').sum()
                df_country = df_country.T

                # H2
                if not df_H2.empty:
                    # filter by country
                    df_H2_country = df_H2[df_H2['Country'] == country]
                    df_H2_country = df_H2_country.drop('Country', axis=1)
                    if country != 'CH':
                        # TODO: might need some debugging as soon as hydrogen is available for neighbour countries
                        # in the neighbour countries all units listed are built
                        # calculate the electric potential by muliplying the E_max with the conversion efficiency
                        df_H2_country['El_pot'] = df_H2_country['Emax_h2stor'] * df_H2_country['Conv_fc']
                        df_H2_country = df_H2_country.drop(['Emax_h2stor', 'Conv_fc'], axis=1)

                        df_H2_country = df_H2_country.groupby(by='Technology').sum(numeric_only=True)
                        df_H2_country = df_H2_country.T

                    else:
                        # in Switzerland not all units listed in the Input DB are actually built
                        fn_cap_p2g2p = os.path.join(CentIvDirectory, "NewUnits_P2G2P.xlsx")
                        if os.path.exists(fn_cap_p2g2p):
                            newunits_p2g2p = pd.read_excel(fn_cap_p2g2p, index_col=1)

                            df_H2_country.set_index('GenName', inplace=True)
                            df_H2_country['El_pot'] = 0
                            for unit in newunits_p2g2p.index:
                                # get conversion efficiency from Input DB
                                conversion_FC = df_H2_country.loc[unit, 'Conv_fc']
                                df_H2_country.loc[unit, 'El_potential'] = conversion_FC * newunits_p2g2p.loc[unit, 'Emax_h2stor']

                            df_H2_country = df_H2_country.groupby(by='Technology').sum(numeric_only=True)
                            df_H2_country = df_H2_country.T

                        else:
                            print('Missing file: NewUnits_P2G2P.xlsx')

                    # add H2 to dataframe
                    if 'P2G2P' in df_H2_country:
                        df_country['H2'] = df_H2_country['P2G2P']
                    else:
                        df_country['H2'] = 0

                else:
                    df_country['H2'] = 0

                df_country = df_country.T
                df_country.drop(['Emax', 'eta_dis', 'CandidateUnit'], axis=1, inplace=True)

                df_country.columns = [year]

                df_dic[country] = pd.concat([df_dic[country], df_country], axis=1)
            
            fn_dac = os.path.join(CentIvDirectory, 'P2G2P_CH4DACconsumption_hourly_CH_LP.xlsx')
            p2g2p = False
            if os.path.isfile(fn_dac):
                p2g2p = True
                dac_data = pd.read_excel(fn_dac)
                # p2g2p
                fn_p2g2p_con = os.path.join(CentIvDirectory, 'P2G2P_ELconsumption_hourly_CH_LP.xlsx')
                p2g2p_con_data = pd.read_excel(fn_p2g2p_con)
                fn_p2g2p_gen = os.path.join(CentIvDirectory, 'P2G2P_REgeneration_hourly_CH_LP.xlsx')
                p2g2p_gen_data = pd.read_excel(fn_p2g2p_gen)
            # get storage levels for all countries
            if p2g2p:
                fn_storage_lvl = os.path.join(CentIvDirectory, 'StorageLevelPerGenGenNames_hourly_ALL_LP.csv')
                if os.path.isfile(fn_storage_lvl):
                    data_storage_lvl = pd.read_csv(fn_storage_lvl, low_memory=False)
                else:
                    data_storage_lvl = pd.DataFrame()

                export_h2_balance(
                    year,
                    CentIvDirectory,
                    simulation_postprocess_path
                )

                for country in ['CH', 'DE', 'AT', 'IT', 'FR']:
                    # loop over countries
                    storage_level_country = get_storage_levels(
                        country,
                        filter_by_country_and_format(data_storage_lvl, country, group_techs=False),
                        year,
                        CentIvDirectory,
                        database,
                        host=host,
                        user=user,
                        password=password
                    )
                    if not storage_level_country.empty:
                        # export only dataframes that are not empty
                        group_n_rename(
                            storage_level_country,
                            index_name='Hour',
                            add_columns=['H2']
                        ).to_csv(
                            os.path.join(
                                simulation_postprocess_path,
                                "national_generation_and_capacity",
                                f"storage_levels_c_{country.lower()}_{year}.csv"
                            )
                        )

        # export dataframe
        for country in df_dic:
            if country != 'CH':
                df_grouped = group_n_rename(df_dic[country], transposed=True, index_name='Row', add_columns=['H2'])
                df_grouped.to_csv(
                    os.path.join(
                        simulation_postprocess_path,
                        "national_generation_and_capacity",
                        f"national_capacity_storage_{country.lower()}.csv"
                    )
                )
            else:
                Storage_Cap_CH_centiv = df_dic[country]
        return Storage_Cap_CH_centiv


def __get_battery_parameters() -> tuple[int, int, int]:
        try:
            df = pd.read_csv('battery_parameters.csv')
            # Extract the variables
            bat_inv = df['bat_inv'].values[0]
            p_max = df['p_max'].values[0]
            e_max = df['e_max'].values[0]
        except FileNotFoundError:
            bat_inv = 0
            p_max = 1
            e_max = 0
        return bat_inv, e_max, p_max

def get_storage_cap_CH(simulation_postprocess_path, centIv_listyears, simu_name,DistIV,Storage_Cap_CH_centiv):

    output_path = os.path.join(
        simulation_postprocess_path,
        "national_generation_and_capacity",
    )
    if DistIV:
        bat_inv, e_max, p_max = __get_battery_parameters()

        # add storage capacity of the DistIv technologies

        # read capacities from CentIV
        annual_storage_cap_cap_CH = Storage_Cap_CH_centiv
        # only batteries are added from DistIv

        # loop through all simulated years to battery investments
        for year in centIv_listyears:
            # add up all investments upon current year
            bat_new = 0
            for n in centIv_listyears[:centIv_listyears.index(year)+1]:
                # convert invested power capacity to energy capacity
                bat_new = bat_new + bat_inv * e_max / p_max

            # storage capacity
            if 'Battery-TSO' not in annual_storage_cap_cap_CH.index:
                annual_storage_cap_cap_CH.at['Battery-TSO', year] = bat_new
            else:
                annual_storage_cap_cap_CH.at['Battery-TSO', year] = annual_storage_cap_cap_CH.at['Battery-TSO', year] + bat_new
    else:
        annual_storage_cap_cap_CH = pd.read_csv(os.path.join(output_path, 'national_capacity_gw_ch.csv'))
        annual_storage_cap_cap_CH = annual_storage_cap_cap_CH[annual_storage_cap_cap_CH['Row'] == 'Battery']

    annual_storage_cap_cap_CH = group_n_rename(annual_storage_cap_cap_CH, transposed=True, index_name='Row')
    # group technologies and write csv
    annual_storage_cap_cap_CH.to_csv(
        os.path.join(
            simulation_postprocess_path,
            "national_generation_and_capacity",
            "national_capacity_storage_ch.csv"
        )
    )

def main(simulation: str, database: str, host: str, user: str, password: str):
    os.path.abspath(os.curdir)
    simulation_postprocess_path = "postprocess"

    centiv_years = get_years_simulated_by_centiv(Path())
    # check if DistIv results exist:
    fn1 = "DistIv_2030.mat"
    fn2 = "DistIv_2040.mat"
    fn3 = "DistIv_2050.mat"

    if os.path.exists(fn1) or os.path.exists(fn2) or os.path.exists(fn3):
        DistIV = True
    else:
        DistIV = False

    Storage_Cap_CH_centiv = get_storage_capacities(
        centiv_years,
        simulation_postprocess_path,
        simulation,
        database,
        year_dic,
        host=host,
        user=user,
        password=password
    )    
    get_storage_cap_CH(
        simulation_postprocess_path,
        centiv_years,
        simulation,
        DistIV,
        Storage_Cap_CH_centiv
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
