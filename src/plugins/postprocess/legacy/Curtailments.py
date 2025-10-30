import os
import pandas as pd
import numpy as np
import argparse

from .Generation import group_n_rename, read_generator_file, get_folders_with_prefix,filter_by_country_and_format,get_country_data

class Curtailments:
    def __init__(self, parent_directory, simu_name, centIv_listyears):
        self.parent_directory = parent_directory
        self.simu_name = simu_name
        self.centIv_listyears = centIv_listyears

    def get_curtailment_data(self,centIv_listyears,parentDirectory,simu_name):
            # get curtailment data for all countries
            # only for CentIv at the moment

            # create dataframes to store annual values
            annual_curtailment_dfs = {
                'DE': pd.DataFrame(),
                'FR': pd.DataFrame(),
                'IT': pd.DataFrame(),
                'AT': pd.DataFrame()
            }
            curtailments_CH = pd.DataFrame()
            # loop over years
            curtailments_dic = {}

            for year in centIv_listyears:

                CentIvDirectory = f"{parentDirectory}/../../Results/{simu_name}/CentIv_{year}"

                # curtailment data for CH   
                fn6 = os.path.join(CentIvDirectory, "REScurtailmentDistIv_hourly_ALL_LP.csv")
                curtail_ch = pd.read_csv(fn6, low_memory=False)
                curtail_all = get_country_data(curtail_ch)
                curtailments_CH[year] = curtail_all['CH']


                # read curtailment data
                df_curtailments = pd.read_excel(os.path.join(f"{parentDirectory}/../../Results/{simu_name}/CentIv_{year}", "CurtailmentPerGen_hourly_ALL_LP.xlsx"))

                # iterate over all countries
                for country in ['CH', 'DE', 'FR', 'IT', 'AT']:
                    # get country data
                    df_country = filter_by_country_and_format(df_curtailments, country)

                    # export hourly curtailment to output directory
                    df_h = df_country.copy()

                    if country == 'CH':
                        # for CH export curtailment data to add DistIV results

                        # subtract CentIv's DistIv curtailment
                        if 'PV-roof' in df_h.columns:
               
                            df_h['PV-roof'] = df_h['PV-roof'] - curtailments_CH[year]

                        else:
                            df_h['PV-roof'] = curtailments_CH[year] * -1
                        curtailments_dic[year] = df_h
                    else:
                        # convert to GWh
                        df_country = df_country / 1000

                        # export files for all neighbour countries
                        # group technologies
                        df_h = group_n_rename(df_h, index_name='Hour')
                        df_h.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_curtailment_hourly_c_{country.lower()}_{year}.csv")

                        # monthly curtailment
                        df_m = df_country.copy()
                        # convert in TWh
                        df_m = df_m / 1000
                        df_m['date'] = pd.date_range(start='1/1/2018', periods=len(df_country), freq='H')
                        df_m = df_m.resample('M', on='date').sum()
                        df_m = df_m.reset_index()
                        df_m["Month"] = range(1, 1 + len(df_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly 
                        df_m = df_m.drop(["date"], axis=1)

                        df_m = df_m.set_index("Month")
                        df_m.index.name = "Month"  # Set the index name to "Month"
                        group_n_rename(df_m).to_csv(
                            f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_curtailment_monthly_c_{country.lower()}_{year}.csv")

                        # annual generation has to be done only once
                        df_annual = df_country.T
                        df_annual = df_annual.sum(axis=1)
                        # convert in TWh
                        df_annual = df_annual / 1000

                        # write to dataframe in dictionary
                        annual_curtailment_dfs[country] = pd.concat([annual_curtailment_dfs[country], df_annual], axis=1)

            # write annual dataframes to files
            for country in ['DE', 'FR', 'IT', 'AT']:
                annual_curtailment_dfs[country].columns = centIv_listyears
                group_n_rename(annual_curtailment_dfs[country], index_name='Row', transposed=True).to_csv(
                    f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_curtailment_annual_c_{country.lower()}.csv")
            return curtailments_dic

    def get_curtailment_CH(self, curtailments):
        curtailments_dic = {}

        # calculate the PV curtailments for CH

        # dataframe to store the annual curtailment
        annual_curtailment_df = pd.DataFrame()
        # loop over all years
        for year in self.centIv_listyears:
            # read curtailment file
            curtailment_df = curtailments.get(year, 0)


            # add DistIv PV curtailment
            curtailment_df['PV-roof'] = curtailment_df['PV-roof'] + curtailments_dic.get(year, 0)

            # convert to GWh
            curtailment_df = curtailment_df / 1000

            # group technologies
            df_h = group_n_rename(curtailment_df, index_name='Hour')
            df_h.to_csv(
                f"{self.parent_directory}/Outputs/{self.simu_name}/national_generation_and_capacity/national_curtailment_hourly_c_ch_{year}.csv")

            # monthly curtailment
            df_m = curtailment_df.copy()
            # convert in TWh
            df_m = df_m / 1000
            df_m['date'] = pd.date_range(start='1/1/2018', periods=len(curtailment_df), freq='H')
            df_m = df_m.resample('M', on='date').sum()
            df_m = df_m.reset_index()
            df_m["Month"] = range(1, 1 + len(df_m))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
            df_m = df_m.drop(["date"], axis=1)
            df_m = df_m.set_index("Month")
            group_n_rename(df_m, index_name='Month').to_csv(
                f"{self.parent_directory}/Outputs/{self.simu_name}/national_generation_and_capacity/national_curtailment_monthly_c_ch_{year}.csv")

            # annual generation has to be done only once
            # annual in Twh
            df_annual = df_m.T
            df_annual = df_annual.sum(axis=1)
            df_annual = df_annual

            # write to dataframe in dictionary
            annual_curtailment_df = pd.concat([annual_curtailment_df, df_annual], axis=1)

        annual_curtailment_df.columns = self.centIv_listyears
        group_n_rename(annual_curtailment_df, index_name='Row', transposed=True).to_csv(
            f"{self.parent_directory}/Outputs/{self.simu_name}/national_generation_and_capacity/national_curtailment_annual_c_ch.csv")

        return

def main(simulation: str):
    parentDirectory = os.getcwd()

    centIv_listyears = get_folders_with_prefix(f"{parentDirectory}/../../Results/{simulation}", 'CentIv')

    curtailments = Curtailments(parentDirectory, simulation, centIv_listyears)
    curtailments_dic = curtailments.get_curtailment_data(centIv_listyears,parentDirectory,simulation)
    curtailments.get_curtailment_CH(curtailments_dic)

if __name__ == "__main__":
    argp = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    argp.add_argument(
        "--simuname",
        type=str,
        help="Name of MySQL database results",
        default='pathfndr_s8_241119_cpv_s8'
    )
    args = argp.parse_args()
    main(simulation=args.simuname)
