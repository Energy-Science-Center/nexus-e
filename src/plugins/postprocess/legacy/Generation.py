import os
from pathlib import Path
import pandas as pd
import numpy as np
import argparse
import h5py
import re

from ..results_context import get_years_simulated_by_centiv

year_dic = {
    2018: 1,
    2020: 2,
    2030: 3,
    2040: 4,
    2050: 5
}


def create_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
        print("Created Directory : ", dir)
    else:
        print("Directory already existed : ", dir)
    return dir

def get_folders_with_prefix( directory, prefix):
    # get list of years with the same prefix
    years = []
    for folder in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, folder)) and folder.startswith(prefix):
            years.append([int(num) for num in re.findall(r'\d+', folder)][-1])
    return sorted(years)

def set_unit_names_as_column_names(data):
    data = data.T
    # return dataframe with unit name as column names
    data.index = data[2]
    data = data.iloc[:, 2:].astype(float)
    data = data.T
    return data

def read_generator_file():
        # read generator file to list all relevant technologies
        generator_list_file = os.path.dirname(os.path.abspath(__file__)) + "/Generator_List.xlsx"
        generators = pd.read_excel(generator_list_file)
        # We drop these duplicated lines to avoid counting values twice, especially
        # in the group_n_rename() function
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

def group_n_rename(input_table, transposed=False, index_name=None, add_columns=None):
        # groups certain technologies according to Generator_list
        # transposed: bool,  if dataframe comes with technologies in columns
        # index_name: string, renames the index according to value
        # add_columns: list, appends columns to output dataframe that are not listed in the Generator list
        
        input_table = input_table.copy()
        if not transposed:
            # In this function we work with Technologies as rows and not columns
            input_table = input_table.transpose()

        generators = read_generator_file()
        output = generators.copy()

        # Here we rename some of the technologies in Generator_List.
        # The problem seems to be that the way to refer to table entries is
        # inconsistent between Generator_List and the input_table (which I guess is
        # usually the output of CentIv).
        # In the input_table, the Technology is the unique identifier of the entry 
        # but in Generator_list the unique identifier is a combination of Technology
        # and OutputType.
        #
        # For example:
        # input_table      | Generator_List
        # -----------------------------------------
        # Technology       | Technology OutputType
        # Pump-Open        | Pump-Open  1
        # Pump-Open (Load) | Pump-Open  0
        #
        # So, we rename the technologies in Generator_List to match the ones in
        # input_table. A better solution would be to remove the source of this
        # inconsistency (the fact that these names are defined in different places),
        # because now if the names change in CentIv for some reason we will need to
        # change them here as well, which is prettty bad.

        output = output.reset_index(names="Technology")
        def add_suffix_to_technology(
            table: pd.DataFrame,
            mask: pd.Series,
            suffix: str,
        ):
            column = "Technology"
            table.loc[mask, column] = table.loc[mask, column].apply(
                lambda technology: f"{technology}{suffix}"
            )
        add_suffix_to_technology(
            table=output,
            mask= (
                (output.GeneratorType == "Consumption")
                | (
                    (output.GeneratorType == "Storage")
                    & (output.OutputType == 0)
                )
            ),
            suffix=" (Load)"
        )
        add_suffix_to_technology(
            table=output,
            mask= (
                (output.GeneratorType == "LoadShift")
                & (output.OutputType == 0)
            ),
            suffix=" (Up)"
        )
        output.index = output.Technology

        output = output.join(input_table)
        output = output[["PlotGroup"] + input_table.columns.tolist()]
        output = output.groupby("PlotGroup").sum(numeric_only=True)

        # These are actually rows now, but we kept the name "column" for
        # retrocompatibility
        if add_columns:
            for column in add_columns:
                if column in input_table.index:
                    output.loc[column] = input_table.loc[column]

        if not transposed:
            output = output.transpose()

        if index_name:
            output.index.name = index_name

        return output

def filter_by_country_and_format(data, country, group_techs=True):
        # method to get data for each country, grouped by technology
        # similar to get_gencons but for a slightly different format
        # if group_techs is False, the returned dataframe has idGen as column names and the technology type in the first row
        if data.empty:
            return data
        data = filter_by_country(data, country)
        # drop first row
        data.drop(0, axis="index", inplace=True)
        if group_techs:
            data = group_by_technologies(data)
        else:
            data = set_unit_names_as_column_names(data)
        return data
def get_country_data(data):
        # data calculation for CH and neighbouring countries
        data = data.transpose()
        data_df_CH = data[
            (data[0] != "DE") & (data[0] != "DE_X") & (data[0] != "FR") & (
                    data[0] != "FR_X") &
            (data[0] != "IT") & (data[0] != "IT_X") & (data[0] != "AT") & (
                    data[0] != "AT_X")]
        data_df_CH = data_df_CH.drop([0, 1], axis=1)
        data_df_CH = data_df_CH.drop(["Unnamed: 0"])
        data_df_CH = data_df_CH.astype(float)
        data_sum_CH = data_df_CH.sum()
        data_sum_CH = data_sum_CH.reset_index()
        data_sum_CH = data_sum_CH.drop(["index"], axis=1)

        # DE data

        options_DE = ['DE', 'DE_X']
        data_df_DE = data[data[0].isin(options_DE)]
        data_df_DE = data_df_DE.drop([0, 1], axis=1)
        data_df_DE = data_df_DE.astype(float)
        data_sum_DE = data_df_DE.sum()
        data_sum_DE = data_sum_DE.reset_index()
        data_sum_DE = data_sum_DE.drop(["index"], axis=1)

        # FR data

        options_FR = ['FR', 'FR_X']
        data_df_FR = data[data[0].isin(options_FR)]
        data_df_FR = data_df_FR.drop([0, 1], axis=1)
        data_df_FR = data_df_FR.astype(float)
        data_sum_FR = data_df_FR.sum()
        data_sum_FR = data_sum_FR.reset_index()
        data_sum_FR = data_sum_FR.drop(["index"], axis=1)

        # IT data

        options_IT = ['IT', 'IT_X']
        data_df_IT = data[data[0].isin(options_IT)]
        data_df_IT = data_df_IT.drop([0, 1], axis=1)
        data_df_IT = data_df_IT.astype(float)
        data_sum_IT = data_df_IT.sum()
        data_sum_IT = data_sum_IT.reset_index()
        data_sum_IT = data_sum_IT.drop(["index"], axis=1)

        # AT data

        options_AT = ['AT', 'AT_X']
        data_df_AT = data[data[0].isin(options_AT)]
        data_df_AT = data_df_AT.drop([0, 1], axis=1)
        data_df_AT = data_df_AT.astype(float)
        data_sum_AT = data_df_AT.sum()
        data_sum_AT = data_sum_AT.reset_index()
        data_sum_AT = data_sum_AT.drop(["index"], axis=1)

        return {'DE': data_sum_DE[0], 'CH': data_sum_CH[0], 'FR': data_sum_FR[0], 'IT': data_sum_IT[0],
                'AT': data_sum_AT[0]}

def group_by_technologies(data):
        data = data.T
        # remove unnecessary columns and transform to floats
        data.drop(2, axis=1, inplace=True)
        techs = data[1]
        data = data.iloc[:, 1:].astype(float)
        data[1] = techs
        data = data.groupby(by=[1]).sum()
        # restructure dataframe
        data = data.T
        data.reset_index(inplace=True)
        data.drop(['index'], axis=1, inplace=True)
        return data

def filter_by_country(data, country):
        data = data.T
        data = data[data[0] == country]
        data = data.T
        return data
def get_data(data_df, df, gen_list):
        # loop through all generator types fueled by elements of gen_list
        # return dataframe df with added column
        for fuel in gen_list:
            if fuel in data_df:
                df[fuel] = data_df[fuel]
            else:
                df[fuel] = 0
        return df

class Generation:

    def __init__(
        self,
        postprocess_output_path,
        simulation,
        models,
        centiv_years,
        distiv=True
    ):
        self.postprocess_output_path = postprocess_output_path
        self.simulation = simulation
        self.models = models
        self.centiv_years = centiv_years
        self.distiv = distiv

    def load_data(self, year, model):
            fn = os.path.join(self.postprocess_output_path, f"Gen_CH_{year}_{model}.csv")
            if os.path.isfile(fn):
                return pd.read_csv(fn, low_memory=False)
            return None
    def load_distiv_data(self, year):
        fn = f"DistIv_{year}.mat"
        if os.path.isfile(fn):
            data = h5py.File(fn, 'r')
            contents = data['resDistIv']
            return contents
        else:
            print(f"File {fn} does not exist.")
            return None
    def process_distiv_data(self, contents):
        PV = contents['plotting']["p_gen_fullyear"][()]
        PV2 = PV[:, :, :4]
        PV3 = np.sum(PV2, axis=2)
        PV4 = np.sum(PV3, axis=0)
        PV4_df = pd.DataFrame(PV4) / 1000
        PV4_df = PV4_df.squeeze()

        PV_cur = contents['plotting']["p_curt_fullyear"][()]
        PV_cur2 = np.sum(PV_cur, axis=0)
        PV_cur2_df = pd.DataFrame(PV_cur2) / 1000
        curtailment = PV_cur2_df.squeeze()

        PV_df = PV4_df - curtailment

        PVb = PV[:, :, 6]
        PVb_df = pd.DataFrame(PVb)
        PVb_c_df = PVb_df[PVb_df < 0].sum(axis=0) / 1000
        PV_bat_char_df = PVb_c_df.squeeze()

        PVb_d_df = PVb_df[PVb_df >= 0].sum(axis=0) / 1000
        PV_bat_dis_df = PVb_d_df.squeeze()

        Cap_new = contents['capacity']['invest']["Invest_capacity_sum_MW"][()]
        pv_inv = sum(Cap_new[0][0:4])
        bat_inv = np.array(Cap_new[0][6])

        dsm_up = contents['interface']["Demand_shiftedup_regional_hourly_MWh_fullyear"][()]
        dsm_up_df = pd.DataFrame(dsm_up)

        dsm_down = contents['interface']["Demand_shifteddown_regional_hourly_MWh_fullyear"][()]
        dsm_down_df = pd.DataFrame(dsm_down)

        return PV_df, curtailment, PV_bat_char_df, PV_bat_dis_df, pv_inv, bat_inv, dsm_up_df, dsm_down_df
    def get_gencons(self,gencon_country):
        # transposes the input dataframe into the needed form
        gencon_country = gencon_country.drop([0], axis=1)
        gencon_country = gencon_country.set_index(1)
        gencon_country = gencon_country.transpose()
        gencon_country = gencon_country.reset_index()
        return gencon_country * -1
    def create_gendata(self,generation_df, consumption_df, loads, c, y, model):
        # use this method to transform the data coming from CentIV and FlexEco into the desired structure
        # data_df: generation data of the output files
        # c: country; y: year; model: c for centIV, e for FlexEco

        # create a DF shell
        df_out = pd.DataFrame([])

        # get data of all normal generator types
        generator_list = generators.loc[generators['GeneratorType'] == 'Generator'].index.tolist()
        df_out = get_data(generation_df, df_out, generator_list)
        # get data of storage technologies
        # generation
        storage_list_gen = generators.loc[
            (generators['GeneratorType'] == 'Storage') & (generators['OutputType'] == 1)].index.tolist()
        for technology in storage_list_gen:
            if technology in generation_df:
                df_out[technology] = generation_df[technology]
            else:
                df_out[technology] = 0
        # load
        # storage load
        storage_list_load = generators.loc[
            (generators['GeneratorType'] == 'Storage') & (generators['OutputType'] == 0)].index.tolist()
        for technology in storage_list_load:
            if technology in consumption_df:
                df_out[technology + ' (Load)'] = consumption_df[technology]
            else:
                df_out[technology + ' (Load)'] = 0

        # get data of load shifting technologies
        # down
        loadshift_down = generators.loc[
            (generators['GeneratorType'] == 'LoadShift') & (generators['OutputType'] == 1)].index.tolist()
        for technology in loadshift_down:
            if technology in generation_df:
                df_out[technology] = generation_df[technology]
            else:
                df_out[technology] = 0
        # up
        loadshift_up = generators.loc[
            (generators['GeneratorType'] == 'LoadShift') & (generators['OutputType'] == 0)].index.tolist()
        for technology in loadshift_up:
            if technology in consumption_df:
                df_out[technology + ' (Up)'] = consumption_df[technology]
            else:
                df_out[technology + ' (Up)'] = 0

        # get load shedding data
        loadshed_list = generators.loc[generators['GeneratorType'] == 'LoadShed'].index.tolist()
        df_out = get_data(generation_df, df_out, loadshed_list)

        # get pure consumption technologies
        con_technology_list = generators.loc[generators['GeneratorType'] == 'Consumption'].index.tolist()
        for technology in con_technology_list:
            if technology in consumption_df:
                df_out[technology + ' (Load)'] = consumption_df[technology]
            else:
                df_out[technology + ' (Load)'] = 0

        # load
        df_out["Load (Total)"] = loads[c]

        # transform dataframe into float
        df_out = df_out.astype(float)

        # find and accumulate PV generators
        # subtract PV curtailment (CH: PV-roof can be negative if added from DistIv)
        df_out['PV-roof'] = np.array(df_out["PV-roof"].astype(np.float64)) + np.array(
            consumption_df['PV_curtail'].astype(np.float64).dropna()) #dropna() is needed specially if we run for less than 8760 hours
        pv_types = generators.loc[generators['UnitCategory'] == 'Solar'].index.tolist()
        df_out['PV-Total'] = df_out[pv_types].sum(axis=1)

        # find and accumulate Wind generators
        wind_types = generators.loc[generators['UnitCategory'] == 'Wind'].index.tolist()
        df_out['Wind-Total'] = df_out[wind_types].sum(axis=1)

        if c == 'CH':
            df_out['Imports'] = generation_df['Imports']
            df_out['Exports'] = consumption_df['Exports']
            demand_Ch = pd.read_csv(
                os.path.join(
                    self.postprocess_output_path,
                    "national_generation_and_capacity",
                    f"demand_hourly_{model}_{c.lower()}_{y}.csv"
                ),
                index_col=0
            )
            df_out["Load (Total)"] = demand_Ch.sum(axis=1)

            df_out["Load (Total)"].to_csv(
                os.path.join(
                    self.postprocess_output_path,
                    f"Load_{c}_{y}_{model}.csv"
                )
            )
            
            # name index
            df_out.index.name = "Hour"
            #these files are used in revenue_profit.py
            df_out.to_csv(
                os.path.join(
                    self.postprocess_output_path,
                    f"Gen_{c}_{y}_{model}.csv"
                )
            )

        else:
            # Swiss net load will be calculated in webviewcentiv_distiv
            df_out["Load (Net)"] = df_out["Load (Total)"] - df_out["Wind-Total"] - df_out["PV-Total"]
            # name index
            df_out.index.name = "Hour"
            #these files are used in revenue_profit.py
            df_out.to_csv(
                os.path.join(
                    self.postprocess_output_path,
                    f"Gen_{c}_{y}_{model}.csv"
                )
            )

            # change number format
            df_out = df_out.fillna(0)
            df_out = df_out.astype(float)
            df_out = df_out / 1000  # in GW

            # write dataframe to csv
            extra_columns = ['Exports', 'Imports', 'Imports (Net)', 'Load (Net)', 'Load (Total)']
            group_n_rename(
                df_out, add_columns= extra_columns,index_name="Hour"
            ).to_csv(
                os.path.join(
                    self.postprocess_output_path,
                    "national_generation_and_capacity",
                    f"national_generation_hourly_gwh_{model}_{c.lower()}_{y}.csv"
                )
            )


            # monthly generation
            df_m = df_out / 1000  # in TWh

            df_m['date'] = pd.date_range(start='1/1/2018', periods=len(df_m), freq='H')
            df_m = df_m.resample('M', on='date').sum()
            df_m = df_m.reset_index()
            df_m["Month"] = range(1, 1 + len(df_m)) # to be able to run the model partially, e.g. for 168 hours, it is needed to define this flexibly 
            df_m = df_m.drop(["date"], axis=1)
            group_n_rename(
                df_m, add_columns=extra_columns,index_name="Month"
            ).to_csv(
                os.path.join(
                    self.postprocess_output_path,
                    "national_generation_and_capacity",
                    f"national_generation_monthly_twh_{model}_{c.lower()}_{y}.csv"
                )
            )

            # annual generation has to be done only once
            # annual in Twh
            df_annual = df_out.sum(axis=0)
            df_annual = df_annual / 1000

            # write to dataframe in dictionary
            annual_gen_dfs[model][c][y] = df_annual
        return
    def process_year(self, year):

        CentIvDirectory = f"CentIv_{year}"

        # CentIV
        # TODO: check if CentIV results exist
        if True:
            # read files
            # all countries
            # load
            fn1 = os.path.join(CentIvDirectory, "DemandOriginal_hourly_ALL.csv")
            load = pd.read_csv(fn1, low_memory=False) # this file is used to get the size of the load and the simulation (8760 or 186 etc.), see definition of count_simulation_timestesp below  
            centiv_loads = get_country_data(load)
            # read relevant files
            # curtailment
            fn6 = os.path.join(CentIvDirectory, "REScurtailmentDistIv_hourly_ALL_LP.csv")
            curtail_ch = pd.read_csv(fn6, low_memory=False)
            curtail_all = get_country_data(curtail_ch)
            # Read DSM and emobility shifting data
            fn_dup = os.path.join(CentIvDirectory, "LoadShiftDSMUp_hourly_ALL_LP.xlsx")
            dsm_up_all = get_country_data(pd.read_excel(fn_dup))
            fn_ddown = os.path.join(CentIvDirectory, "LoadShiftDSMDown_hourly_ALL_LP.xlsx")
            dsm_down_all = get_country_data(pd.read_excel(fn_ddown))
            fn_em_up = os.path.join(CentIvDirectory, "LoadShiftEmobUp_hourly_ALL_LP.xlsx")
            emob_up_all = get_country_data(pd.read_excel(fn_em_up))
            fn_em_down = os.path.join(CentIvDirectory, "LoadShiftEmobDown_hourly_ALL_LP.xlsx")
            emob_down_all = get_country_data(pd.read_excel(fn_em_down))
            fn_hp_up = os.path.join(CentIvDirectory, "LoadShiftHpUp_hourly_ALL_LP.xlsx")
            if os.path.exists(fn_hp_up):
                hp_up = get_country_data(pd.read_excel(fn_hp_up))
            else:
                hp_up = None

            fn_hp_down = os.path.join(CentIvDirectory, "LoadShiftHpDown_hourly_ALL_LP.xlsx")
            if os.path.exists(fn_hp_down):
                hp_down = get_country_data(pd.read_excel(fn_hp_down))
            else:
                hp_down = None
            # consumption
            fn17 = os.path.join(CentIvDirectory, "ConsumptionPerGen_full_ALL_LP.xlsx")
            gencons = pd.read_excel(fn17)
            gencons = gencons.transpose()
            # Loadshed
            fn_loadshed = os.path.join(CentIvDirectory, "LoadShedding_hourly_ALL_LP.xlsx")
            centiv_loadshed = get_country_data(pd.read_excel(fn_loadshed))
            # check if file for LoadShedIndustry exists
            fn_lsi = os.path.join(CentIvDirectory, 'LoadSheddingIndustry_hourly_ALL_LP.csv')
            loadshed_ind = os.path.isfile(fn_lsi)
            # DAC
            # TODO remove condition once p2g2p is integrated
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

            if loadshed_ind:
                df_lsi = pd.read_csv(fn_lsi, low_memory=False)
                df_lsi.columns = df_lsi.iloc[1]
                df_lsi = df_lsi.drop([0, 1])
                df_lsi = df_lsi.set_index(0)
                df_lsi = df_lsi.astype(float)

                # get columns of each country
                countries = ['CH', 'DE', 'AT', 'IT', 'FR']
                lsi_countrydic = {}
                for count in countries:
                    lsi_countrydic[count] = []
                    for col in df_lsi.columns:
                        if count in col:
                            lsi_countrydic[count].append(col)

            # add data to generation / consumption dataframe
            centiv_filepath = os.path.join(CentIvDirectory, "GenerationPerGenType_hourly_CH_LP.csv")
            centiv_gen_CH = pd.read_csv(centiv_filepath, low_memory=False)
            centiv_gen_CH = centiv_gen_CH.drop([0])
            centiv_gen_CH.reset_index(drop=True, inplace=True)

            # Finding the size of the simulation time steps: 
            # Count the number of time steps based on the first data column (time index),
            # ignoring NaNs (used for headers and co.) and repeated zeros (except keeping the first zero, which is a valid time step)
            count_simulation_timestesp =  len(load.iloc[:, 0].dropna().loc[lambda x: (x != 0) | (x.shift(fill_value=1) != 0)])

            centiv_con_CH = pd.DataFrame(index=range(count_simulation_timestesp)) # hard coding 8760 hours was not ideal

            # DSM
            centiv_gen_CH['DSM'] = dsm_down_all['CH']
            centiv_con_CH['DSM'] = dsm_up_all['CH'] * -1

            centiv_gen_CH['EMob Shift'] = emob_down_all['CH']
            centiv_con_CH['EMob Shift'] = emob_up_all['CH'] * -1

            if hp_up is not None:
                centiv_con_CH['HP Shift'] = hp_up['CH']*-1
            if hp_down is not None:
                centiv_gen_CH['HP Shift'] = hp_down['CH']

            # add curtailment to consumption dataframe
            centiv_con_CH['PV_curtail'] = curtail_all['CH']
            if p2g2p:
                # add DAC to consumption dataframe
                dacP2G2PCH = filter_by_country_and_format(dac_data, 'CH')
                if 'P2G2P' in dacP2G2PCH.columns:
                    centiv_con_CH['Methanation'] = dacP2G2PCH['P2G2P'] * -1
                else:
                    centiv_con_CH['Methanation'] = 0

                # add P2G2P power consumption to consumption df
                p2g2p_con_CH = filter_by_country_and_format(p2g2p_con_data, 'CH')
                if 'P2G2P' in p2g2p_con_CH.columns:
                    centiv_con_CH['P2G2P'] = p2g2p_con_CH['P2G2P'] * -1
                else:
                    centiv_con_CH['P2G2P'] = 0

                # add P2G2P power production to production df
                p2g2p_gen_CH = filter_by_country_and_format(p2g2p_gen_data, 'CH')

                if 'P2G2P' in p2g2p_gen_CH.columns:
                    centiv_gen_CH['P2G2P'] = p2g2p_gen_CH['P2G2P']
                else:
                    centiv_gen_CH['P2G2P'] = 0

            # storage technologies
            # battery
            gencons_CH = self.get_gencons(gencons[gencons[0] == "CH"])
            if "Battery-TSO" in gencons_CH:
                batt_CH = gencons_CH["Battery-TSO"]
                batt_CH_com = batt_CH.sum() if isinstance(batt_CH, pd.Series) else batt_CH.sum(axis=1) # it is a Series if there is only one plant of type Battery-TSO (e.g., in single electric node case)
                centiv_con_CH["Battery-TSO"] = batt_CH_com
            else:
                centiv_con_CH["Battery-TSO"] = 0

            if "Battery-DSO" in gencons_CH:
                batt_CH = gencons_CH["Battery-DSO"]
                batt_CH_com = batt_CH.sum() if isinstance(batt_CH, pd.Series) else batt_CH.sum(axis=1) # it is a Series if there is only one plant of type Battery-DSO (e.g., in single electric node case)
                centiv_con_CH["Battery-DSO"] = batt_CH_com
            else:
                centiv_con_CH["Battery-DSO"] = 0

            # add DAC to consumption dataframe
            if 'DAC' in gencons_CH:
                centiv_con_CH['DAC'] = gencons_CH['DAC'].sum() if isinstance(gencons_CH['DAC'], pd.Series) else gencons_CH['DAC'].sum(axis=1) # it is a Series if there is only one plant of type DAC (e.g., in single electric node case)
            else:
                centiv_con_CH['DAC'] = 0

            # CH specific Import & Export
            fn3 = os.path.join(CentIvDirectory, "CH_exports.csv")
            fn4 = os.path.join(CentIvDirectory, "CH_imports.csv")

            centiv_exp_CH = pd.read_csv(fn3, low_memory=False)
            centiv_imp_CH = pd.read_csv(fn4, low_memory=False)

            centiv_exp_CH.columns = ['Time', 'CH']
            centiv_imp_CH.columns = ['Time', 'CH']

            centiv_gen_CH['Imports'] = centiv_imp_CH['CH']
            centiv_con_CH['Exports'] = centiv_exp_CH['CH']

            # hydro pump
            fn5 = os.path.join(CentIvDirectory, "PumpConsumption_hourly_CH_LP.xlsx")
            pumpcon_ch = pd.read_excel(fn5)
            pumpcon_ch = pumpcon_ch.drop(["Unnamed: 0"], axis=1)
            pumpcon_CH_sum = pumpcon_ch.sum(axis=1)

            # TODO: check in what file pump-open & pump-closed are written (not used at the moment)
            centiv_con_CH["Pump-Open"] = pumpcon_CH_sum * -1
            centiv_con_CH["Pump-Closed"] = 0

            # load shedding
            centiv_gen_CH['Load_Shed'] = centiv_loadshed['CH']
            if loadshed_ind:
                centiv_gen_CH['Load_Shed-Ind'] =  (
                    df_lsi[lsi_countrydic['CH']].sum() 
                    if isinstance(df_lsi[lsi_countrydic['CH']], pd.Series) 
                    else df_lsi[lsi_countrydic['CH']].sum(axis=1)
                ) # it is a Series if there is only one plant of type LoadShedIndustry (e.g., in single electric node case)
            else:
                centiv_gen_CH['Load_Shed-Ind'] = 0

            self.create_gendata(centiv_gen_CH, centiv_con_CH, centiv_loads, 'CH', year, 'c')

            # compute generation data from the neighbour countries
            fn16 = os.path.join(CentIvDirectory, "PumpConsumption_hourly_Neighbours_LP.xlsx")

            pump_neigh = pd.read_excel(fn16)
            pump_neigh = pump_neigh.T.reset_index().T.reset_index(drop=True)
            pump_neigh = pump_neigh.drop([0])
            pump_neigh = pump_neigh.set_index(0)

            pump_neigh_dic = { #TODO: there are too many hard coded countries, this should be generalized
                "DE": pump_neigh.iloc[:, 0] * -1,
                "FR": pump_neigh.iloc[:, 1] * -1,
                "IT": pump_neigh.iloc[:, 2] * -1,
                "AT": pump_neigh.iloc[:, 3] * -1
            }

            # loop to extract data from all neighbouring countries
            for country in ['DE', 'AT', 'IT', 'FR']:
                # read CentIV results
                gen_filepath = os.path.join(CentIvDirectory, f"GenerationPerGenType_hourly_{country}_LP.csv")
                centiv_gen = pd.read_csv(gen_filepath, low_memory=False)
                centiv_gen = centiv_gen.drop([0])
                centiv_gen.reset_index(drop=True, inplace=True)

                # get consumption data of each technology
                centiv_con = self.get_gencons(gencons[gencons[0] == country])
                centiv_con.reset_index(drop=True, inplace=True)

                # add pump to consumption df
                centiv_con['Pump'] = pump_neigh_dic[country]

                centiv_gen['DSM'] = dsm_down_all[country]
                centiv_con['DSM'] = dsm_up_all[country] * -1

                centiv_gen['EMob Shift'] = emob_down_all[country]
                centiv_con['EMob Shift'] = emob_up_all[country] * -1

                # curtailment
                centiv_con['PV_curtail'] = curtail_all[country]

                # load shedding
                centiv_gen['Load_Shed'] = centiv_loadshed[country]
                if loadshed_ind:
                    centiv_gen['Load_Shed-Ind'] = ( # it is a Series if there is only one asset (node?) of type LoadShedIndustry (e.g., in single electric node case)
                        df_lsi[lsi_countrydic[country]].sum() 
                        if isinstance(df_lsi[lsi_countrydic[country]], pd.Series) 
                        else df_lsi[lsi_countrydic[country]].sum(axis=1)
                    )
                else:
                    centiv_gen['Load_Shed-Ind'] = 0

                # add DAC to consumption dataframe
                if 'DAC' in centiv_con:
                    centiv_con['DAC'] = ( # it is a Series if there is only one plant of type DAC (e.g., in single electric node case)
                        centiv_con['DAC'].sum() 
                        if isinstance(centiv_con['DAC'], pd.Series) 
                        else centiv_con['DAC'].sum(axis=1)
                    )
                else:
                    centiv_con['DAC'] = 0

                # add DAC for methanation to consumption dataframe
                # methanation is the load used for the methanation
                # DAC is the load only for CO2 used for methanation
                if p2g2p:
                    dacP2G2PCH = filter_by_country_and_format(dac_data, country)
                    if 'DAC' in dacP2G2PCH.columns:
                        centiv_con['Methanation'] = dacP2G2PCH['DAC'] * -1
                    else:
                        centiv_con['Methanation'] = 0

                    # add P2G2P to generation and consumption dataframes
                    p2g2p_con = filter_by_country_and_format(p2g2p_con_data, country)
                    if 'P2G2P' in p2g2p_con.columns:
                        centiv_con['P2G2P'] = p2g2p_con['P2G2P'] * -1
                    else:
                        centiv_con['P2G2P'] = 0

                    p2g2p_gen = filter_by_country_and_format(p2g2p_gen_data, country)
                    if 'P2G2P' in p2g2p_gen.columns:
                        centiv_gen['P2G2P'] = p2g2p_gen['P2G2P']
                    else:
                        centiv_gen['P2G2P'] = 0

                # create data
                self.create_gendata(centiv_gen, centiv_con, centiv_loads, country, year, "c")          
    def write_annual_generation_values(self):
        # write annual generation values to file
        for model in annual_gen_dfs:
            # generation for CentIV and FlexEco
            for country in ['DE', 'AT', 'IT', 'FR']:
                if not (country == 'CH' and model == 'c'):
                    # annual generation per generator type
                    # rename index
                    # additional columns for Switzerland
                    extra_columns = ['Exports', 'Imports', 'Imports (Net)', 'Load (Net)', 'Load (Total)']
                    group_n_rename(
                        annual_gen_dfs[model][country],
                        transposed=True,
                        index_name='Row',
                        add_columns=extra_columns
                    ).to_csv(
                        os.path.join(
                            self.postprocess_output_path,
                            "national_generation_and_capacity",
                            f"national_generation_annual_twh_{model}_{country.lower()}.csv"
                        )
                    )
    def combine_generation_data(self):
        for model in self.models:
            annual_gen_CH = pd.DataFrame([])
            pv_inv = {}
            bat_inv = {}
            curtailments_dic = {}
            extra_columns = ['Exports', 'Imports', 'Imports (Net)', 'Load (Net)', 'Load (Total)']

            for v, i in enumerate(self.centiv_years):
                gen = self.load_data(i, model)
                if gen is not None:
                    if i == 2020 or not self.distiv:
                        pv_inv[i] = 0
                        bat_inv[i] = 0
                        dsm_up_df = pd.DataFrame(np.zeros((1, 8760))).transpose()
                        dsm_down_df = pd.DataFrame(np.zeros((1, 8760))).transpose()
                        curtailments_dic[i] = pd.Series(0, index=range(8760))
                    else:
                        contents = self.load_distiv_data(i)
                        if contents is not None:
                            PV_df, curtailment, PV_bat_char_df, PV_bat_dis_df, pv_inv[i], bat_inv[i], dsm_up_df, dsm_down_df = self.process_distiv_data(contents)
                            curtailments_dic[i] = curtailment
                        else:
                            # Load DistIV Results
                            fn = f"DistIv_{i}.mat"
                            data = h5py.File(fn)
                            contents = data['resDistIv']

                            # get data from DistIv (PV generation)
                            # is the hourly generation for each of the generator types (9 currently) and by municipality
                            PV = contents['plotting']["p_gen_fullyear"][
                                ()]  # includes hourly generation per municipality and per the 9 different generator types
                            # keep only the first four gen types, which are the PV gen types
                            PV2 = PV[:, :, :4]
                            # now sum across all the four PV types
                            PV3 = np.sum(PV2, axis=2)
                            # sum across all municipalities to get the total hourly PV generation hourly
                            PV4 = np.sum(PV3, axis=0)  # hourly PV generation in kWh
                            # put the hourly PV gen into a dataframe
                            PV4_df = pd.DataFrame(PV4)
                            PV4_df = PV4_df / 1000  # conversion in MWh
                            PV4_df = PV4_df.squeeze()

                            # get data from DistIv (PV curtailments)
                            # is the hourly curtailment by municipality
                            PV_cur = contents['plotting']["p_curt_fullyear"][()]  # includes hourly curtailment per municipality
                            # sum across all municipalities to get the total hourly PV generation curtailment
                            PV_cur2 = np.sum(PV_cur, axis=0)
                            # put the hourly PV curtailment into a dataframe
                            PV_cur2_df = pd.DataFrame(PV_cur2)  # hourly PV curtailments in kWh
                            # Remove the default column name to match the same structure as PV4_df
                            # PV_cur3_df = PV_cur2_df.sum(axis=1)
                            PV_cur2_df = PV_cur2_df / 1000  # conversion in MWh
                            curtailment = PV_cur2_df.squeeze()

                            # combine the hourly PV generation and curtailments
                            PV_df = PV4_df - curtailment

                            # add curtailments to dataframe
                            curtailments_dic[i] = curtailment

                            # get data from DistIv (PV-battery)
                            PVb = PV[:, :, 6]  # keep only the 6th column, which is the PV-battery charging and discharging
                            # put the PV-batt charge/discharge into a dataframe
                            PVb_df = pd.DataFrame(PVb)
                            # separate the charging values (-)
                            PVb_c_df = PVb_df[PVb_df < 0]  # Battery charging dataframe
                            # now sum across all the municipalities
                            PVb_c_df_h = PVb_c_df.sum(axis=0)  # hourly battery charging in kWh
                            PV_bat_char_df = PVb_c_df_h / 1000  # conversion in MWh
                            PV_bat_char_df = PV_bat_char_df.squeeze()

                            # separate the discharging values (+)
                            PVb_d_df = PVb_df[PVb_df >= 0]  # Battery discharging dataframe
                            # now sum across all the municipalities
                            PVb_d_df_h = PVb_d_df.sum(axis=0)  # hourly battery discharging in kWh
                            PV_bat_dis_df = PVb_d_df_h / 1000  # conversion in MWh
                            PV_bat_dis_df = PV_bat_dis_df.squeeze()

                            # Get DistIv battery and PV investments
                            # PV capacity from DistIv
                            Cap_new = contents['capacity']['invest']["Invest_capacity_sum_MW"][()]  # in MW
                            pv_inv[i] = sum(Cap_new[0][0:4])

                            # Battery Capacity from DistIv
                            bat_inv[i] = np.array(Cap_new[0][6])  # in MW

                            # get p_max and e_max for an individual battery from DistIv data
                            # the ratio p_max / e_max is defined as the C-rate of a battery
                            # to convert power capacity to energy capacity
                            p_max = contents['data']['Unit']['units']['Pmax'][0][6]
                            # TODO: read e_max directly from DistIv
                            e_max = 13.5
                            battery_parameters = {
                                'bat_inv': [bat_inv],
                                'p_max': [p_max],
                                'e_max': [e_max]
                            }
                            df = pd.DataFrame(battery_parameters)
                            df.to_csv('battery_parameters.csv', index=False)


                            # DSM from DistIv
                            dsm_up = contents['interface']["Demand_shiftedup_regional_hourly_MWh_fullyear"][()]
                            dsm_up_df = pd.DataFrame(dsm_up)
                            dsm_up_df = dsm_up_df.sum()
                            dsm_up_df = dsm_up_df * (-1)

                            dsm_down = contents['interface']["Demand_shifteddown_regional_hourly_MWh_fullyear"][()]
                            dsm_down_df = pd.DataFrame(dsm_down)
                            dsm_down_df = dsm_down_df.sum()
                            dsm_down_df = dsm_down_df

                    gen = gen.astype(float)
                    cen_dis_combined = gen.copy()

                    if i != 2020 and self.distiv:
                        cen_dis_combined["PV-roof"] = gen['PV-roof'] + PV_df
                        gen["PV-Total"] = gen["PV-Total"] + PV_df
                        cen_dis_combined["Battery-TSO"] = gen['Battery-TSO'] + PV_bat_dis_df
                        cen_dis_combined["Battery-TSO (Load)"] = gen['Battery-TSO (Load)'] + PV_bat_char_df

                    cen_dis_combined["DSM (Up)"] = gen['DSM (Up)'] + dsm_up_df[0]
                    cen_dis_combined["DSM"] = gen['DSM'] + dsm_up_df[0]

                    cen_dis_combined.drop('Hour', axis=1, inplace=True)
                    cen_dis_combined.index.name = 'Hour'
                    #cen_dis_combined.to_csv(f'Gen_CH_{i}_{model}.csv')

                    cen_dis_combined["Exports"] = gen["Exports"]
                    cen_dis_combined["Imports"] = gen["Imports"]
                    cen_dis_combined["Imports (Net)"] = gen["Imports"] + gen["Exports"]
                    cen_dis_combined["Load (Net)"] = gen["Load (Total)"] - gen["Wind-Total"] - gen["PV-Total"]
                    cen_dis_combined["Load (Total)"] = gen["Load (Total)"]
                    cen_dis_combined = cen_dis_combined/1000
                    group_n_rename(
                        cen_dis_combined,
                        index_name="Hour",
                        add_columns=extra_columns
                    ).to_csv(
                        os.path.join(
                            self.postprocess_output_path,
                            "national_generation_and_capacity",
                            f"national_generation_hourly_gwh_{model}_ch_{i}.csv"
                        )
                    )

                    cen_dis_m = cen_dis_combined / 1000
                    cen_dis_m['date'] = pd.date_range(start='1/1/2018', periods=len(cen_dis_m), freq='H')
                    cen_dis_m1 = cen_dis_m.resample('M', on='date').sum().reset_index()
                    cen_dis_m1.index = cen_dis_m1.index + 1
                    cen_dis_m1["Month"] = range(1, 1 + len(cen_dis_m1)) # to be able to run the model partially, e.g. for 168 hours, it is needed to define this flexibly 
                    cen_dis_m1 = cen_dis_m1.drop(["date"], axis=1)
                    group_n_rename(
                        cen_dis_m1,
                        add_columns=extra_columns,
                        index_name="Month"
                    ).to_csv(
                        os.path.join(
                            self.postprocess_output_path,
                            "national_generation_and_capacity",
                            f"national_generation_monthly_twh_{model}_ch_{i}.csv"
                        )
                    )
                    
                    cen_dis_annual = cen_dis_m1.sum(axis=0)
                    annual_gen_CH[i] = cen_dis_annual

                    group_n_rename(
                        annual_gen_CH,
                        transposed=True,
                        index_name='Row',
                        add_columns=extra_columns
                    ).to_csv(
                        os.path.join(
                            self.postprocess_output_path,
                            "national_generation_and_capacity",
                            f"national_generation_annual_twh_{model}_ch.csv"
                        )
                    )
                    
                    annual_gen_CH_output = pd.read_csv(
                        os.path.join(
                            self.postprocess_output_path,
                            "national_generation_and_capacity",
                            f"national_generation_annual_twh_{model}_ch.csv"
                        ),
                        index_col=0
                    )

                    Demand = annual_gen_CH_output.loc['Load (Total)']
                    Exports = annual_gen_CH_output.loc['Exports']
                    Imports = annual_gen_CH_output.loc['Imports']
                    Load_Shed = annual_gen_CH_output.loc['Load Shed']
                    Generation = annual_gen_CH_output.drop(['Load (Total)', 'Exports', 'Imports', 'Load Shed','Imports (Net)','Load (Net)'], axis=0).sum()
                    value = Generation -Demand + Imports + Exports + Load_Shed
                    print("")
                    print("---------Swiss Annual Energy Balance ----------------")
                    print(f"Demand: {Demand.values[0]}")
                    print(f"Generation: {Generation.values[0]}")
                    print(f"Exports: {Exports.values[0]}")
                    print(f"Imports: {Imports.values[0]}")
                    print(f"Load Shed: {Load_Shed.values[0]}")
                    print(f"Demand - Generation + Exports + Imports - Load Shed = {value.values[0]}")
                    print("---------------------------------------------------------")
                    print("")

def main(simulation: str):
    os.path.abspath(os.curdir)
    global postprocess_output_directory
    postprocess_output_directory = "postprocess"
    create_dir(
        os.path.join(
            postprocess_output_directory,
            "national_generation_and_capacity"
        )
    )

    # read generator file
    global generators
    generators = read_generator_file()

    global country_names
    country_names = {
        "CH": "Switzerland",
        "DE": "Germany",
        "IT": "Italy",
        "AT": "Austria",
        "FR": "France"
    }

    # dataframe declaration for annual values
    global annual_gen_dfs
    annual_gen_dfs = {
        "c": {
            'DE': pd.DataFrame([]),
            'IT': pd.DataFrame([]),
            'AT': pd.DataFrame([]),
            'FR': pd.DataFrame([])
        }
    }
    # get CentIV years
    global centiv_years
    centiv_years = get_years_simulated_by_centiv(Path())
    # check if DistIc results exist:
    fn1 = "DistIv_2030.mat"
    fn2 = "DistIv_2040.mat"
    fn3 = "DistIv_2050.mat"

    if os.path.exists(fn1) or os.path.exists(fn2) or os.path.exists(fn3):
        DistIV = True
    else:
        DistIV = False

    # active models
    # CentIV: c
    # FlexEco: e
    models = ['c']

    generation = Generation(
        postprocess_output_directory,
        simulation,
        models,
        centiv_years,
        distiv=DistIV
    )

    for year in centiv_years:
        generation.process_year(year)
        generation.write_annual_generation_values()

    generation.combine_generation_data()


if __name__ == "__main__":
    argp = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    argp.add_argument(
        "--simuname",
        type=str,
        help="Name of MySQL database results",
    )
    args = argp.parse_args()
    main(simulation=args.simuname)
