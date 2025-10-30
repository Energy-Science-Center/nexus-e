import os
import pandas as pd
import numpy as np
import argparse
import scipy.io

os.path.abspath(os.curdir)
parentDirectory = os.getcwd()

argp = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

argp.add_argument("--simuname", type=str, help="Name of MySQL database results", default='nexus_scenario_base_Jan20_T0019_JP_NoRenewTarget_20-Jan-2022_00-19')

args = argp.parse_args()
simu_name = args.simuname


pv_list = []
bat_list = []


def create_dir(dir):
  if not os.path.exists(dir):
    os.makedirs(dir)
    print("Created Directory : ", dir)
  else:
    print("Directory already existed : ", dir)
  return dir

dir_new = (f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity")
create_dir(dir_new)

fnpath1 = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_2050")
fnpath2 = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_2040")

# if you want results for 2018 callibration year just add 2018 in the list_years and uncomment code for 2018

if os.path.exists(fnpath1):
    list_years = [2020,2030,2040,2050]

elif os.path.exists(fnpath2):
    list_years = [2020, 2030, 2040]
else:
    list_years = [2020, 2030]

CH_annual_gen = pd.DataFrame([])

for i in list_years:

    if i == 2018:
        eMarkDirectory = (f"{parentDirectory}/2018_calibration/eMark_2018.mat")
    else:
        eMarkDirectory = (f"{parentDirectory}/../../Results/{simu_name}/eMark_{i}.mat")


    data = scipy.io.loadmat(eMarkDirectory)
    contents= data['resEMark']

    # Common data for all countries
    # Pump and battery
    Charging = contents[0, 0]['AllM'][0, 0]["GenType_GenCharging_sum_CH"]
    Charging_df = pd.DataFrame(Charging)


    # Load
    load = contents[0, 0]['AllM'][0, 0]["ZoneLoadNoPump"]
    load_df = pd.DataFrame(load)
    load_df = load_df.transpose()

    # Export
    export_a = contents[0, 0]['AllM'][0, 0]["ZoneExport"]
    export_df = pd.DataFrame(export_a)
    export_df = export_df.transpose()

    # Import
    import_a = contents[0, 0]['AllM'][0, 0]["ZoneImport"]
    import_df = pd.DataFrame(import_a)
    import_df = import_df.transpose()

    # Names and order of generators
    gen_names = contents[0, 0]['AllM'][0, 0]["GenType_Names"]
    gen_names_df = pd.DataFrame(gen_names)

    # CH pump and Batt profiles
    CH_con_df = pd.concat([gen_names_df, Charging_df], axis=1)
    CH_con_df.columns = pd.RangeIndex(CH_con_df.columns.size)
    CH_con_df[0] = CH_con_df[0].str.get(0)

    CH_con_df = CH_con_df.fillna(0)
    CH_con_df = CH_con_df.transpose()
    
    CH_con_df.columns = CH_con_df.iloc[0]
    CH_con_df= CH_con_df.drop([0])

    #########  Load ABM Results ################################################
    if i == 2020:


        # Historical year so ABM does not run
        PV = np.zeros((1,8760))
        PV_df = pd.DataFrame(PV)
        PV_df = PV_df.transpose()

        PV_bat_char = np.zeros((1,8760))
        PV_bat_char_df = pd.DataFrame(PV_bat_char)
        PV_bat_char_df = PV_bat_char_df.transpose()

        PV_bat_dis = np.zeros((1,8760))
        PV_bat_dis_df = pd.DataFrame(PV_bat_dis)
        PV_bat_dis_df = PV_bat_dis_df.transpose()


        pvcap_v = 0
        pv_list.append(pvcap_v)

        # Battery Capacity from ABM

        pvbat_v = 0
        bat_list.append(pvbat_v)

    else:
        fn = (f"{parentDirectory}/../../Results/{simu_name}/ABM_{i}.mat")
        data = scipy.io.loadmat(fn)
        contents2 = data['ABMtoPostprocess']

        # Get ABM PV generation data
        PV = contents2[0, 0]['PV_gen_profiles']
        PV_df = pd.DataFrame(PV)


        PV_bat_char = contents2[0, 0]['batt_charge_profiles']
        PV_bat_char_df = pd.DataFrame(PV_bat_char)
        PV_bat_char_df = PV_bat_char_df.transpose()

        PV_bat_dis = contents2[0, 0]['batt_discharge_profiles']
        PV_bat_dis_df = pd.DataFrame(PV_bat_dis)
        PV_bat_dis_df = PV_bat_dis_df.transpose()

        # Get  ABM battery and PV capacity installed data
        fn_abm = (f"{parentDirectory}/../../Results/{simu_name}/ABM_{i}/Cumulativecapacity.csv")
        abm = pd.read_csv(fn_abm)

        # PV capacity from ABM
        pvcap = (abm["cum_PV_adoption_KW"].tail(1))  # in MW
        pvcap_v = np.array(pvcap)[0]
        pv_list.append(pvcap_v)

        # Battery Capacity from ABM

        pvbat = (abm["cum_PVBS_adoption_KW"].tail(1)) * (5 / (13.5))  # in MW
        pvbat_v = np.array(pvbat)[0]
        bat_list.append(pvbat_v)

    #########  Combine files for CH ################################################

    # Gentpe order from 2020 : ROR, Nuclear, Lignite,Coal,Gas CC Gas SC, Biomass, Oil, WindOn, WindOff, PV, Oil DNS, Dam, Pump, Batt DSO , BattTSO, DSM
    CH_gen = contents[0, 0]["AllM"][0, 0]["GenType_GenInjections_nopump_sum_CH"]
    CH_gen_df_1 = pd.DataFrame(CH_gen)

    CH_gen_df = pd.concat([gen_names_df,CH_gen_df_1],axis =1)
    CH_gen_df.columns = pd.RangeIndex(CH_gen_df.columns.size)
    CH_gen_df[0] = CH_gen_df[0].str.get(0)

    CH_gen_df = CH_gen_df.fillna(0)
    CH_gendata_df = pd.DataFrame(CH_gen_df)

    CH_gendata_df[0] = CH_gendata_df[0].str.replace("GasCC", "Gas (combined cycle)")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("GasSC", "Gas (simple cycle)")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("BattTSO", "Battery (Generation)")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("BattDSO", "Battery (Generation)")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("RoR", "Run of river")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("WindOn", "Wind Onshore")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("WindOff", "Wind Offshore")
    CH_gendata_df[0] = CH_gendata_df[0].str.replace("Pump", "Pump (Generation)")
    #CH_gendata_df[0] = CH_gendata_df[0].str.replace("DSM", "DSM (Down)")

    CH_zone = 1
    #

    CH_gendata_df = CH_gendata_df.transpose()
    CH_gendata_df.columns = CH_gendata_df.iloc[0]
    CH_gendata_df= CH_gendata_df.drop([0])
    CH_gendata_df = CH_gendata_df.reset_index()
    CH_gendata_df = CH_gendata_df.drop(["index"],axis=1)
    CH_gendata_df["Pump (Load)"] = CH_con_df["Pump"]


    # CH_gendata_df["Import"] = import_df[CH_zone]
    # CH_gendata_df["Export"] = export_df[CH_zone] * -1

    # Placeholder for Battery(Load)
    CH_gendata_df["Battery (Load)"] = CH_con_df["BattDSO"] +CH_con_df["BattTSO"]
    CH_gendata_df["DSM (Up)"] = 0
    CH_gendata_df["DSM (Down)"] = 0
    CH_gendata_df["Load Shed"] = 0
    CH_gendata_df["Export"] = export_df[0] * -1
    CH_gendata_df["Import"] = import_df[0]
    CH_gendata_df["Import (Net)"] = CH_gendata_df["Export"].astype(float) + CH_gendata_df["Import"].astype(float)

    CH_gendata_df = CH_gendata_df.fillna(0)

    # CH_gendata_df["Import (Net)"] = import_df[CH_zone] - export_df[CH_zone]
    CH_gendata_df2 = CH_gendata_df.groupby(lambda x: x, axis=1).sum()

    if i ==2020:

        CH_gendata_df2["Battery (Load)"] = (CH_gendata_df2["Battery (Load)"].astype(float) )*-1
        CH_gendata_df2["Battery (Generation)"] = CH_gendata_df2["Battery (Generation)"].astype(float)
        CH_gendata_df2["PV"] = CH_gendata_df2["PV"].astype(float)

    else:
        # Combine with ABM results
        CH_gendata_df2["Battery (Load)"] = (CH_gendata_df2["Battery (Load)"].astype(float) + PV_bat_char_df[0].astype(float))*-1
        CH_gendata_df2["Battery (Generation)"] = CH_gendata_df2["Battery (Generation)"].astype(float) + PV_bat_dis_df[0].astype(float)
        CH_gendata_df2["PV"] = CH_gendata_df2["PV"].astype(float) + PV_df[0].astype(float)


    CH_gendata_df2["Load (Total)"] = load_df[CH_zone] + CH_gendata_df2["PV"] - CH_gendata_df2["Battery (Generation)"] +(CH_gendata_df2["Battery (Load)"]*-1)

    CH_gendata_df2["Load (Net)"] = CH_gendata_df2["Load (Total)"] - CH_gendata_df2["Wind Onshore"] - CH_gendata_df2["Wind Offshore"] -CH_gendata_df2["PV"]

    CH_gendata_df_order = CH_gendata_df2[
        ["Nuclear", "Lignite","Coal","Gas (combined cycle)","Gas (simple cycle)","Oil", "Run of river", "Dam", "Biomass", "Wind Onshore", "Wind Offshore",
         "PV", "Pump (Generation)", "Battery (Generation)", "DSM (Up)", "Battery (Load)", "DSM (Down)", "Pump (Load)","Load (Total)", "Load (Net)", "Load Shed", "Export", "Import", "Import (Net)"]]

    CH_gendata_df_order = CH_gendata_df_order / 1000  # in GW
    CH_gendata_df_order.index.name = "Hour"
    CH_gendata_df_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_e_CH_{i}.csv")

    #### monthlygeneration #########
    CH_df_m = CH_gendata_df_order / 1000  # in TWh
    CH_df_m['date'] = pd.date_range(start='1/1/2018', periods=len(CH_df_m), freq='H')
    CH_df_m1 = CH_df_m.resample('M', on='date').sum()
    CH_df_m1 = CH_df_m1.reset_index()
    CH_df_m1["Month"] = range(1, 1 + len(CH_df_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    CH_df_m1 = CH_df_m1.drop(["date"], axis=1)
    CH_df_m1 = CH_df_m1.set_index("Month")
    CH_df_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_e_CH_{i}.csv")

    ### annual in Twh ######
    CH_df_annual = CH_gendata_df_order.sum(axis=0)
    CH_df_annual = CH_df_annual / 1000

    CH_annual_gen[i] =  CH_df_annual

    # # Calculate Capacity
    #
    # geninfotable = contents[0, 0]["Scenario"][0, 0]["grid"][0, 0]["genInfoTable"][0, 0]
    # geninfotable_pd = pd.DataFrame(geninfotable)




CH_annual_gen.index.name = "Row"
CH_annual_gen.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_e_CH.csv")

