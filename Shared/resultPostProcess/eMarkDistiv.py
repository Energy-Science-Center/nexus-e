import os
import pandas as pd
import numpy as np
import argparse
import scipy.io
import h5py

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
  return dir

dir_new = (f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity")
create_dir(dir_new)

fnpath1 = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_2050")
fnpath2 = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_2040")
fnpath3 = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_2030")
fnpath4 = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_2020")

# if you want results for 2018 callibration year just add 2018 in the list_years

if os.path.exists(fnpath1) and os.path.exists(fnpath2) and os.path.exists(fnpath3) and os.path.exists(fnpath4):
    list_years = [2020, 2030, 2040, 2050]

elif os.path.exists(fnpath2) and os.path.exists(fnpath3) and os.path.exists(fnpath4):
    list_years = [2020, 2030, 2040]

elif os.path.exists(fnpath3) and os.path.exists(fnpath4):
    list_years = [2020, 2030]

elif os.path.exists(fnpath1):
    list_years = [2050]

elif os.path.exists(fnpath2):
    list_years = [2040]

elif os.path.exists(fnpath3):
    list_years = [2030]

else:
    list_years = [2020]

CH_annual_gen = pd.DataFrame([])

for i in list_years:

    if i == 2018:
        eMarkDirectory = (f"{parentDirectory}/2018_calibration/eMark_2018.mat")
    else:
        eMarkDirectory = (f"{parentDirectory}/../../Results/{simu_name}/eMark_{i}.mat")


    try:
        data = scipy.io.loadmat(eMarkDirectory)
    except Exception as e:
        print(f"No eMark file found for {i}")
        exit(1)
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

    # to have CentIv load curtailment
    CentIvDirectory = (f"{parentDirectory}/../../Results/{simu_name}/CentIv_{i}")
    fn_c = os.path.join(CentIvDirectory, "REScurtailmentDistIv_hourly_ALL_LP.csv")
    Curtail_CH = pd.read_csv(fn_c)

    # Curtail calculation for CH and neighbouring countries
    Curtail_CH = Curtail_CH.transpose()
    Curtail_CH_df = Curtail_CH[
        (Curtail_CH[0] != "DE") & (Curtail_CH[0] != "DE_X") & (Curtail_CH[0] != "FR") & (Curtail_CH[0] != "FR_X") &
        (Curtail_CH[0] != "IT") & (Curtail_CH[0] != "IT_X") & (Curtail_CH[0] != "AT") & (Curtail_CH[0] != "AT_X")]
    Curtail_CH_df= Curtail_CH_df.drop([0, 1], axis=1)
    Curtail_CH_df = Curtail_CH_df.drop(["Unnamed: 0"])
    Curtail_CH_df = Curtail_CH_df.astype(float)
    Curtail_CH_df_sum = Curtail_CH_df.sum()
    Curtail_CH_df_sum = Curtail_CH_df_sum.reset_index()
    Curtail_CH_df_sum = Curtail_CH_df_sum.drop(["index"], axis=1)





    #########  Load DistIv Results ################################################
    if i == 2020:


        # Historical year so DistIv does not run
        PV = np.zeros((1,8760))
        PV_df = pd.DataFrame(PV)
        PV_df = PV_df.transpose()

        PV_bat_char = np.zeros((1,8760))
        PV_bat_char_df = pd.DataFrame(PV_bat_char)
        PV_bat_char_df = PV_bat_char_df.transpose()

        PV_bat_dis = np.zeros((1,8760))
        PV_bat_dis_df = pd.DataFrame(PV_bat_dis)
        PV_bat_dis_df = PV_bat_dis_df.transpose()
        
        dsm_up = np.zeros((1,8760))
        dsm_up_df = pd.DataFrame(dsm_up)
        dsm_up_df = dsm_up_df.transpose()
        
        dsm_down = np.zeros((1,8760))
        dsm_down_df = pd.DataFrame(dsm_down)
        dsm_down_df = dsm_down_df.transpose()



    else:
        fn = (f"{parentDirectory}/../../Results/{simu_name}/DistIv_{i}.mat")
        data = h5py.File(fn)
        contents = data['resDistIv']

        # Get DistIv PV and Battery generation data
        PV = contents['plotting']["PV_gen_fullyear"][()]
        PV_cur = contents['plotting']["PV_curt_fullyear"][()]
        PV_net = PV -PV_cur
        PV_df2 = pd.DataFrame(PV_net)
        PV_df = PV_df2.sum()
        PV_df = PV_df/1000 #conversion in gwh


        PV_bat_char = contents['plotting']["pb_consum_fullyear"][()]
        PV_bat_char_df = pd.DataFrame(PV_bat_char)
        PV_bat_char_df = PV_bat_char_df.sum()
        PV_bat_char_df = PV_bat_char_df / 1000  # conversion in gwh

        PV_bat_dis =contents['plotting']["pb_gen_fullyear"][()]
        PV_bat_dis_df = pd.DataFrame(PV_bat_dis)
        PV_bat_dis_df = PV_bat_dis_df.sum()
        PV_bat_dis_df = PV_bat_dis_df / 1000  # conversion in gwh
        
        dsm_up =contents['interface']["Demand_shiftedup_regional_hourly_MWh_fullyear"][()]
        dsm_up_df = pd.DataFrame(dsm_up)
        dsm_up_df = dsm_up_df.sum()
        dsm_up_df = dsm_up_df*(-1) / 1000  # conversion in gwh
        
        dsm_down =contents['interface']["Demand_shifteddown_regional_hourly_MWh_fullyear"][()]
        dsm_down_df = pd.DataFrame(dsm_down)
        dsm_down_df = dsm_down_df.sum()
        dsm_down_df = dsm_down_df / 1000  # conversion in gwh



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
    CH_gendata_df["DSM (Up)"] = dsm_up_df
    CH_gendata_df["DSM (Down)"] = dsm_down_df
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
        # Combine with DistIv results
        CH_gendata_df2["Battery (Load)"] = (CH_gendata_df2["Battery (Load)"].astype(float) + PV_bat_char_df[0].astype(float))*-1
        CH_gendata_df2["Battery (Generation)"] = CH_gendata_df2["Battery (Generation)"].astype(float) + PV_bat_dis_df[0].astype(float)
        CH_gendata_df2["PV"] = CH_gendata_df2["PV"].astype(float) + PV_df[0].astype(float) + Curtail_CH_df_sum.astype(float)


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

