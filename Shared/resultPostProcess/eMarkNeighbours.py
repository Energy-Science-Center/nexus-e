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
argp.add_argument("--DBname", type=str, help="Name of MySQL database", default='nexuse_schema2_disagg_netzero_ga_hidns') #nexuse_schema2_disagg_ch2040
args = argp.parse_args()
simu_name = args.simuname
database = args.DBname

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

DE_annual_gen = pd.DataFrame([])
FR_annual_gen = pd.DataFrame([])
IT_annual_gen = pd.DataFrame([])
AT_annual_gen = pd.DataFrame([])

Elecprice_annual = pd.DataFrame([])

for i in list_years:

    if i == 2018:
        eMarkDirectory = (f"{parentDirectory}/2018_calibration/eMark_2018.mat")
    else:
        eMarkDirectory = (f"{parentDirectory}/../../Results/{simu_name}/eMark_{i}.mat")



    if not os.path.exists(eMarkDirectory):
        print(f"No eMark file found for {i}")
        exit(1)
    data = scipy.io.loadmat(eMarkDirectory)
    contents = data['resEMark']

    # Common data for all countries

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

    #########  Combine files for DE ################################################

    # Pump and battery
    Charging_DE = contents[0, 0]['AllM'][0, 0]["GenType_GenCharging_sum_DE"]
    Charging_DE_df = pd.DataFrame(Charging_DE)

    # DE pump and Batt profiles
    DE_con_df = pd.concat([gen_names_df, Charging_DE_df], axis=1)
    DE_con_df.columns = pd.RangeIndex(DE_con_df.columns.size)
    DE_con_df[0] = DE_con_df[0].str.get(0)

    DE_con_df = DE_con_df.fillna(0)
    DE_con_df = DE_con_df.transpose()

    DE_con_df.columns = DE_con_df.iloc[0]
    DE_con_df = DE_con_df.drop([0])

    # Gentpe order from 2020 : ROR, Nuclear, Lignite,Coal,Gas CC Gas SC, Biomass, Oil, WindOn, WindOff, PV, Oil DNS, Dam, Pump, Batt DSO , BattTSO, DSM
    DE_gen = contents[0, 0]['AllM'][0, 0]["GenType_GenInjections_nopump_sum_DE"]
    DE_gen_df_1 = pd.DataFrame(DE_gen)

    DE_gen_df = pd.concat([gen_names_df,DE_gen_df_1],axis =1)
    DE_gen_df.columns = pd.RangeIndex(DE_gen_df.columns.size)
    DE_gen_df[0] = DE_gen_df[0].str.get(0)

    DE_gen_df = DE_gen_df.fillna(0)
    DE_gendata_df = pd.DataFrame(DE_gen_df)

    DE_gendata_df[0] = DE_gendata_df[0].str.replace("GasCC", "Gas (combined cycle)")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("GasSC", "Gas (simple cycle)")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("BattTSO", "Battery (Generation)")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("BattDSO", "Battery (Generation)")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("RoR", "Run of river")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("WindOn", "Wind Onshore")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("WindOff", "Wind Offshore")
    DE_gendata_df[0] = DE_gendata_df[0].str.replace("Pump", "Pump (Generation)")
    #DE_gendata_df[0] = DE_gendata_df[0].str.replace("DSM", "DSM (Down)")

    DE_zone = 0
    #

    DE_gendata_df = DE_gendata_df.transpose()
    DE_gendata_df.columns = DE_gendata_df.iloc[0]
    DE_gendata_df= DE_gendata_df.drop([0])
    DE_gendata_df = DE_gendata_df.reset_index()
    DE_gendata_df = DE_gendata_df.drop(["index"],axis=1)
    DE_gendata_df["Pump (Load)"] = DE_con_df["Pump"]


    DE_gendata_df["Load (Total)"] = load_df[DE_zone]
    # DE_gendata_df["Import"] = import_df[DE_zone]
    # DE_gendata_df["Export"] = export_df[DE_zone] * -1

    # Placeholder for Battery(Load)
    DE_gendata_df["Battery (Load)"] = DE_con_df["BattDSO"] +DE_con_df["BattTSO"]

    # Emark has no DSM and loadshed
    DE_gendata_df["DSM (Up)"] = 0
    DE_gendata_df["DSM (Down)"] = 0
    DE_gendata_df["Load Shed"] = 0

    DE_gendata_df = DE_gendata_df.fillna(0)
    DE_gendata_df["Load (Net)"] = load_df[DE_zone] - DE_gendata_df["Wind Onshore"] - DE_gendata_df["Wind Offshore"] -DE_gendata_df["PV"]
    # DE_gendata_df["Import (Net)"] = import_df[DE_zone] - export_df[DE_zone]
    DE_gendata_df2 = DE_gendata_df.groupby(lambda x: x, axis=1).sum()

    DE_gendata_df_order = DE_gendata_df2[
        ["Nuclear", "Lignite","Coal","Gas (combined cycle)","Gas (simple cycle)","Oil", "Run of river", "Dam", "Biomass", "Wind Onshore", "Wind Offshore",
         "PV", "Pump (Generation)", "Battery (Generation)", "DSM (Up)", "Battery (Load)", "DSM (Down)", "Pump (Load)","Load (Total)", "Load (Net)", "Load Shed"]]

    DE_gendata_df_order = DE_gendata_df_order / 1000  # in GW
    DE_gendata_df_order.index.name = "Hour"
    DE_gendata_df_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_e_DE_{i}.csv")

    #### monthlygeneration #########
    DE_df_m = DE_gendata_df_order / 1000  # in TWh
    DE_df_m['date'] = pd.date_range(start='1/1/2018', periods=len(DE_df_m), freq='H')
    DE_df_m1 = DE_df_m.resample('M', on='date').sum()
    DE_df_m1 = DE_df_m1.reset_index()
    DE_df_m1["Month"] = range(1, 1 + len(DE_df_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    DE_df_m1 = DE_df_m1.drop(["date"], axis=1)
    DE_df_m1 = DE_df_m1.set_index("Month")
    DE_df_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_e_DE_{i}.csv")

    ### annual in Twh ######
    DE_df_annual = DE_gendata_df_order.sum(axis=0)
    DE_df_annual = DE_df_annual / 1000

    DE_annual_gen[i] =  DE_df_annual


    #########  Combine files for FR ################################################
    
    # Pump and battery
    Charging_FR = contents[0, 0]['AllM'][0, 0]["GenType_GenCharging_sum_FR"]
    Charging_FR_df = pd.DataFrame(Charging_FR)

    # FR pump and Batt profiles
    FR_con_df = pd.concat([gen_names_df, Charging_FR_df], axis=1)
    FR_con_df.columns = pd.RangeIndex(FR_con_df.columns.size)
    FR_con_df[0] = FR_con_df[0].str.get(0)

    FR_con_df = FR_con_df.fillna(0)
    FR_con_df = FR_con_df.transpose()

    FR_con_df.columns = FR_con_df.iloc[0]
    FR_con_df = FR_con_df.drop([0])
    # Gentpe order from 2020 : ROR, Nuclear, Lignite,Coal,Gas CC Gas SC, Biomass, Oil, WindOn, WindOff, PV, Oil DNS, Dam, Pump, Batt DSO , BattTSO, DSM
    FR_gen = contents[0, 0]['AllM'][0, 0]["GenType_GenInjections_nopump_sum_FR"]
    FR_gen_df_1 = pd.DataFrame(FR_gen)

    FR_gen_df = pd.concat([gen_names_df,FR_gen_df_1],axis =1)
    FR_gen_df.columns = pd.RangeIndex(FR_gen_df.columns.size)
    FR_gen_df[0] = FR_gen_df[0].str.get(0)

    FR_gen_df = FR_gen_df.fillna(0)
    FR_gendata_df = pd.DataFrame(FR_gen_df)

    FR_gendata_df[0] = FR_gendata_df[0].str.replace("GasCC", "Gas (combined cycle)")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("GasSC", "Gas (simple cycle)")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("BattTSO", "Battery (Generation)")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("BattDSO", "Battery (Generation)")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("RoR", "Run of river")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("WindOn", "Wind Onshore")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("WindOff", "Wind Offshore")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("Pump", "Pump (Generation)")
    FR_gendata_df[0] = FR_gendata_df[0].str.replace("DSM", "DSM (Down)")

    FR_zone = 2
    #

    FR_gendata_df = FR_gendata_df.transpose()
    FR_gendata_df.columns = FR_gendata_df.iloc[0]
    FR_gendata_df= FR_gendata_df.drop([0])
    FR_gendata_df = FR_gendata_df.reset_index()
    FR_gendata_df = FR_gendata_df.drop(["index"],axis=1)
    FR_gendata_df["Pump (Load)"] = FR_con_df["Pump"]


    FR_gendata_df["Load (Total)"] = load_df[FR_zone]
    # FR_gendata_df["Import"] = import_df[FR_zone]
    # FR_gendata_df["Export"] = export_df[FR_zone] * -1

    # Placeholder for Battery(Load)
    FR_gendata_df["Battery (Load)"] = FR_con_df["BattDSO"] +FR_con_df["BattTSO"]
    FR_gendata_df["DSM (Up)"] = 0
    FR_gendata_df["DSM (Down)"] = 0
    FR_gendata_df["Load Shed"] = 0

    FR_gendata_df = FR_gendata_df.fillna(0)
    FR_gendata_df["Load (Net)"] = load_df[FR_zone] - FR_gendata_df["Wind Onshore"] - FR_gendata_df["Wind Offshore"] -FR_gendata_df["PV"]
    # FR_gendata_df["Import (Net)"] = import_df[FR_zone] - export_df[FR_zone]
    FR_gendata_df2 = FR_gendata_df.groupby(lambda x: x, axis=1).sum()


    FR_gendata_df_order = FR_gendata_df2[
        ["Nuclear", "Lignite","Coal","Gas (combined cycle)","Gas (simple cycle)","Oil", "Run of river", "Dam", "Biomass", "Wind Onshore", "Wind Offshore",
         "PV", "Pump (Generation)", "Battery (Generation)", "DSM (Up)", "Battery (Load)", "DSM (Down)", "Pump (Load)","Load (Total)", "Load (Net)", "Load Shed"]]

    FR_gendata_df_order = FR_gendata_df_order / 1000  # in GW
    FR_gendata_df_order.index.name = "Hour"
    FR_gendata_df_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_e_FR_{i}.csv")

    #### monthlygeneration #########
    FR_df_m = FR_gendata_df_order / 1000  # in TWh
    FR_df_m['date'] = pd.date_range(start='1/1/2018', periods=len(FR_df_m), freq='H')
    FR_df_m1 = FR_df_m.resample('M', on='date').sum()
    FR_df_m1 = FR_df_m1.reset_index()
    FR_df_m1["Month"] = range(1, 1 + len(FR_df_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    FR_df_m1 = FR_df_m1.drop(["date"], axis=1)
    FR_df_m1 = FR_df_m1.set_index("Month")
    FR_df_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_e_FR_{i}.csv")

    ### annual in Twh ######
    FR_df_annual = FR_gendata_df_order.sum(axis=0)
    FR_df_annual = FR_df_annual / 1000

    FR_annual_gen[i] =  FR_df_annual
    #########  Combine files for AT ################################################
    
    # Pump and battery
    Charging_AT = contents[0, 0]['AllM'][0, 0]["GenType_GenCharging_sum_AT"]
    Charging_AT_df = pd.DataFrame(Charging_AT)

    # AT pump and Batt profiles
    AT_con_df = pd.concat([gen_names_df, Charging_AT_df], axis=1)
    AT_con_df.columns = pd.RangeIndex(AT_con_df.columns.size)
    AT_con_df[0] = AT_con_df[0].str.get(0)

    AT_con_df = AT_con_df.fillna(0)
    AT_con_df = AT_con_df.transpose()

    AT_con_df.columns = AT_con_df.iloc[0]
    AT_con_df = AT_con_df.drop([0])
    # Gentpe order from 2020 : ROR, Nuclear, Lignite,Coal,Gas CC Gas SC, Biomass, Oil, WindOn, WindOff, PV, Oil DNS, Dam, Pump, Batt DSO , BattTSO, DSM
    AT_gen = contents[0, 0]['AllM'][0, 0]["GenType_GenInjections_nopump_sum_AT"]
    AT_gen_df_1 = pd.DataFrame(AT_gen)

    AT_gen_df = pd.concat([gen_names_df,AT_gen_df_1],axis =1)
    AT_gen_df.columns = pd.RangeIndex(AT_gen_df.columns.size)
    AT_gen_df[0] = AT_gen_df[0].str.get(0)

    AT_gen_df = AT_gen_df.fillna(0)
    AT_gendata_df = pd.DataFrame(AT_gen_df)

    AT_gendata_df[0] = AT_gendata_df[0].str.replace("GasCC", "Gas (combined cycle)")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("GasSC", "Gas (simple cycle)")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("BattTSO", "Battery (Generation)")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("BattDSO", "Battery (Generation)")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("RoR", "Run of river")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("WindOn", "Wind Onshore")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("WindOff", "Wind Offshore")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("Pump", "Pump (Generation)")
    AT_gendata_df[0] = AT_gendata_df[0].str.replace("DSM", "DSM (Down)")

    AT_zone = 4
    #

    AT_gendata_df = AT_gendata_df.transpose()
    AT_gendata_df.columns = AT_gendata_df.iloc[0]
    AT_gendata_df= AT_gendata_df.drop([0])
    AT_gendata_df = AT_gendata_df.reset_index()
    AT_gendata_df = AT_gendata_df.drop(["index"],axis=1)
    AT_gendata_df["Pump (Load)"] = AT_con_df["Pump"]

    AT_gendata_df["Load (Total)"] = load_df[AT_zone]
    # AT_gendata_df["Import"] = import_df[AT_zone]
    # AT_gendata_df["Export"] = export_df[AT_zone] * -1

    # Placeholder for Battery(Load)
    AT_gendata_df["Battery (Load)"] = AT_con_df["BattDSO"] +AT_con_df["BattTSO"]
    AT_gendata_df["DSM (Up)"] = 0
    AT_gendata_df["DSM (Down)"] = 0
    AT_gendata_df["Load Shed"] = 0

    AT_gendata_df = AT_gendata_df.fillna(0)
    AT_gendata_df["Load (Net)"] = load_df[AT_zone] - AT_gendata_df["Wind Onshore"] - AT_gendata_df["Wind Offshore"] -AT_gendata_df["PV"]
    # AT_gendata_df["Import (Net)"] = import_df[AT_zone] - export_df[AT_zone]
    AT_gendata_df2 = AT_gendata_df.groupby(lambda x: x, axis=1).sum()


    AT_gendata_df_order = AT_gendata_df2[
        ["Nuclear", "Lignite","Coal","Gas (combined cycle)","Gas (simple cycle)","Oil", "Run of river", "Dam", "Biomass", "Wind Onshore", "Wind Offshore",
         "PV", "Pump (Generation)", "Battery (Generation)", "DSM (Up)", "Battery (Load)", "DSM (Down)", "Pump (Load)","Load (Total)", "Load (Net)", "Load Shed"]]

    AT_gendata_df_order = AT_gendata_df_order / 1000  # in GW
    AT_gendata_df_order.index.name = "Hour"
    AT_gendata_df_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_e_AT_{i}.csv")

    #### monthlygeneration #########
    AT_df_m = AT_gendata_df_order / 1000  # in TWh
    AT_df_m['date'] = pd.date_range(start='1/1/2018', periods=len(AT_df_m), freq='H')
    AT_df_m1 = AT_df_m.resample('M', on='date').sum()
    AT_df_m1 = AT_df_m1.reset_index()
    AT_df_m1["Month"] = range(1, 1 + len(AT_df_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    AT_df_m1 = AT_df_m1.drop(["date"], axis=1)
    AT_df_m1 = AT_df_m1.set_index("Month")
    AT_df_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_e_AT_{i}.csv")

    ### annual in Twh ######
    AT_df_annual = AT_gendata_df_order.sum(axis=0)
    AT_df_annual = AT_df_annual / 1000

    AT_annual_gen[i] =  AT_df_annual

    #### monthlygeneration #########
    AT_df_m = AT_gendata_df_order / 1000  # in TWh
    AT_df_m['date'] = pd.date_range(start='1/1/2018', periods=len(AT_df_m), freq='H')
    AT_df_m1 = AT_df_m.resample('M', on='date').sum()
    AT_df_m1 = AT_df_m1.reset_index()
    AT_df_m1["Month"] = range(1, 1 + len(AT_df_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    AT_df_m1 = AT_df_m1.drop(["date"], axis=1)
    AT_df_m1 = AT_df_m1.set_index("Month")
    AT_df_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_e_AT_{i}.csv")

    ### annual in Twh ######
    AT_df_annual = AT_gendata_df_order.sum(axis=0)
    AT_df_annual = AT_df_annual / 1000

    AT_annual_gen[i] = AT_df_annual

    #########  Combine files for IT ################################################
    
    # Pump and battery
    Charging_IT = contents[0, 0]['AllM'][0, 0]["GenType_GenCharging_sum_IT"]
    Charging_IT_df = pd.DataFrame(Charging_IT)

    # IT pump and Batt profiles
    IT_con_df = pd.concat([gen_names_df, Charging_IT_df], axis=1)
    IT_con_df.columns = pd.RangeIndex(IT_con_df.columns.size)
    IT_con_df[0] = IT_con_df[0].str.get(0)

    IT_con_df = IT_con_df.fillna(0)
    IT_con_df = IT_con_df.transpose()

    IT_con_df.columns = IT_con_df.iloc[0]
    IT_con_df = IT_con_df.drop([0])
    # Gentpe order from 2020 : ROR, Nuclear, Lignite,Coal,Gas CC Gas SC, Biomass, Oil, WindOn, WindOff, PV, Oil DNS, Dam, Pump, Batt DSO , BattTSO, DSM
    IT_gen = contents[0, 0]['AllM'][0, 0]["GenType_GenInjections_nopump_sum_IT"]
    IT_gen_df_1 = pd.DataFrame(IT_gen)

    IT_gen_df = pd.concat([gen_names_df,IT_gen_df_1],axis =1)
    IT_gen_df.columns = pd.RangeIndex(IT_gen_df.columns.size)
    IT_gen_df[0] = IT_gen_df[0].str.get(0)

    IT_gen_df = IT_gen_df.fillna(0)
    IT_gendata_df = pd.DataFrame(IT_gen_df)

    IT_gendata_df[0] = IT_gendata_df[0].str.replace("GasCC", "Gas (combined cycle)")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("GasSC", "Gas (simple cycle)")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("BattTSO", "Battery (Generation)")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("BattDSO", "Battery (Generation)")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("RoR", "Run of river")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("WindOn", "Wind Onshore")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("WindOff", "Wind Offshore")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("Pump", "Pump (Generation)")
    IT_gendata_df[0] = IT_gendata_df[0].str.replace("DSM", "DSM (Down)")

    IT_zone = 3
    #

    IT_gendata_df = IT_gendata_df.transpose()
    IT_gendata_df.columns = IT_gendata_df.iloc[0]
    IT_gendata_df= IT_gendata_df.drop([0])
    IT_gendata_df = IT_gendata_df.reset_index()
    IT_gendata_df = IT_gendata_df.drop(["index"],axis=1)
    IT_gendata_df["Pump (Load)"] = IT_con_df["Pump"]


    IT_gendata_df["Load (Total)"] = load_df[IT_zone]
    # IT_gendata_df["Import"] = import_df[IT_zone]
    # IT_gendata_df["Export"] = export_df[IT_zone] * -1

    # Placeholder for Battery(Load)
    IT_gendata_df["Battery (Load)"] = IT_con_df["BattDSO"] +IT_con_df["BattTSO"]
    IT_gendata_df["DSM (Up)"] = 0
    IT_gendata_df["DSM (Down)"] = 0
    IT_gendata_df["Load Shed"] = 0

    IT_gendata_df = IT_gendata_df.fillna(0)
    IT_gendata_df["Load (Net)"] = load_df[IT_zone] - IT_gendata_df["Wind Onshore"] - IT_gendata_df["Wind Offshore"] -IT_gendata_df["PV"]
    # IT_gendata_df["Import (Net)"] = import_df[IT_zone] - export_df[IT_zone]
    IT_gendata_df2 = IT_gendata_df.groupby(lambda x: x, axis=1).sum()


    IT_gendata_df_order = IT_gendata_df2[
        ["Nuclear", "Lignite","Coal","Gas (combined cycle)","Gas (simple cycle)","Oil", "Run of river", "Dam", "Biomass", "Wind Onshore", "Wind Offshore",
         "PV", "Pump (Generation)", "Battery (Generation)", "DSM (Up)", "Battery (Load)", "DSM (Down)", "Pump (Load)","Load (Total)", "Load (Net)", "Load Shed"]]

    IT_gendata_df_order = IT_gendata_df_order / 1000  # in GW
    IT_gendata_df_order.index.name = "Hour"
    IT_gendata_df_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_e_IT_{i}.csv")

    #### monthlygeneration #########
    IT_df_m = IT_gendata_df_order / 1000  # in TWh
    IT_df_m['date'] = pd.date_range(start='1/1/2018', periods=len(IT_df_m), freq='H')
    IT_df_m1 = IT_df_m.resample('M', on='date').sum()
    IT_df_m1 = IT_df_m1.reset_index()
    IT_df_m1["Month"] = range(1, 1 + len(IT_df_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    IT_df_m1 = IT_df_m1.drop(["date"], axis=1)
    IT_df_m1 = IT_df_m1.set_index("Month")
    IT_df_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_e_IT_{i}.csv")

    ### annual in Twh ######
    IT_df_annual = IT_gendata_df_order.sum(axis=0)
    IT_df_annual = IT_df_annual / 1000

    IT_annual_gen[i] =  IT_df_annual

    #########  Electrcity Prices ################################################

    if i == 2018:
        Elecprice = contents[0, 0]['AllM'][0, 0]["ZonePrices_DaM"]
    else:
        Elecprice = contents[0, 0]['AllM'][0, 0]["ZonePrices_DaM_Adj"]

    Elecprice_df = pd.DataFrame(Elecprice)

    Elecprice_df = Elecprice_df.transpose()

    Elecprice_CH = Elecprice_df[1]
    Elecprice = pd.DataFrame([])

    Elecprice["Switzerland"] = Elecprice_CH
    Elecprice["Germany"] = Elecprice_df[0]
    Elecprice["Italy"] = Elecprice_df[3]
    Elecprice["France"] = Elecprice_df[2]
    Elecprice["Austria"] = Elecprice_df[4]
    Elecprice.index.name = "Hour"
    Elecprice.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_elecprice_hourly_e_{i}.csv")

    # monthly
    Elecprice_m = Elecprice
    Elecprice_m['date'] = pd.date_range(start='1/1/2018', periods=len(Elecprice_m), freq='H')
    Elecprice_m1 = Elecprice_m.resample('M', on='date').mean()
    Elecprice_m1 = Elecprice_m1.reset_index()
    Elecprice_m1["Month"] = range(1, 1 + len(Elecprice_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    Elecprice_m1 = Elecprice_m1.drop(["date"], axis=1)
    Elecprice_m1 = Elecprice_m1.set_index("Month")
    Elecprice_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_elecprice_monthly_e_{i}.csv")

    Elecprice_annual[i] = Elecprice.mean(axis=0)

DE_annual_gen.index.name = "Row"
DE_annual_gen.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_e_DE.csv")

FR_annual_gen.index.name = "Row"
FR_annual_gen.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_e_FR.csv")

AT_annual_gen.index.name = "Row"
AT_annual_gen.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_e_AT.csv")
      
IT_annual_gen.index.name = "Row"
IT_annual_gen.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_e_IT.csv")
        
Elecprice_annual.index.name = "Row"
Elecprice_annual.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_elecprice_annual_e.csv")

     

        

