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


# # 2018
#
# #########  Load CentIv Results ################################################
#
# fn2018 = os.path.join(parentDirectory, "CentIvgeneration2018.csv")
# fn2018_c = os.path.join(parentDirectory, "Cap2018.csv")
#
# gen_2018 = pd.read_csv(fn2018)
# cap_2018 = pd.read_csv(fn2018_c)

# # For combined hourly generation
#
# #2018
# cen_abm_combined_2018 = pd.DataFrame([])
# cen_abm_combined_2018["Nuclear"] = gen_2018["Nuclear"]
# cen_abm_combined_2018["Oil"] = gen_2018["Oil"]
# cen_abm_combined_2018["Gas (combined cycle)"] = gen_2018["Gas (combined cycle)"]
# cen_abm_combined_2018["Gas (simple cycle)"] = gen_2018["Gas (simple cycle)"]
# cen_abm_combined_2018["Lignite"] = gen_2018["Lignite"]
# cen_abm_combined_2018["Coal"] = gen_2018["Coal"]
# cen_abm_combined_2018["Run of river"] = gen_2018["RoR"]
# cen_abm_combined_2018["Dam"] = gen_2018["Dam"]
# cen_abm_combined_2018["Biomass"] = gen_2018["Biomass"]
# cen_abm_combined_2018["Wind Onshore"] = gen_2018["Wind"]
# cen_abm_combined_2018["Wind Offshore"] = 0
# cen_abm_combined_2018["PV"] = gen_2018["PV"].astype(float) + gen_2018["DistIv_curtail"].astype(float)
# cen_abm_combined_2018["Pump (Generation)"] = gen_2018["Pump (Generation)"]
# cen_abm_combined_2018["Battery (Generation)"] = 0
# cen_abm_combined_2018["DSM (Down)"] = 0
# cen_abm_combined_2018["Import"] = gen_2018["Imports_CH"]
# cen_abm_combined_2018["Pump (Load)"] = gen_2018["Pump_load"]
# cen_abm_combined_2018["Battery (Load)"] = 0
# cen_abm_combined_2018["DSM (Up)"] = 0
# cen_abm_combined_2018["Export"] = gen_2018["Exports_CH"]
# cen_abm_combined_2018["Import (Net)"] = gen_2018["Imports_CH"].astype(float) + (gen_2018["Exports_CH"]).astype(float)
# cen_abm_combined_2018["Load (Net)"] = gen_2018["Load_CH"].astype(float) - gen_2018["Wind"].astype(float) - gen_2018["PV"].astype(float)
# cen_abm_combined_2018["Load (Total)"] = gen_2018["Load_CH"]
# cen_abm_combined_2018["Load Shed"] = gen_2018["LoadShed"]
#
# cen_abm_combined_2018  = cen_abm_combined_2018 /1000 #in GW
# cen_abm_combined_2018.index.name = "Hour"
# cen_abm_combined_2018.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_c_CH_2018.csv")
#
# #### monthlygeneration #########
# cen_abm_2018_m = cen_abm_combined_2018/1000 #in TWh
# cen_abm_2018_m['date'] = pd.date_range(start='1/1/2018', periods=len(cen_abm_2018_m), freq='H')
# cen_abm_2018_m1 = cen_abm_2018_m.resample('M', on='date').sum()
# cen_abm_2018_m1 = cen_abm_2018_m1.reset_index()
# cen_abm_2018_m1["Month"] = range(1, 1 + len(cen_abm_2018_m1))
# cen_abm_2018_m1 = cen_abm_2018_m1.drop(["date"],axis=1)
# cen_abm_2018_m1  = cen_abm_2018_m1.set_index("Month")
# cen_abm_2018_m1.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_c_CH_2018.csv")
#
# ### annual in Twh ######
# cen_abm_2018_annual = cen_abm_combined_2018.sum(axis=0)
# cen_abm_2018_annual = cen_abm_2018_annual/1000
    
# Compile annual TWh for CH
CH_annual_cap = pd.DataFrame([])
Cap_annual = pd.DataFrame([])

pv_list = []
bat_list = []


def create_revenue(years):
    # create revenue data by multiplying price with hourly generation

    country_names = {
        'CH': 'Switzerland',
        'FR': 'France',
        'IT': 'Italy',
        'AT': 'Austria',
        'DE': 'Germany'
    }
    years_list = []
    for country in country_names:

        revenue_all_years = pd.DataFrame()

        for year in years:
            # check if and electricity price file exist
            price_file = os.path.join(dir_new, f"national_elecprice_hourly_c_{year}.csv")
            if os.path.exists(price_file):
                # read price file
                price_h = pd.read_csv(price_file)
                gen_file = os.path.join(dir_new, f"national_generation_hourly_gwh_c_{country}_{i}.csv")
                # check if generation file exists
                if os.path.exists(gen_file):
                    gen_annual_h = pd.read_csv(gen_file)
                    # price array to series
                    s_price_h = price_h[country_names[country]]

                    # multiply all rows with electricity price
                    # GWh * CHF / MWh * 1000 MWh / GWh
                    revenue_h = gen_annual_h.apply(lambda x: np.asarray(x) * np.asarray(s_price_h) * 1000)

                    # hourly revenue
                    # write to csv file
                    revenue_h.drop(columns=['Hour'], inplace=True)
                    revenue_h.index.rename('Hour', inplace=True)
                    revenue_h.to_csv(os.path.join(dir_new, f'generation_revenue_hourly_{country}_{i}.csv'))

                    # monthly revenue
                    revenue_m = revenue_h
                    revenue_m['date'] = pd.date_range(start='1/1/2018', periods=len(revenue_m), freq='H')
                    revenue_m = revenue_m.resample('M', on='date').sum()
                    revenue_m = revenue_m.reset_index()
                    revenue_m["Month"] = range(1, 1 + len(revenue_m))
                    revenue_m = revenue_m.drop(["date"], axis=1)
                    revenue_m = revenue_m.set_index("Month")
                    # write to csv file
                    revenue_m.to_csv(os.path.join(dir_new, f'generation_revenue_monthly_{country}_{i}.csv'))

                    # annual revenue
                    revenue_a = revenue_h.sum(axis=0)
                    revenue_a.index.rename('Row', inplace=True)
                    revenue_a.columns = [year]
                    print(revenue_a.columns)

                    # combine all annual columns to a dataframe
                    revenue_all_years = pd.concat([revenue_all_years, revenue_a], axis=1)
                    years_list.append(year)

        # write annual revenue
        revenue_all_years.index.rename('Row', inplace=True)
        revenue_all_years.columns = years_list
        revenue_all_years.to_csv(os.path.join(dir_new, f'generation_revenue_annual_{country}.csv'))

    return


for i in list_years:

    #########  Load CentIv Results ################################################

    fn = os.path.join(parentDirectory, f"CentIvgeneration{i}.csv")
    print(fn)
    fn_c = os.path.join(parentDirectory, f"Cap{i}.csv")
    gen = pd.read_csv(fn)
    cap = pd.read_csv(fn_c)


    if i == 2020:
        # PV capacity from ABM

        pvcap_v = 0
        pv_list.append(pvcap_v)

        # Battery Capacity from ABM

        pvbat_v = 0
        bat_list.append(pvbat_v)
    else:
        #########  Load ABM Results ################################################
        fn = (f"{parentDirectory}/../../Results/{simu_name}/ABM_{i}.mat")
        data = scipy.io.loadmat(fn)
        contents = data['ABMtoPostprocess']

        # Get ABM PV generation data
        PV = contents[0, 0]['PV_gen_profiles']
        PV_df = pd.DataFrame(PV)


        PV_bat_char = contents[0, 0]['batt_charge_profiles']
        PV_bat_char_df = pd.DataFrame(PV_bat_char)
        PV_bat_char_df = PV_bat_char_df.transpose()

        PV_bat_dis = contents[0, 0]['batt_discharge_profiles']
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

    # 2020
    cen_abm_combined = pd.DataFrame([])
    cen_abm_combined["Nuclear"] = gen["Nuclear"]
    cen_abm_combined["Oil"] = gen["Oil"]
    cen_abm_combined["GasCC"] = gen["GasCC"]
    cen_abm_combined["GasSC"] = gen["GasSC"]
    cen_abm_combined["GasCC-CCS"] = gen["GasCC-CCS"]
    cen_abm_combined["Lignite"] = gen["Lignite"]
    cen_abm_combined["Coal"] = gen["Coal"]
    cen_abm_combined["Run of river"] = gen["RoR"]
    cen_abm_combined["Dam"] = gen["Dam"]
    cen_abm_combined["Biomass"] = gen["Biomass"]
    cen_abm_combined["Wind Onshore"] = gen["WindOn"]
    cen_abm_combined["Wind Offshore"] = 0

    if i == 2020:
        cen_abm_combined["PV"] = gen["PV"].astype(float) + gen["DistIv_curtail"].astype(float)
        cen_abm_combined["Battery (Generation)"] = gen["Battery (Generation)"]
        cen_abm_combined["Battery (Load)"] = gen["Battery (Load)"]
    else :
        cen_abm_combined["PV"] = PV_df[0].astype(float) + gen["DistIv_curtail"].astype(float)
        cen_abm_combined["Battery (Generation)"] = gen["Battery (Generation)"].astype(float) + PV_bat_dis_df[0]
        cen_abm_combined["Battery (Load)"] = gen["Battery (Load)"].astype(float) + PV_bat_char_df[0] * -1

    cen_abm_combined["Pump (Generation)"] = gen["Pump (Generation)"]
    cen_abm_combined["DSM (Down)"] = 0
    cen_abm_combined["DSM (Up)"] = 0
    cen_abm_combined["Import"] = gen["Imports_CH"]
    cen_abm_combined["Pump (Load)"] = gen["Pump (Load)"]
    cen_abm_combined["Export"] = gen["Exports_CH"]
    cen_abm_combined["Import (Net)"] = (gen["Imports_CH"]).astype(float) + (gen["Exports_CH"]).astype(
        float)
    cen_abm_combined["Load (Net)"] = gen["Load_CH"].astype(float) - gen["WindOn"].astype(float) - \
                                          cen_abm_combined["PV"].astype(float)
    cen_abm_combined["Load (Total)"] = gen["Load_CH"]
    cen_abm_combined["Load Shed"] = gen["LoadShed"]
    cen_abm_combined["Load Shed Industry"] = gen['LoadShedInd']

    cen_abm_combined = cen_abm_combined / 1000  # in GW
    cen_abm_combined.index.name = "Hour"
    cen_abm_combined.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_hourly_gwh_c_CH_{i}.csv")

    #### monthlygeneration #########
    cen_abm_m = cen_abm_combined / 1000  # in TWh
    cen_abm_m['date'] = pd.date_range(start='1/1/2018', periods=len(cen_abm_m), freq='H')
    cen_abm_m1 = cen_abm_m.resample('M', on='date').sum()
    cen_abm_m1 = cen_abm_m1.reset_index()
    cen_abm_m1["Month"] = range(1, 1 + len(cen_abm_m1))  # to be able to run the model partially, e.g. for 168 hours, it is needed to defined this flexibly
    cen_abm_m1 = cen_abm_m1.drop(["date"], axis=1)
    cen_abm_m1 = cen_abm_m1.set_index("Month")
    cen_abm_m1.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_monthly_twh_c_CH_{i}.csv")

    ### annual in Twh ######
    cen_abm_annual = cen_abm_combined.sum(axis=0)
    cen_abm_annual = cen_abm_annual / 1000

    CH_annual_cap[i] = cen_abm_annual

    if i == 2020:
        # Cap_annual_a = pd.merge(cap_2018, cap, on="Technology", how="left")
        Cap_annual_a = cap
    else:
        Cap_annual_a =  pd.merge(Cap_annual_a, cap, on="Technology", how="outer")

# Compile annual TWh for CH

# if you want to include 2018
#CH_annual_cap["2018"] = cen_abm_2018_annual

CH_annual_cap.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_c_CH.csv")
CH_annual_cap.index.name = "Row"
CH_annual_cap.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_generation_annual_twh_c_CH.csv")

# compute generation revenues by type
create_revenue(list_years)

# capacity
if os.path.exists(fnpath1):
    Cap_annual = Cap_annual_a[["Technology","2020","2030","2040","2050"]]
    Cap_annual= Cap_annual.rename(columns={"Technology": 'Row'})

    Cap_annual = Cap_annual.set_index("Row")

    # Cap_annual.at[7, "2020"] = pv_list[0]
    Cap_annual.at["PV", "2030"] = pv_list[1]
    Cap_annual.at["PV", "2040"] = pv_list[2]
    Cap_annual.at["PV", "2050"] = pv_list[3]

    cap_annual_t = Cap_annual.transpose()
    col_len = len(Cap_annual)

    if "BattTSO" in cap_annual_t:

        # Cap_annual.at[11, "2020"] = bat_list[0]
        Cap_annual.at["BattTSO", "2030"] = bat_list[1] + Cap_annual.loc["BattTSO"]["2030"]
        Cap_annual.at["BattTSO", "2040"] = bat_list[2] + Cap_annual.loc["BattTSO"]["2040"]
        Cap_annual.at["BattTSO", "2050"] = bat_list[3] + Cap_annual.loc["BattTSO"]["2050"]
        Cap_annual = Cap_annual.reset_index()
        Cap_annual["Row"]= Cap_annual["Row"].str.replace("BattTSO", "Battery")
    else:
        Cap_annual = Cap_annual.reset_index()
        Cap_annual.at[11, "Row"] = "Battery"
        # Cap_annual.at[11, "2020"] = bat_list[0]
        Cap_annual.at[col_len, "2030"] = bat_list[1]
        Cap_annual.at[col_len, "2040"] = bat_list[2]
        Cap_annual.at[col_len, "2050"] = bat_list[3]

    Cap_annual["Row"] = Cap_annual["Row"].str.replace("RoR", "Run of river")
    Cap_annual["Row"] = Cap_annual["Row"].str.replace("WindOn", "Wind Onshore")
    Cap_annual["Row"] = Cap_annual["Row"].str.replace("WindOff", "Wind Offshore")
    Cap_annual = Cap_annual[Cap_annual["Row"] != "Oil-DNS"]
    Cap_annual.at[col_len+1, "Row"] = "Wind Offshore"
    Cap_annual.at[col_len+2, "Row"] = "Lignite"
    Cap_annual.at[col_len+3, "Row"] = "Coal"
    Cap_annual.fillna(0)
    Cap_annual2 = Cap_annual.transpose()
    # order : 0 (Biomass), 1(Dam), 2,3,5 (fossil),4 (Nuclear), 6 DNS,7 (PV), 8 (Pump), 9 (ROR), 10 WindOn, 11 battery 12 Windoffshore
    #Cap_annual_order = Cap_annual2[[4,2,3,5,9,1,0,10,12,7,8,11]]
    Cap_annual_order = Cap_annual2.transpose()
    Cap_annual_order = Cap_annual_order.groupby(by = "Row", sort = False).sum()
    Cap_annual_order = Cap_annual_order/1000 #in GW
    Cap_annual_order.to_csv(f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_capacity_gw_CH.csv")

elif  os.path.exists(fnpath2):
    Cap_annual = Cap_annual_a[["Technology", "2020", "2030", "2040"]]
    Cap_annual = Cap_annual.rename(columns={"Technology": 'Row'})

    Cap_annual = Cap_annual.set_index("Row")

    # Cap_annual.at[7, "2020"] = pv_list[0]
    Cap_annual.at["PV", "2030"] = pv_list[1]
    Cap_annual.at["PV", "2040"] = pv_list[2]

    cap_annual_t = Cap_annual.transpose()
    col_len = len(Cap_annual)
    if "BattTSO" in cap_annual_t:

        # Cap_annual.at[11, "2020"] = bat_list[0]
        Cap_annual.at["BattTSO", "2030"] = bat_list[1] + Cap_annual.loc["BattTSO"]["2030"]
        Cap_annual.at["BattTSO", "2040"] = bat_list[2] + Cap_annual.loc["BattTSO"]["2040"]

        Cap_annual = Cap_annual.reset_index()
        Cap_annual["Row"] = Cap_annual["Row"].str.replace("BattTSO", "Battery")
    else:
        Cap_annual = Cap_annual.reset_index()
        Cap_annual.at[col_len, "Row"] = "Battery"
        # Cap_annual.at[11, "2020"] = bat_list[0]
        Cap_annual.at[col_len, "2030"] = bat_list[1]
        Cap_annual.at[col_len, "2040"] = bat_list[2]

    Cap_annual["Row"] = Cap_annual["Row"].str.replace("RoR", "Run of river")
    Cap_annual["Row"] = Cap_annual["Row"].str.replace("WindOn", "Wind Onshore")
#    Cap_annual["Row"] = Cap_annual["Row"].str.replace("WindOff", "Wind Offshore")
    Cap_annual = Cap_annual[Cap_annual["Row"] != "Oil-DNS"]
    Cap_annual.at[col_len+1, "Row"] = "Wind Offshore"
    Cap_annual.at[col_len+2, "Row"] = "Lignite"
    Cap_annual.at[col_len+3, "Row"] = "Coal"
    Cap_annual.fillna(0)
    Cap_annual2 = Cap_annual.transpose()
    # order : 0 (Biomass), 1(Dam), 2,3,5 (fossil),4 (Nuclear), 6 DNS,7 (PV), 8 (Pump), 9 (ROR), 10 WindOn, 11 battery 12 Windoffshore
    # Cap_annual_order = Cap_annual2[[4,2,3,5,9,1,0,10,12,7,8,11]]
    Cap_annual_order = Cap_annual2.transpose()
    Cap_annual_order = Cap_annual_order.groupby(by="Row", sort=False).sum()
    Cap_annual_order = Cap_annual_order / 1000  # in GW
    Cap_annual_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_capacity_gw_CH.csv")

else:
    Cap_annual = Cap_annual_a[["Technology", "2020", "2030"]]
    Cap_annual = Cap_annual.rename(columns={"Technology": 'Row'})
    Cap_annual = Cap_annual.set_index("Row")

    # Cap_annual.at[7, "2020"] = pv_list[0]
    Cap_annual.at["PV", "2030"] = pv_list[1]

    cap_annual_t = Cap_annual.transpose()
    col_len = len(Cap_annual)
    if "BattTSO" in cap_annual_t:

        # Cap_annual.at[11, "2020"] = bat_list[0]
        Cap_annual.at["BattTSO", "2030"] = bat_list[1] + Cap_annual.loc["BattTSO"]["2030"]
        Cap_annual = Cap_annual.reset_index()
        Cap_annual["Row"] = Cap_annual["Row"].str.replace("BattTSO", "Battery")
    else:
        Cap_annual = Cap_annual.reset_index()
        Cap_annual.at[col_len, "Row"] = "Battery"
        # Cap_annual.at[11, "2020"] = bat_list[0]
        Cap_annual.at[col_len, "2030"] = bat_list[1]

    Cap_annual["Row"] = Cap_annual["Row"].str.replace("RoR", "Run of river")
    Cap_annual["Row"] = Cap_annual["Row"].str.replace("WindOn", "Wind Onshore")
    Cap_annual["Row"] = Cap_annual["Row"].str.replace("WindOff", "Wind Offshore")
    Cap_annual = Cap_annual[Cap_annual["Row"] != "Oil-DNS"]

    Cap_annual.at[col_len+1, "Row"] = "Wind Offshore"
    Cap_annual.at[col_len+2, "Row"] = "Lignite"
    Cap_annual.at[col_len+3, "Row"] = "Coal"

    Cap_annual.fillna(0)
    Cap_annual2 = Cap_annual.transpose()
    # order : 0 (Biomass), 1(Dam), 2,3,5 (fossil),4 (Nuclear), 6 DNS,7 (PV), 8 (Pump), 9 (ROR), 10 WindOn, 11 battery 12 Windoffshore
    # Cap_annual_order = Cap_annual2[[4,2,3,5,9,1,0,10,12,7,8,11]]
    Cap_annual_order = Cap_annual2.transpose()
    Cap_annual_order = Cap_annual_order.groupby(by="Row", sort=False).sum()
    Cap_annual_order = Cap_annual_order / 1000  # in GW
    Cap_annual_order.to_csv(
        f"{parentDirectory}/Outputs/{simu_name}/national_generation_and_capacity/national_capacity_gw_CH.csv")
