from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class BatteryStoragesExisting:
    def __init__(self, state, gens):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.generators = gens #all battery storages that exist
        
        """
        variables
        """
        self.model.BattSoC = Var(
            self.generators, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #state of charge [MWh] of existing battery storages

    def select_batt(self, l):
        return SetOf(l)
      
    """
    sets operational constraints for existing batteries
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum discharge/charge power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh] 
        Charge efficiency
        Discharge efficiency
        Temporal resolution of simulation
    """
    def set_battery_daily(self, GENS, min_energy_capacity_batt, max_energy_capacity_batt, max_power_discharge_batt, max_power_charge_batt, capacityT0_batt, eff_charge, eff_discharge, self_discharge_batt, tpResolution):
        self.model.MinEnergyCapacityBatt = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_batt[g])
        self.model.MaxEnergyCapacityBatt = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_batt[g])
        self.model.MaxPowerDischargeBatt = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_discharge_batt[g])
        self.model.MaxPowerChargeBatt = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_charge_batt[g])        
        self.model.CapacityT0Batt = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_batt[g] * m.MaxEnergyCapacityBatt[g] * tpResolution) 
        
        self.model.MaxPowerGeneratedBattCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerDischargeBatt[g])
        self.model.MaxPowerStoredBattCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerChargeBatt[g])
        self.model.MinMaxEnergyStoredBattCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityBatt[g] * tpResolution <= m.BattSoC[g,t] and m.BattSoC[g,t] <= m.MaxEnergyCapacityBatt[g] * tpResolution)
        self.model.SoCBattCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.BattSoC[g,t] == (m.CapacityT0Batt[g] if t == 0 else (1 - self_discharge_batt) * m.BattSoC[g,t-1]) - tpResolution * m.PowerGenerated[g,t]/eff_discharge[g] + tpResolution * m.PowerConsumed[g,t]*eff_charge[g])#should multiply this constraint by the time step to get energy (our timestep is hour so everything works out)        
                
    """
    sets reserve constraints for batteries
    use only for existing Swiss batteries
    """   
    def set_battery_daily_reserves(self, reserves, GENS, tpResolution):
        if not hasattr(self.model, 'MaxPowerDischargeBatt'):
            raise Exception('Missing call to set_battery_daily')
        if not hasattr(self.model, 'MaxPowerChargeBatt'):
            raise Exception('Missing call to set_battery_daily')
        
        #up reserves
        self.model.ActualUpFRReserveBattery = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRReserveBattery = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)

        #power limit with up reserves
        self.model.RUpDischargeLimitBatt = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveBattery[g,t] + m.ActualUpRReserveBattery[g,t] <= m.MaxPowerDischargeBatt[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t]) #this means the battery can provide upward reserve if it's at 0 (off)
        self.model.RUpDischargeLimitBatt1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveBattery[g,t] + m.ActualUpRReserveBattery[g,t] <= m.MaxPowerDischargeBatt[g] + m.MaxPowerChargeBatt[g]) #this is a non binding constraint 

        # soc limit with up reserves
        self.model.reserve_up_constraint_with_SoC_battery_existing_1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualUpFRReserveBattery[g,t] + m.ActualUpRReserveBattery[g,t])*tpResolution <=
                                  m.BattSoC[g, t] - m.MinEnergyCapacityBatt[g] * tpResolution))
        self.model.reserve_up_constraint_with_SoC_battery_existing_2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualUpFRReserveBattery[g,t] + m.ActualUpRReserveBattery[g,t])*tpResolution <=
                                  ((m.BattSoC[g, t - 1] - m.MinEnergyCapacityBatt[g] * tpResolution) if t > 0
                                  else (m.CapacityT0Batt[g] - m.MinEnergyCapacityBatt[g] * tpResolution))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveBattery[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRReserveBattery[g,t]
        
        #down reserves
        self.model.ActualDownFRReserveBattery = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRReserveBattery = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)

        #power limit with down reserves
        self.model.RDownChargeLimitBatt = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:  m.PowerConsumed[g,t] - m.PowerGenerated[g,t] + (m.ActualDownFRReserveBattery[g,t] + m.ActualDownRReserveBattery[g,t]) <= m.MaxPowerChargeBatt[g])
        self.model.RDownChargeLimitBatt1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveBattery[g,t] + m.ActualDownRReserveBattery[g,t] <= m.MaxPowerChargeBatt[g] + m.MaxPowerDischargeBatt[g]) #this is a non binding constraint 

        #soc limit with down reserves
        self.model.reserve_down_constraint_with_SoC_battery_existing_1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualDownFRReserveBattery[g,t] + m.ActualDownRReserveBattery[g,t])*tpResolution <=
                                  - m.BattSoC[g, t] + m.MaxEnergyCapacityBatt[g] * tpResolution))
        self.model.reserve_down_constraint_with_SoC_battery_existing_2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualDownFRReserveBattery[g,t] + m.ActualDownRReserveBattery[g,t])*tpResolution <=
                                  ((-m.BattSoC[g, t - 1] + m.MaxEnergyCapacityBatt[g] * tpResolution) if t > 0
                                  else (- m.CapacityT0Batt[g] + m.MaxEnergyCapacityBatt[g] * tpResolution))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveBattery[g,t]  
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRReserveBattery[g,t] 

    """
    sets constraints for dsm units
    inputs are: 
        Minimum/Maximum storage capacity for dsm unit [MWh]
        Maximum discharge/charge power for dsm unit [MW]
        Initial capacity of storage unit at T0 [MWh] 
        Charge efficiency
        Discharge efficiency
        Temporal resolution of simulation
    """   
    def set_dsm_daily(self, GENS, min_energy_capacity_dsm, max_energy_capacity_dsm, max_power_discharge_dsm, max_power_charge_dsm, capacityT0_dsm, tpResolution):
        self.model.MinEnergyCapacityDSM = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_dsm[g])
        self.model.MaxEnergyCapacityDSM = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_dsm[g])
        self.model.MaxPowerDischargeDSM = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_discharge_dsm[g])
        self.model.MaxPowerChargeDSM = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_charge_dsm[g])        
        self.model.CapacityT0DSM = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_dsm[g] * m.MaxEnergyCapacityDSM[g] * tpResolution) 
        
        self.model.MaxPowerGeneratedDSMCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerDischargeDSM[g])
        self.model.MaxPowerStoredDSMCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerChargeDSM[g])
        self.model.MinMaxEnergyStoredDSMCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityDSM[g] * tpResolution <= m.BattSoC[g,t] and m.BattSoC[g,t] <= m.MaxEnergyCapacityDSM[g] * tpResolution)
        self.model.SoCDSMCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.BattSoC[g,t] == (m.CapacityT0DSM[g] if t == 0 else m.BattSoC[g,t-1]) - tpResolution * m.PowerGenerated[g,t] + tpResolution * m.PowerConsumed[g,t])#should multiply this constraint by the time step to get energy (our timestep is hour so everything works out)        

    """
    post-processing routines
    """
    #get state-of-charge of batteries
    def get_battery_SoC(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.BattSoC[battery,time])
        return results
    
    #get secondary reserve contributions of batteries
    def frr_up_battery(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveBattery[battery,time])
        return results
    
    def frr_down_battery(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveBattery[battery,time])
        return results

    #get tertiary reserve contributions of batteries
    def rr_up_battery(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRReserveBattery[battery,time])
        return results
    
    def rr_down_battery(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRReserveBattery[battery,time])
        return results
