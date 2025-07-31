from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np
from value_format import ValueFormatter

class BatteryStoragesInvest:
    def __init__(self, state, candidates_batt):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.model.CandidateBatteries = candidates_batt #all candidate battery storages
        
        """
        variables
        """
        self.model.BattSoCInvest = Var(
            self.model.CandidateBatteries, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #state of charge of candidate battery storages

        self.model.BattInv = Var(
            self.model.CandidateBatteries, 
            within=NonNegativeReals) #continuous variable for investments in battery storages


    def select_batt(self, l):
        return SetOf(l)
      
    """
    sets operational constraints for candidate batteries
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum discharge/charge power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh] 
        Charge efficiency
        Discharge efficiency
        Temporal resolution of simulation
    """
    def set_battery_daily_invest(self, min_energy_capacity_candbatt, max_energy_capacity_candbatt, max_power_discharge_candbatt, max_power_charge_candbatt, capacityT0_candbatt, eff_charge_candbatt, eff_discharge_candbatt, self_discharge_batt, tpResolution):
        self.model.MinEnergyCapacityCandBatt = Param(
            self.model.CandidateBatteries,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_candbatt[g])
        self.model.MaxEnergyCapacityCandBatt = Param(
            self.model.CandidateBatteries,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_candbatt[g])
        self.model.MaxPowerDischargeCandBatt = Param(
            self.model.CandidateBatteries,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_discharge_candbatt[g])
        self.model.MaxPowerChargeCandBatt = Param(
            self.model.CandidateBatteries,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_charge_candbatt[g])        
        self.model.BattInvCon = Constraint(
            self.model.CandidateBatteries,
            rule=lambda m,g: m.BattInv[g] <= m.MaxPowerDischargeCandBatt[g])
        
        
        self.model.CapacityT0CandBatt = Param(
            self.model.CandidateBatteries,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_candbatt[g] * tpResolution * m.BattInv[g] / m.MaxPowerDischargeCandBatt[g]) 
        
        self.model.MaxPowerGeneratedCandBattCon = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.BattInv[g])
        self.model.MaxPowerStoredCandBattCon =  Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.BattInv[g])
        self.model.MinMaxEnergyStoredCandBattCon1 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityCandBatt[g] * tpResolution * m.BattInv[g] / m.MaxPowerDischargeCandBatt[g] <= m.BattSoCInvest[g,t])
        self.model.MinMaxEnergyStoredCandBattCon2 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.BattSoCInvest[g,t] <= m.MaxEnergyCapacityCandBatt[g] * tpResolution * m.BattInv[g] / m.MaxPowerDischargeCandBatt[g])
        self.model.SoCBattCandCon = Constraint(
            self.model.CandidateBatteries, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.BattSoCInvest[g,t] == (m.CapacityT0CandBatt[g] if t == 0 else (1 - self_discharge_batt) * m.BattSoCInvest[g,t-1]) - tpResolution * m.PowerGenerated[g,t]/eff_discharge_candbatt[g] + tpResolution * m.PowerConsumed[g,t] * eff_charge_candbatt[g]) #should multiply this constraint by the time step to get energy (our timestep is hour so everything works out)        

    """
    sets reserve constraints for batteries
    """   
    def set_battery_daily_reserves_invest(self, reserves, GENS, tpResolution):
        if not hasattr(self.model, 'MaxPowerDischargeCandBatt'):
            raise Exception('Missing call to set_battery_daily_invest')
        if not hasattr(self.model, 'MaxPowerChargeCandBatt'):
            raise Exception('Missing call to set_battery_daily_invest')

        #up reserves
        self.model.ActualUpFRReserveBatteryInv = Var(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRReserveBatteryInv = Var(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            within=NonNegativeReals)
        #power limit with up reserves
        self.model.RUpDischargeLimitCandBatt1 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveBatteryInv[g,t] + m.ActualUpRReserveBatteryInv[g,t] <= m.BattInv[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t]) #this means the battery can provide upward reserve if it's at 0 (off)
        self.model.RUpDischargeLimitCandBatt2 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveBatteryInv[g,t] + m.ActualUpRReserveBatteryInv[g,t] <= m.MaxPowerDischargeCandBatt[g] + m.MaxPowerChargeCandBatt[g]) #this is a non binding constraint 
        # soc limit with up reserves
        self.model.reserve_up_constraint_with_SoC_battery_invest_1 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualUpFRReserveBatteryInv[g,t] + m.ActualUpRReserveBatteryInv[g,t])*tpResolution <=
                                  m.BattSoCInvest[g, t] - m.MinEnergyCapacityCandBatt[g] * tpResolution))
        self.model.reserve_up_constraint_with_SoC_battery_invest_2 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualUpFRReserveBatteryInv[g,t] + m.ActualUpRReserveBatteryInv[g,t])*tpResolution <=
                                  ((m.BattSoCInvest[g, t - 1] - m.MinEnergyCapacityCandBatt[g] * tpResolution) if t > 0
                                  else (m.CapacityT0CandBatt[g] - m.MinEnergyCapacityCandBatt[g] * tpResolution))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in self.model.CandidateBatteries:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveBatteryInv[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpFRReserveBatteryInv[g,t]
        
        #down reserves
        self.model.ActualDownFRReserveBatteryInv = Var(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRReserveBatteryInv = Var(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            within=NonNegativeReals)

        #power limit with down reserves
        self.model.RDownChargeLimitCandBatt1 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t:  m.PowerConsumed[g,t] - m.PowerGenerated[g,t] + (m.ActualDownFRReserveBatteryInv[g,t] + m.ActualDownRReserveBatteryInv[g,t]) <= m.BattInv[g])
        self.model.RDownChargeLimitCandBatt2 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveBatteryInv[g,t] + m.ActualDownRReserveBatteryInv[g,t] <= m.MaxPowerChargeCandBatt[g] + m.MaxPowerDischargeCandBatt[g]) #this is a non binding constraint 

        # soc limit with down reserves
        self.model.reserve_down_constraint_with_SoC_battery_invest_1 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualDownFRReserveBatteryInv[g,t] + m.ActualDownRReserveBatteryInv[g,t])*tpResolution <=
                                  - m.BattSoCInvest[g, t] + m.MaxEnergyCapacityCandBatt[g] * tpResolution))
        self.model.reserve_down_constraint_with_SoC_battery_invest_2 = Constraint(
            self.model.CandidateBatteries,
            self.model.TimePeriods,
            rule=lambda m, g, t: ((m.ActualDownFRReserveBatteryInv[g,t] + m.ActualDownRReserveBatteryInv[g,t])*tpResolution <=
                                  ((-m.BattSoCInvest[g, t - 1] + m.MaxEnergyCapacityCandBatt[g] * tpResolution) if t > 0
                                  else (- m.CapacityT0CandBatt[g] + m.MaxEnergyCapacityCandBatt[g] * tpResolution))))

        # link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in self.model.CandidateBatteries:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveBatteryInv[g,t]  
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRReserveBatteryInv[g,t] 

    """
    Generates an expression for the investment costs of all candidate batteries in the system
    """
    def get_investment_cost_batt(self, investment_cost_candbatt, fixed_cost_candbatt = None):
        self.model.InvCostCandBatt = Param(
                self.model.CandidateBatteries,
                within=NonNegativeReals,
                initialize=lambda m,g: investment_cost_candbatt[g])
        if fixed_cost_candbatt:
            return sum((self.model.InvCostCandBatt[g] + fixed_cost_candbatt[g]) * self.model.BattInv[g] for g in self.model.CandidateBatteries) #we need to multiply by max installed power because values in the tables are in CHF/MW
        else:    
            return sum(self.model.InvCostCandBatt[g] * self.model.BattInv[g] for g in self.model.CandidateBatteries) #we need to multiply by max installed power because values in the tables are in CHF/MW
     
    """
    post-processing routines
    """
    #get state-of-charge of batteries
    def get_battery_SoC_inv(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.BattSoCInvest[battery,time])
        return results

    def get_batteries_built(self):
        capacities = {}
        for unit in self.model.CandidateBatteries:
            original_capacity = ValueFormatter(float(value(self.model.BattInv[unit])))
            capacities[unit] = (
                original_capacity
                .truncate(decimal=3)
                .round_up(decimal=2)
                .get_formatted_value()
            )
        return capacities
    
    #get secondary reserve contributions of batteries
    def frr_up_battery_inv(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveBatteryInv[battery,time])
        return results
    
    def frr_down_battery_inv(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveBatteryInv[battery,time])
        return results

    #get tertiary reserve contributions of batteries
    def rr_up_battery_inv(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRReserveBatteryInv[battery,time])
        return results
    
    def rr_down_battery_inv(self, battery):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRReserveBatteryInv[battery,time])
        return results
