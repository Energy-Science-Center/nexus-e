from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class HydrogenExisting:
    def __init__(self, state, existing_H2units):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.model.ExistingHydrogen = existing_H2units #all existing hydrogen units
        
        """
        variables
        """
        self.model.HydrogenSoC = Var(
            self.model.ExistingHydrogen, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #state of charge [MWh] of existing hydrogen units in each time period
        self.model.HydrogenDemandContribution = Var(
            self.model.ExistingHydrogen,
            self.model.TimePeriods, 
            within=NonNegativeReals) #contribution towards hydrogen demand [MWh] of existing hydrogen units in each time period
        
    def select_hydrogenStorageExisting(self, H2unit):
        return SetOf(H2unit)
      
    """
    sets operational constraints for existing hydrogen units
    inputs are: 
        Maximum storage capacity for energy storage unit [MWh]
        Maximum discharge/charge power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh] 
        Charge efficiency
        Discharge efficiency
        Temporal resolution of simulation
    """
    def set_hydrogen(self, max_energy_capacity_hydrogen, max_power_discharge_hydrogen, max_power_charge_hydrogen, capacityT0_hydrogen, eff_charge_hydrogen, eff_discharge_hydrogen, tpResolution):
        self.model.MaxEnergyCapacityHydrogen = Param(
            self.model.ExistingHydrogen,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_hydrogen[g])
        self.model.MaxPowerDischargeHydrogen = Param(
            self.model.ExistingHydrogen,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_discharge_hydrogen[g])
        self.model.MaxPowerChargeHydrogen = Param(
            self.model.ExistingHydrogen,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_charge_hydrogen[g])        
        #Energy @ T0
        self.model.CapacityT0Hydrogen = Param(
            self.model.ExistingHydrogen,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_hydrogen[g] * tpResolution * m.MaxEnergyCapacityHydrogen[g]) 
        #min / max power constraint
        self.model.MaxPowerGeneratedHydrogenCon = Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerDischargeHydrogen[g])
        self.model.MaxPowerChargedHydrogenCon =  Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerChargeHydrogen[g])
        #max SoC constraint
        self.model.MaxEnergyStoredHydrogenCon = Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.HydrogenSoC[g,t] <= m.MaxEnergyCapacityHydrogen[g] * tpResolution)
        #energy balance of hydrogen storage candidates
        self.model.SoCHydrogenCandCon = Constraint(
            self.model.ExistingHydrogen, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.HydrogenSoC[g,t] == (m.CapacityT0Hydrogen[g] if t == 0 else m.HydrogenSoC[g,t-1]) - tpResolution * m.PowerGenerated[g,t]/eff_discharge_hydrogen + tpResolution * m.PowerConsumed[g,t] * eff_charge_hydrogen - tpResolution * m.HydrogenDemandContribution[g,t])#should multiply this constraint by the time step to get energy (our timestep is hour so everything works out)        
        #equal energy @ T
        self.model.EqualEnergyHydrogenCon = Constraint(
            self.model.ExistingHydrogen, 
            rule=lambda m,g: m.HydrogenSoC[g,self.num_snaphots-1] >= (m.CapacityT0Hydrogen[g]))

    """
    sets reserve constraints for candidate hydrogen units
    """   
    def set_hydrogen_reserves(self, reserves):
        if not hasattr(self.model, 'MaxPowerDischargeHydrogen'):
            raise Exception('Missing call to set_hydrogen')
        if not hasattr(self.model, 'MaxPowerChargeHydrogen'):
            raise Exception('Missing call to set_hydrogen')
        
        self.model.ActualUpFRReserveHydrogen = Var(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRReserveHydrogen = Var(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.RUpDischargeLimitHydrogenCon = Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveHydrogen[g,t] + m.ActualUpRReserveHydrogen[g,t] <= m.MaxPowerDischargeHydrogen[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t]) #this means the hydrogen storage unit can provide upward reserve if it's at 0 (off)
        self.model.RUpDischargeLimitHydrogenCon1 = Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveHydrogen[g,t] + m.ActualUpRReserveHydrogen[g,t] <= m.MaxPowerDischargeHydrogen[g] + m.MaxPowerChargeHydrogen[g]) #non-binding constraint
        for t in self.model.TimePeriods:
            for g in self.model.ExistingHydrogen:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveHydrogen[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRReserveHydrogen[g,t]
        
        self.model.ActualDownFRReserveHydrogen = Var(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRReserveHydrogen = Var(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.RDownChargeLimitHydrogenCon = Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t:  m.PowerConsumed[g,t] - m.PowerGenerated[g,t] + (m.ActualDownFRReserveHydrogen[g,t] + m.ActualDownRReserveHydrogen[g,t]) <= m.MaxPowerChargeHydrogen[g])
        self.model.RDownChargeLimitHydrogenCon1 = Constraint(
            self.model.ExistingHydrogen,
            self.model.TimePeriods,
            rule=lambda m,g,t:  m.ActualDownFRReserveHydrogen[g,t] + m.ActualDownRReserveHydrogen[g,t] <= m.MaxPowerDischargeHydrogen[g] + m.MaxPowerChargeHydrogen[g]) #non-binding constraint
        for t in self.model.TimePeriods:
            for g in self.model.ExistingHydrogen:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveHydrogen[g,t]  
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRReserveHydrogen[g,t] 

    """
    Sets hydrogen demand constraint
    """
    def set_hydrogen_demand(self, GENS, H2_demand, baseMVA):
        self.model.H2Demand = Param(
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,t: H2_demand[t]/baseMVA)
        self.model.H2DemandCon = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(m.HydrogenDemandContribution[g,t] for g in GENS) == m.H2Demand[t])

    """
    Generates an expression for the revenue from selling hydrogen
    """
    def get_hydrogen_revenue(self, H2_market_price):
        self.model.H2MarketPrice = Param(
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,t: H2_market_price[t])
        m = self.model
        return sum(m.HydrogenDemandContribution[g,t] * m.H2MarketPrice[t] for g in m.ExistingHydrogen for t in m.TimePeriods)

    """
    post-processing routines
    """
    #get state-of-charge of existing H2 units
    def get_hydrogen_SoC(self, H2units):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.HydrogenSoCInvest[H2units,time])
        return results

    #get hydrogen demand contribution
    def get_hydrogendemand_contribution(self, H2units):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.HydrogenDemandContribution[H2units,time])
        return results


