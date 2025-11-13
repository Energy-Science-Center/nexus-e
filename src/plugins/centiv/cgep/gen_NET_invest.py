from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np
import math
from .value_format import ValueFormatter

class NETInvest:
    def __init__(self, state, cand_NET):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.model.CandNET = cand_NET #all candidate NET units

        """
        investment variables
        """
        #Direct air capture (DAC) unit 
        self.model.DACInvest = Var(
            self.model.CandNET, 
            within=NonNegativeReals, bounds=(0,1)) #cont. variable indicating how much of the DAC candidate unit is built   
            
        """
        operation variables
        """
        #MWh-equivalent variables
        self.model.PconDAC = Var(
            self.model.CandNET,
            self.model.TimePeriods, 
            within=NonNegativeReals) #electricity consumed by the DAC unit  

        #limit PowerGenerated[g,t] of the system
        self.model.PgeneratedDACSystemCon = Constraint(
            self.model.CandNET,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] == 0)             
            
        #limit PowerConsumed[g,t] of the system
        self.model.PconsumedDACSystemCon = Constraint(
            self.model.CandNET,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == m.PconDAC[g,t])          
                        
    """
    sets operation constraints
    """
    def set_NET_invest(self, max_power_con_DAC):
        #define the maximum size of each technology to invest in (DAC)
        self.model.ConMaxDAC = Param(
            self.model.CandNET,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_con_DAC[g]) #MW_e
            
        #DAC consumption limited by Pmax of candidate unit
        self.model.DACBuildCon = Constraint(
            self.model.CandNET,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PconDAC[g,t] <= m.DACInvest[g] * m.ConMaxDAC[g]) #max DAC size constraint (MW_e)    

    """
    Generates an expression for the investment costs of all NET technologies in the system
    """
    def get_investment_cost_NET(self, max_power_cand, investment_cost_gens, fixed_cost_gens):
        self.model.InvCostNET = Param(
            self.model.CandNET,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_gens[g])
        self.model.MaxPowerCandNET = Param(
            self.model.CandNET,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_cand[g])
        m = self.model
        if fixed_cost_gens:
            return sum((m.InvCostNET[g] * m.MaxPowerCandNET[g] + fixed_cost_gens[g] * m.MaxPowerCandNET[g]) * m.DACInvest[g] for g in m.CandNET) 
        else: 
            return sum(m.InvCostNET[g] * m.MaxPowerCandNET[g]* m.DACInvest[g] for g in m.CandNET)                   
                   
    """
    Generates an expression for the operational costs of all candidate NET units in the system

    """                                      
    def get_operational_costs_NET_disagg(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, tpRes, baseMVA):
        self.model.FuelPriceNET = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffNET = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceNET = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateNET = Param(
            GENS,
            within=Reals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMNET = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        return (sum((m.FuelPriceNET[g,t] / m.FuelEffNET[g] + m.CO2PriceNET[g,t] * m.CO2RateNET[g] + m.NonFuelVOMNET[g]) *  m.PowerConsumed[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes

    def get_operational_costs_NET_disagg_LP(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, baseMVA):
        self.model.FuelPriceNETLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffNETLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceNETLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateNETLP = Param(
            GENS,
            within=Reals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMNETLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        return sum((m.FuelPriceNETLP[g,t] / m.FuelEffNETLP[g] + m.CO2PriceNETLP[g,t] * m.CO2RateNETLP[g] + m.NonFuelVOMNETLP[g]) *  m.PowerConsumed[g,t] for t in self.model.TimePeriods for g in GENS)                   
            
    """
    post-processing routines
    """
    #get the size of the DAC unit
    def get_DAC_inv(self):
        dac_units = {}
        for unit in self.model.CandNET:
            original_investment = ValueFormatter(float(value(self.model.DACInvest[unit])))
            dac_units[unit] = (
                original_investment
                .truncate(decimal=1)
                .round_up(decimal=0)
                .get_formatted_value()
            )
        return dac_units            
        
    #get hourly operational costs        
    #used for all NET units after linear re-solve
    def get_hourly_gencost_NET_LP(self, generator):
        if not hasattr(self.model, 'FuelPriceLP'):
            raise Exception('Missing call to get_operational_costs_disagg_LP')
        results = np.zeros(self.num_snaphots)    
        for time in range(self.num_snaphots):
            results[time] = value((self.model.FuelPriceNETLP[generator,time] / self.model.FuelEffNETLP[generator] + self.model.CO2PriceNETLP[generator,time] * self.model.CO2RateNETLP[generator] + self.model.NonFuelVOMNETLP[generator]) * self.model.PowerConsumed[generator,time])
        return results        
            

            
            
            
            
            
            