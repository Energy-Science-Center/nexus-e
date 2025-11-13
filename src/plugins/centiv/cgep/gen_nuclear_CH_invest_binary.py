from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class NuclearGeneratorsCHInvestBinary:
    def __init__(self, state, candidate_nucs):
        self.state = state
        self.model = state.model
        self.model.CandNucGens = candidate_nucs #candidate nuclear gens
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        self.model.GenBuildNuclear = Var(
        self.model.CandNucGens,
        within=Binary) #binary variable to indicate whether a nuclear generator is built (1) or not (0)

        self.model.NoPowerConsumedNuclearCH = Constraint(
            self.model.CandNucGens,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    def _pgen_before_nuclear(self, g, t):
        if t == 0:
            return self.model.PowerGeneratedT0NuclearCH[g]
        else:
            return self.model.PowerGenerated[g, t - 1]

    """
    set power output of units at t=0 
    """    
    def set_pgen_t0_nuclear(self, GENS, power_generated_t0):
        self.model.PowerGeneratedT0NuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: power_generated_t0[g])   

    """
    set minimum generation levels
    """
    def set_min_power_CH_nuclear(self, GENS, min_power):
        self.model.MinPowerNuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.MinPowerConNuclearCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] >= 0)
        
    """
    set maximum generation levels
    """
    def set_max_power_CH_nuclear(self, GENS, max_power):
        self.model.MaxPowerNuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.MaxPowerConNuclearCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerNuclearCH[g] * self.model.GenBuildNuclear[g])
    
    """
    set ramp limits 
    """
    def set_ramp_nuclear(self, GENS, ramp_up, ramp_down):
        if not hasattr(self.model, 'PowerGeneratedT0NuclearCH'):
            raise Exception('Missing call to set_pgen_t0_nuclear')
        if not hasattr(self.model, 'MaxPowerNuclearCH'):
            raise Exception('Missing call to set_max_power_CH_nuclear')
        if not hasattr(self.model, 'MinPowerNuclearCH'):
            raise Exception('Missing call to set_min_power_CH_nuclear')
        #ramp limit parameters
        self.model.RampUpLimitNuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_up[g])
        self.model.RampDownLimitNuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_down[g])
        #ramp down constraint
        self.model.RampDownConNuclear = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._pgen_before_nuclear(g, t) - m.PowerGenerated[g,t] <= m.RampDownLimitNuclearCH[g] * self.model.GenBuildNuclear[g])
        #ramp up constraint
        self.model.RampUpConNuclear = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - self._pgen_before_nuclear(g, t) <= m.RampUpLimitNuclearCH[g] * self.model.GenBuildNuclear[g])

    """
    set ramp limits considering reserve provision
    """
    def set_ramp_nuclear_reserves(self, GENS, ramp_up, ramp_down):
        if not hasattr(self.model, 'PowerGeneratedT0NuclearCH'):
            raise Exception('Missing call to set_pgen_t0_nuclear')
        if not hasattr(self.model, 'MaxPowerNuclearCH'):
            raise Exception('Missing call to set_max_power_CH_nuclear')
        if not hasattr(self.model, 'MinPowerNuclearCH'):
            raise Exception('Missing call to set_min_power_CH_nuclear')
        #secondary and tertiary reserves variables 
        self.model.ActualUpFRReserveCHNuclear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveCHNuclear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownFRReserveCHNuclear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveCHNuclear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        #ramp limit parameters
        self.model.RampUpLimitNuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_up[g])
        self.model.RampDownLimitNuclearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_down[g])
        #ramp down constraints
        self.model.RampDown0NuclearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._pgen_before_nuclear(g, t) - m.PowerGenerated[g,t] + (m.ActualDownFRReserveCHNuclear[g,t] + m.ActualDownRRReserveCHNuclear[g,t]) <= m.RampDownLimitNuclearCH[g] * self.model.GenBuildNuclear[g])
        self.model.RampDown1NuclearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - (m.ActualDownFRReserveCHNuclear[g,t] + m.ActualDownRRReserveCHNuclear[g,t]) >= 0) #can not provide down reserve if at 0
        self.model.RampDown2NuclearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveCHNuclear[g,t] + m.ActualDownRRReserveCHNuclear[g,t] <= m.RampDownLimitNuclearCH[g] * self.model.GenBuildNuclear[g])
        #ramp up constraints
        self.model.RampUp0NuclearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - self._pgen_before_nuclear(g, t) + (m.ActualUpFRReserveCHNuclear[g,t] + m.ActualUpRRReserveCHNuclear[g,t]) <= m.RampUpLimitNuclearCH[g] * self.model.GenBuildNuclear[g])
        self.model.RampUp1NuclearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] + (m.ActualUpFRReserveCHNuclear[g,t] + m.ActualUpRRReserveCHNuclear[g,t]) <= m.MaxPowerNuclearCH[g] * self.model.GenBuildNuclear[g])
        self.model.RampUp2NuclearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveCHNuclear[g,t] + m.ActualUpRRReserveCHNuclear[g,t] <= m.RampUpLimitNuclearCH[g] * self.model.GenBuildNuclear[g])
        
    """
    sets FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator
    """
    def set_FRR_RR_Nuclear(self, reserves, GENS):        
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveCHNuclear[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveCHNuclear[g,t]
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveCHNuclear[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveCHNuclear[g,t]      

    """
    Generates an expression for the investment costs of all nuclear generators in the system
    """
    def get_investment_cost_convCHNuclear(self, max_power_cand, investment_cost_gens, fixed_cost_gens):
        self.model.InvCostNuclear = Param(
            self.model.CandNucGens,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_gens[g])
        self.model.MaxPowerCandNuclear = Param(
            self.model.CandNucGens,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_cand[g])
        m = self.model
        if fixed_cost_gens:
            return sum((m.InvCostNuclear[g] * m.MaxPowerCandNuclear[g] + fixed_cost_gens[g] * self.model.MaxPowerCandNuclear[g]) * m.GenBuildNuclear[g] for g in m.CandNucGens) 
        else: 
            return sum(m.InvCostNuclear[g] * m.MaxPowerCandNuclear[g] * m.GenBuildNuclear[g] for g in m.CandNucGens)

    """
    post-processing routines
    """             
    def frr_up_conv_CHNuclear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveCHNuclear[generator,time])
        return results
    
    def frr_down_conv_CHNuclear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveCHNuclear[generator,time])
        return results
            
    def rr_up_conv_CHNuclear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReserveCHNuclear[generator,time])
        return results  
    
    def rr_down_conv_CHNuclear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReserveCHNuclear[generator,time])
        return results   

    def get_gens_built_nuclear(self):
        generators = {}
        for g in self.model.CandNucGens:
            generators[g] = int(value(self.model.GenBuildNuclear[g]))
        return generators        