from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np
from value_format import ValueFormatter

class ConventionalGeneratorsCHInvestContinuous: 
    def __init__(self, state, gens, candidate_gens, existing_gens):
        self.state = state
        self.model = state.model
        self.generators = gens
        self.model.CandGens = candidate_gens #candidate gens
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        self.model.GenBuild = Var(
        self.generators,
        within=NonNegativeReals, bounds=(0,1)) #continuous variable to indicate whether a generator is built
        for Id in existing_gens:
            self.model.GenBuild[Id].fix(1) #set the value of GenBuild variable to 1 for all gens that exist

        self.model.NoPowerConsumedLinearCH = Constraint(
            self.generators,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    def _pgen_before_linear(self, g, t):
        if t == 0:
            return self.model.PowerGeneratedT0LinearCH[g]
        else:
            return self.model.PowerGenerated[g, t - 1]

    """
    set power output of units at t=0 
    """    
    def set_pgen_t0_linear(self, GENS, power_generated_t0):
        self.model.PowerGeneratedT0LinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: power_generated_t0[g])   

    """
    set minimum generation levels
    """
    def set_min_power_CH_linear(self, GENS, min_power):
        self.model.MinPowerLinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.MinPowerConCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] >= 0)
        
    """
    set maximum generation levels
    """
    def set_max_power_CH_linear(self, GENS, max_power):
        self.model.MaxPowerLinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.MaxPowerConCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerLinearCH[g] * self.model.GenBuild[g])
    
    """
    set ramp limits 
    """
    def set_ramp_linear(self, GENS, ramp_up, ramp_down):
        if not hasattr(self.model, 'PowerGeneratedT0LinearCH'):
            raise Exception('Missing call to set_pgen_t0_linear')
        if not hasattr(self.model, 'MaxPowerLinearCH'):
            raise Exception('Missing call to set_max_power_CH_linear')
        if not hasattr(self.model, 'MinPowerLinearCH'):
            raise Exception('Missing call to set_min_power_CH_linear')
        #ramp limit parameters
        self.model.RampUpLimitLinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_up[g])
        self.model.RampDownLimitLinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_down[g])
        #ramp down constraint
        self.model.RampDownCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._pgen_before_linear(g, t) - m.PowerGenerated[g,t] <= m.RampDownLimitLinearCH[g] * self.model.GenBuild[g])
        #ramp up constraint
        self.model.RampUpCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - self._pgen_before_linear(g, t) <= m.RampUpLimitLinearCH[g] * self.model.GenBuild[g])

    """
    set ramp limits considering reserve provision
    """
    def set_ramp_linear_reserves(self, GENS, ramp_up, ramp_down):
        if not hasattr(self.model, 'PowerGeneratedT0LinearCH'):
            raise Exception('Missing call to set_pgen_t0_linear')
        if not hasattr(self.model, 'MaxPowerLinearCH'):
            raise Exception('Missing call to set_max_power_CH_linear')
        if not hasattr(self.model, 'MinPowerLinearCH'):
            raise Exception('Missing call to set_min_power_CH_linear')
        #secondary and tertiary reserves variables 
        self.model.ActualUpFRReserveCHLinear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveCHLinear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownFRReserveCHLinear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveCHLinear = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        #ramp limit parameters
        self.model.RampUpLimitLinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_up[g])
        self.model.RampDownLimitLinearCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_down[g])
        #ramp down constraints
        self.model.RampDown0LinearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._pgen_before_linear(g, t) - m.PowerGenerated[g,t] + (m.ActualDownFRReserveCHLinear[g,t] + m.ActualDownRRReserveCHLinear[g,t]) <= m.RampDownLimitLinearCH[g] * self.model.GenBuild[g])
        self.model.RampDown1_2LinearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - (m.ActualDownFRReserveCHLinear[g,t] + m.ActualDownRRReserveCHLinear[g,t]) >= 0) #can not provide down reserve if at 0
        self.model.RampDown2LinearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveCHLinear[g,t] + m.ActualDownRRReserveCHLinear[g,t] <= m.RampDownLimitLinearCH[g] * self.model.GenBuild[g])
        #ramp up constraints
        self.model.RampUp0LinearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - self._pgen_before_linear(g, t) + (m.ActualUpFRReserveCHLinear[g,t] + m.ActualUpRRReserveCHLinear[g,t]) <= m.RampUpLimitLinearCH[g] * self.model.GenBuild[g])
        self.model.RampUp1LinearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] + (m.ActualUpFRReserveCHLinear[g,t] + m.ActualUpRRReserveCHLinear[g,t]) <= m.MaxPowerLinearCH[g] * self.model.GenBuild[g])
        self.model.RampUp2LinearCHCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveCHLinear[g,t] + m.ActualUpRRReserveCHLinear[g,t] <= m.RampUpLimitLinearCH[g] * self.model.GenBuild[g])
        
    """
    sets FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator
    """
    def set_FRR_RR_Linear(self, reserves, GENS):        
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveCHLinear[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveCHLinear[g,t]
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveCHLinear[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveCHLinear[g,t]      

    """
    Generates an expression for the investment costs of all conventional generators in the system
    """
    def get_investment_cost_convCHlinear(self, max_power_cand, investment_cost_gens, fixed_cost_gens):
        self.model.InvCostConvGenLinear = Param(
            self.model.CandGens,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_gens[g])
        self.model.MaxPowerCand = Param(
            self.model.CandGens,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_cand[g])
        m = self.model
        if fixed_cost_gens:
            return sum((m.InvCostConvGenLinear[g] * m.MaxPowerCand[g] + fixed_cost_gens[g] * self.model.MaxPowerCand[g]) * m.GenBuild[g] for g in m.CandGens) 
        else: 
            return sum(m.InvCostConvGenLinear[g] * m.MaxPowerCand[g]* m.GenBuild[g] for g in m.CandGens)

    """
    post-processing routines
    """             
    def frr_up_conv_CHLinear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveCHLinear[generator,time])
        return results
    
    def frr_down_conv_CHLinear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveCHLinear[generator,time])
        return results
            
    def rr_up_conv_CHLinear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReserveCHLinear[generator,time])
        return results  
    
    def rr_down_conv_CHLinear(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReserveCHLinear[generator,time])
        return results   

    def get_gens_built(self):
        generators = {}
        for g in self.model.CandGens:
            original_capacity = ValueFormatter(float(value(self.model.GenBuild[g])))
            generators[g] = (
                original_capacity
                .truncate(decimal=3)
                .round_up(decimal=2)
                .get_formatted_value()
            )
        return generators        