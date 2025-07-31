from __future__ import division
from pyomo.environ import *  # @UnusedWildImport
from pyomo.opt import SolverFactory,SolverStatus,TerminationCondition  # @Reimport

import logging

logging.getLogger('pyomo.core').setLevel(logging.ERROR)

import numpy as np
import pandas as pd

class CostOptimization(object):
    def __init__(self, num_generators, num_snaphots):
        self.num_generators = num_generators
        self.num_snaphots = num_snaphots
        self.solver = "gurobi"
        self.model = ConcreteModel()
        self.model.dual = Suffix(direction=Suffix.IMPORT)
        
        self.model.Generators = RangeSet(0, num_generators - 1)
        self.model.TimePeriods = RangeSet(0, num_snaphots - 1)
        #self.up_reserves_at = [{} for _ in  range(num_snaphots)] #holder for total up reserves provided by each generator at each time period
        #self.down_reserves_at = [{} for _ in range(num_snaphots)] #holder for total down reserves provided by each generator at each time period
        
        #self.up_FCR_reserves_at = [{} for _ in range(num_snaphots)] #holder for FCR up reserves provided by each generator at each time period
        #self.down_FCR_reserves_at = [{} for _ in range(num_snaphots)] #holder for FRR up reserves provided by each generator at each time period
        self.up_FRR_reserves_at = [{} for _ in range(num_snaphots)] #holder for RR up reserves provided by each generator at each time period
        self.down_FRR_reserves_at = [{} for _ in range(num_snaphots)] #holder for FCR down reserves provided by each generator at each time period
        self.up_RR_reserves_at = [{} for _ in range(num_snaphots)] #holder for FRR down reserves provided by each generator at each time period
        self.down_RR_reserves_at = [{} for _ in range(num_snaphots)] #holder for RR down reserves provided by each generator at each time period

        """
        Optimization Variables
        """
        #self.model.PowerGenerated = Var(self.model.Generators, self.model.TimePeriods, within=Reals) #power produced by each generator at each time period (can be negative in case we have a battery that is charging)
        self.model.MaximumPowerAvailable = Var(self.model.Generators, self.model.TimePeriods, within=NonNegativeReals) #maximum power produced by each generator at each time period
        self.model.UnitOn = Var(self.model.Generators, self.model.TimePeriods, within=IntegerSet(bounds=(0,1))) #on/off status of each generator at each time period (in case of storage 1 is for discharging aka production, 0 for charging aka consumption) 
        self.model.SoC = Var(self.model.Generators, self.model.TimePeriods, within=NonNegativeReals) #State of charge (SoC) for a generic energy storage unit - used to model battery storage
        self.model.PowerConsumed = Var(self.model.Generators, self.model.TimePeriods, within=NonNegativeReals) #power consumed by each generator at each time period
        self.model.PowerGenerated = Var(self.model.Generators, self.model.TimePeriods, within=NonNegativeReals) #power produced by each generator at each time period

    def all_gen(self):
        return self.model.Generators

    def select_gen(self, l):
        return SetOf(l)

    def _uniton_before(self, g, t):
        if t == 0:
            return self.model.UnitOnT0[g]
        else:
            return self.model.UnitOn[g, t - 1]

    def _pgen_before(self, g, t):
        if t == 0:
            return self.model.PowerGeneratedT0[g]
        else:
            return self.model.PowerGenerated[g, t - 1]

    """
    set which units are operational at t=0 
    """
    def set_initial_status(self, GENS, generator_t0_state):
        unitsOn = { g: 1 if generator_t0_state[g] > 0 else 0 for g in GENS } #1 for indices of switched on generators
        for x in GENS:
            if generator_t0_state[x] == 0:
                raise Exception('State 0 is not valid')
        self.model.UnitOnT0 = Param(
            GENS,
            within=Binary,
            initialize=lambda m,g: unitsOn[g])
        self.model.UnitOnT0State = Param(
            GENS,
            within=Integers,
            initialize=lambda m,g: generator_t0_state[g]) 

    """
    set power output of units at t=0 
    """    
    def set_pgen_t0(self, GENS, power_generated_t0):
        self.model.PowerGeneratedT0 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: power_generated_t0[g])           

    """
    set production equals demand 
    """ 
    def set_demand(self, demand):
        self.model.Demand = Param(
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,t: demand[t])
 
        def production_constraint(m,t):
            result = sum(m.PowerGenerated[g,t] for g in m.Generators)
            if hasattr(self, 'PowerConsumed'):
                result -= sum(m.PowerConsumed[g,t] for g in m.Generators)
            return result == m.Demand[t]
        
        self.model.ProductionEqualsDemandCon = Constraint(
            self.model.TimePeriods,
            rule=production_constraint)

    """
    set minimum generation levels for each generator (units in MW or p.u.)
    """
    def set_min_power(self, GENS, min_power):
        self.model.MinPower = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.MinPowerCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0)   #conventional generators do not have storage capabilities
        self.model.MinPowerCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MinPower[g])  
        
    """
    set maximum generation levels for each generator (units in MW or p.u.)
    """
    def set_max_power(self, GENS, max_power):
        self.model.MaxPower = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.MaxPowerCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPower[g])
        
    """
    limits for power generated in each time period (1/2)
    this splits the constraint MinimumPowerOutput[g] * m.UnitOn[g, t] <= m.PowerGenerated[g,t] <= m.MaximumPowerAvailable[g, t] into two:
    1) MinPowerOutput[g]*UnitOn[g,t] <= PowerGenerated[g,t]
    2) PowerGenerated[g,t] <= MaximumPowerAvailable[g,t])
    """
    def set_genlimits_min_power(self, GENS, min_power):
        self.model.MinPower = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.GenLimitsPerPeriodCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0)   #conventional generators do not have storage capabilities
        self.model.GenLimitsPerPeriodCon1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinPower[g] * m.UnitOn[g,t] <= m.PowerGenerated[g,t])
        self.model.GenLimitsPerPeriodCon2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaximumPowerAvailable[g,t])

    """
    limits for power generated in each time period (2/2)
    this sets the constraint 0 <= MaximumPowerAvailable[g,t] <= MaxPower[g]*UnitOn[g,t]
    """
    def set_genlimits_max_power(self, GENS, max_power):
        self.model.MaxPower = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.GenLimitsPerPeriodCon3 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MaximumPowerAvailable[g,t] <= m.MaxPower[g]*m.UnitOn[g,t])
    
    """
    limits MaximumPowerAvailable[g,t] by ramp-up and startup ramp rates.
    set_initial_status must have been already called to set which units are operational at t=0 
    set_genlimits_max_power must have been already called to get Min/MaxPower
    """
    def set_genlimits_ramp_up(self, GENS, start_up_ramp_limit, nominal_ramp_up_limit):
        if not hasattr(self.model, 'UnitOnT0'):
            raise Exception('Missing call to set_initial')
        if not hasattr(self.model, 'PowerGeneratedT0'):
            raise Exception('Missing call to set_pgen_t0')
        if not hasattr(self.model, 'MaxPower'):
            raise Exception('Missing call to set_genlimits_max_power')
        if hasattr(self.model, 'MinPower'):
            for i in GENS:
                assert start_up_ramp_limit[i] >= self.model.MinPower[i]
                assert start_up_ramp_limit[i] <= self.model.MaxPower[i]
        self.model.StartUpRampLimit = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_ramp_limit[g])
        self.model.NominalRampUpLimit = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nominal_ramp_up_limit[g])
        self.model.RampUpStartUpRampCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    m.MaximumPowerAvailable[g,t] <= self._pgen_before(g,t) +
                                                    m.NominalRampUpLimit[g] * self._uniton_before(g,t) +
                                                    m.StartUpRampLimit[g] * (m.UnitOn[g,t] - self._uniton_before(g,t)) +
                                                    m.MaxPower[g] * (1 - m.UnitOn[g,t]))
                    
    """
    limits MaximumPowerAvailable[g,t] by ramp-down and shutdown ramp rates
    limits PowerGenerated[g,t] w.r.t ramp-down rates 
    set_genlimits_max_power/min_power must have been already called to get MaxPower/MinPower
    set_initial must have been already called to get UnitOnT0
    set_pgen_t0 must have been already called to get PowerGeneratedT0
    """
    def set_genlimits_ramp_down(self, GENS, shut_down_ramp_limit, nominal_ramp_down_limit):
        if not hasattr(self.model, 'UnitOnT0'):
            raise Exception('Missing call to set_initial')
        if not hasattr(self.model, 'PowerGeneratedT0'):
            raise Exception('Missing call to set_pgen_t0')
        if not hasattr(self.model, 'MaxPower'):
            raise Exception('Missing call to set_genlimits_max_power')
        if hasattr(self.model, 'MinPower'):
            for i in GENS:
                assert shut_down_ramp_limit[i] >= self.model.MinPower[i]
                assert shut_down_ramp_limit[i] <= self.model.MaxPower[i]
        self.model.ShutDownRampLimit = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: shut_down_ramp_limit[g])
        self.model.NominalRampDownLimit = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nominal_ramp_down_limit[g])
        self.model.RampDownShutDownRampCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    Constraint.Skip if (t == self.num_snaphots-1) else
                    m.MaximumPowerAvailable[g,t] <= m.MaxPower[g] * m.UnitOn[g,t+1] + 
                                                    m.ShutDownRampLimit[g] * (m.UnitOn[g,t] - m.UnitOn[g,t+1]))
        self.model.RampDownPowerOutCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    self._pgen_before(g, t) - m.PowerGenerated[g,t] <= m.NominalRampDownLimit[g] * m.UnitOn[g,t] +
                                                                    m.ShutDownRampLimit[g] * (self._uniton_before(g,t) - m.UnitOn[g,t]) + 
                                                                    m.MaxPower[g] * (1 - self._uniton_before(g,t)))
                                                          
    """
    constraint for min. up time for both initial and non-initial conditions
    """
    def set_up_time(self, GENS, minimum_uptime):
        if not hasattr(self.model, 'UnitOnT0State'):
            raise Exception('Missing call to set_initial_status') 
        if not hasattr(self.model, 'UnitOnT0'):
            raise Exception('Missing call to set_initial_status') 
        self.model.MinimumUpTime = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: minimum_uptime[g])
        self.model.InitialTimeOnLine = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: min(self.num_snaphots, max(0,m.MinimumUpTime[g] - m.UnitOnT0State[g])) if m.UnitOnT0[g] == 1 else 0)
        self.model.MinimumUpTimeAtT = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: min(minimum_uptime[g], self.num_snaphots-t))
        self.model.UpTimeInitialCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] == 1 if t < m.InitialTimeOnLine[g] else Constraint.Skip)
        self.model.UpTimeNonInitialCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    sum(m.UnitOn[g,t1] for t1 in self.model.TimePeriods if t1 >= t and t1 < t + minimum_uptime[g] and t1 < self.num_snaphots) >= self.model.MinimumUpTimeAtT[g,t] * (m.UnitOn[g,t] - self._uniton_before(g,t)))
    
    """
    constraint for min. down time for both initial and non-initial conditions
    """
    def set_down_time(self, GENS, minimum_downtime):
        if not hasattr(self.model, 'UnitOnT0State'):
            raise Exception('Missing call to set_initial_status') 
        if not hasattr(self.model, 'UnitOnT0'):
            raise Exception('Missing call to set_initial_status') 
        self.model.MinimumDownTime = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: minimum_downtime[g])
        self.model.InitialTimeOffLine = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: min(self.num_snaphots, max(0, m.MinimumDownTime[g] + m.UnitOnT0State[g])) if m.UnitOnT0[g] == 0 else 0)
        self.model.MinimumDownTimeAtT = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: min(minimum_downtime[g], self.num_snaphots-t))
        self.model.DownTimeInitialCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] == 0 if t < m.InitialTimeOffLine[g] else Constraint.Skip)
        self.model.DownTimeNonInitialCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    sum(1 - m.UnitOn[g,t1] for t1 in self.model.TimePeriods if t1 >= t and t1 < t + minimum_downtime[g] and t1 < self.num_snaphots) >= self.model.MinimumDownTimeAtT[g,t] * (self._uniton_before(g,t) - m.UnitOn[g,t]))

    """
    Sets FCR (Frequency Containment Reserve) limits for each generator.
        gen_up and gen_down for each generator.
        system_up_requirements and system_down_requirements at each timeperiod.
    """
    def set_FCR(self, GENS, gen_up, gen_down, system_up_requirements, system_down_requirements):
        if not hasattr(self.model, 'MaxPower'):
            raise Exception('Missing call to set_genlimits_max_power')
        if not hasattr(self.model, 'MinPower'):
            raise Exception('Missing call to set_genlimits_min_power')
        
        self.model.ActualUpFCReserve = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.FCUpGenLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFCReserve[g,t] <= gen_up[g] * m.UnitOn[g,t]) 
        self.model.FCUpMaxPowerLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFCReserve[g,t] <= m.MaxPower[g] * m.UnitOn[g,t] - m.PowerGenerated[g,t]) 
        self.model.FCUpReserve = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(m.ActualUpFCReserve[g,t] for g in GENS) >= system_up_requirements[t])

        self.model.ActualDownFCReserve = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.FCDownGenLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFCReserve[g,t] <= gen_down[g] * m.UnitOn[g,t])
        self.model.FCDownMaxPowerLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFCReserve[g,t] <= m.PowerGenerated[g,t] - m.MinPower[g] * m.UnitOn[g,t])
        self.model.FCDownReserve = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(m.ActualDownFCReserve[g,t] for g in GENS) >= system_down_requirements[t])

    """
    Sets FCR (Frequency Containment Reserve), FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator.
        gen_up_FCR/FRR/RR and gen_down_FCR/FRR/RR for each generator.
        system_up_requirements_FCR/FRR/RR and system_down_requirements_FCR/FRR/RR for each timeperiod.
    """
    def set_FCR_FRR_RR(self, GENS, gen_up_FCR, gen_down_FCR, gen_up_FRR, gen_down_FRR, gen_up_RR, gen_down_RR):        
        if not hasattr(self.model, 'MaxPower'):
            raise Exception('Missing call to set_genlimits_max_power')
        if not hasattr(self.model, 'MinPower'):
            raise Exception('Missing call to set_genlimits_min_power')
        
        #self.model.ActualUpFCReserve = Var(
        #    GENS,
        #    self.model.TimePeriods,
        #    within=NonNegativeReals)
        #self.model.FCUpGenLimit = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReserve[g,t] <= gen_up_FCR[g] * m.UnitOn[g,t])
        self.model.ActualUpFRReserve = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserve = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.RUpGenLimit0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserve[g,t] <= gen_up_FRR[g] * m.UnitOn[g,t])
        self.model.RUpGenLimit1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpRRReserve[g,t] <= gen_up_RR[g] * m.UnitOn[g,t])
        self.model.RUpGenLimit3 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserve[g,t] + m.ActualUpRRReserve[g,t] <= gen_up_RR[g] * m.UnitOn[g,t])
        self.model.ReservesUpMaxPowerLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserve[g,t] + m.ActualUpRRReserve[g,t] <= m.MaxPower[g] * m.UnitOn[g,t] - m.PowerGenerated[g,t])
        for t in self.model.TimePeriods:
            for g in GENS:
                #self.up_reserves_at[t][g] = self.model.ActualUpFCReserve[g,t] + self.model.ActualUpFRReserve[g,t] + self.model.ActualUpRRReserve[g,t]
                #self.up_FCR_reserves_at[t][g] = self.model.ActualUpFCReserve[g,t]
                self.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserve[g,t]
                self.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserve[g,t]

        #self.model.ActualDownFCReserve = Var(
        #    GENS,
        #    self.model.TimePeriods,
        #    within=NonNegativeReals)
        #self.model.FCDownGenLimit = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReserve[g,t] <= gen_down_FCR[g] * m.UnitOn[g,t])
        self.model.ActualDownFRReserve = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserve = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.RDownGenLimit0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserve[g,t] <= gen_down_FRR[g] * m.UnitOn[g,t])
        self.model.RDownGenLimit1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownRRReserve[g,t] <= gen_down_RR[g] * m.UnitOn[g,t])
        self.model.RDownGenLimit2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserve[g,t] + m.ActualDownRRReserve[g,t] <= gen_down_RR[g] * m.UnitOn[g,t])
        self.model.ReservesDownMaxPowerLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserve[g,t] + m.ActualDownRRReserve[g,t] <= m.PowerGenerated[g,t] - m.MinPower[g] * m.UnitOn[g,t])
        for t in self.model.TimePeriods:
            for g in GENS:
                #self.down_reserves_at[t][g] = self.model.ActualDownFCReserve[g,t] + self.model.ActualDownFRReserve[g,t] + self.model.ActualDownRRReserve[g,t]
                #self.down_FCR_reserves_at[t][g] = self.model.ActualDownFCReserve[g,t]
                self.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserve[g,t]
                self.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserve[g,t]
                    
    def set_no_reserves(self, GENS):  
        for t in self.model.TimePeriods:
            for g in GENS:
                #self.up_reserves_at[t][g] = 0
                #self.up_FCR_reserves_at[t][g] = 0
                self.up_FRR_reserves_at[t][g] = 0
                self.up_RR_reserves_at[t][g] = 0

                #self.down_reserves_at[t][g] = 0
                #self.down_FCR_reserves_at[t][g] = 0
                self.down_FRR_reserves_at[t][g] = 0
                self.down_RR_reserves_at[t][g] = 0
                
    def set_no_RRreserves(self, GENS): #needs to be called for all generator types which do not provide tertiary reserves  
        for t in self.model.TimePeriods:
            for g in GENS:
                self.up_RR_reserves_at[t][g] = 0
                self.down_RR_reserves_at[t][g] = 0

    def set_system_reserve_constraints(self, system_up_requirements_FCR, system_down_requirements_FCR, system_up_requirements_FRR, system_down_requirements_FRR, system_up_requirements_RR, system_down_requirements_RR):
        for g in self.model.Generators:
            #if g not in self.up_reserves_at[0]:
            #    raise Exception('Total up reserves not set for generator {}'.format(g))
            #if g not in self.up_FCR_reserves_at[0]:
            #    raise Exception('FCR up reserves not set for generator {}'.format(g))
            if g not in self.up_FRR_reserves_at[0]:
                raise Exception('FRR up reserves not set for generator {}'.format(g))
            if g not in self.up_RR_reserves_at[0]:
                raise Exception('RR up reserves not set for generator {}'.format(g))
            #if g not in self.down_reserves_at[0]:
            #    raise Exception('Total down reserves not set for generator {}'.format(g))
            #if g not in self.down_FCR_reserves_at[0]:
            #    raise Exception('FCR down reserves not set for generator {}'.format(g))
            if g not in self.down_FRR_reserves_at[0]:
                raise Exception('FRR down reserves not set for generator {}'.format(g))
            if g not in self.down_RR_reserves_at[0]:
                raise Exception('RR down reserves not set for generator {}'.format(g))
        
        #self.model.UpReserveFCR = Constraint(
        #    self.model.TimePeriods,
        #    rule=lambda m,t: sum(self.up_FCR_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_FCR[t])
        self.model.UpReserveFRR = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(self.up_FRR_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_FRR[t])
        self.model.UpReserveRR = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(self.up_RR_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_RR[t])
        
        #self.model.DownReserveFCR = Constraint(
        #    self.model.TimePeriods,
        #    rule=lambda m,t: sum(self.down_FCR_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_FCR[t])
        self.model.DownReserveFRR = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(self.down_FRR_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_FRR[t])
        self.model.DownReserveRR = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(self.down_RR_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_RR[t])
              
        #These constraints are used in Philipp's model
        #self.model.UpReserve = Constraint(
        #    self.model.TimePeriods,
        #    rule=lambda m,t: sum(self.up_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_FCR[t] + system_up_requirements_FRR[t] + system_up_requirements_RR[t])
        #self.model.DownReserve = Constraint(
        #    self.model.TimePeriods,
        #    rule=lambda m,t: sum(self.down_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_FCR[t] + system_down_requirements_FRR[t] + system_down_requirements_RR[t])
           
    """
    Sets operational constraints for battery storage from paper "Unit Commitment With Ideal and Generic Energy Storage Units"
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum discharging/charging power for energy storage unit [MW]
        Efficiency rate - discharging/charging [-]
        Initial capacity of storage unit at T0 [MWh]
    """
    def set_battery_storage(self, GENS, min_energy_capacity, max_energy_capacity, max_power_discharge, max_power_charge, discharge_eff, charge_eff, capacityT0):
        self.model.MinEnergyCapacity = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity[g])
        self.model.MaxEnergyCapacity = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity[g])
        self.model.MaxPowerDischarge = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_discharge[g])
        self.model.MaxPowerCharge = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_charge[g]) 
        self.model.EfficiencyDischarge = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: discharge_eff[g])  
        self.model.EfficiencyCharge = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: charge_eff[g])        
        self.model.CapacityT0 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0[g]) 
        
        self.model.MaxPowerGeneratedCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerDischarge[g] * m.UnitOn[g,t])  
        #self.model.MaxPowerStoredCon =  Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MaxPowerCharge[g] * (m.UnitOn[g,t]-1)) 
        self.model.MaxPowerStoredCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerCharge[g] * (1 - m.UnitOn[g,t]))   
        self.model.MinMaxEnergyStoredCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacity[g] <= m.SoC[g,t] and m.SoC[g,t] <= m.MaxEnergyCapacity[g])
        #self.model.SoCCon = Constraint(
        #    GENS, 
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0[g] if t == 0 else m.SoC[g,t-1]) + m.EfficiencyCharge[g] * m.PowerGenerated[g,t] * (m.UnitOn[g,t]-1) - (1/m.EfficiencyDischarge[g]) * m.PowerGenerated[g,t] * m.UnitOn[g,t])
        self.model.SoCCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0[g] if t == 0 else m.SoC[g,t-1]) + m.EfficiencyCharge[g] * m.PowerConsumed[g,t] - (1/m.EfficiencyDischarge[g]) * m.PowerGenerated[g,t])
 
    """
    Sets operational constraints for battery storage providing reserves from paper "Flexible Operation of Batteries in Power System Scheduling With Renewable Energy"
    """
    def set_battery_storage_reserves(self, GENS, gen_up_FCR_batt, gen_down_FCR_batt, gen_up_FRR_batt, gen_down_FRR_batt):
        if not hasattr(self.model, 'MinEnergyCapacity'):
            raise Exception('Missing call to set_battery_storage')
        if not hasattr(self.model, 'MaxEnergyCapacity'):
            raise Exception('Missing call to set_battery_storage')
        if not hasattr(self.model, 'MaxPowerDischarge'):
            raise Exception('Missing call to set_battery_storage')
        if not hasattr(self.model, 'EfficiencyDischarge'):
            raise Exception('Missing call to set_battery_storage')
        if not hasattr(self.model, 'EfficiencyCharge'):
            raise Exception('Missing call to set_battery_storage')
        if not hasattr(self.model, 'CapacityT0'):
            raise Exception('Missing call to set_battery_storage')
        self.model.ActualUpFCReserveBatt = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpFRReserveBatt = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        self.model.RUpFCRLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFCReserveBatt[g,t] <= gen_up_FCR_batt[g] * m.UnitOn[g,t])
        self.model.RUpFRRLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveBatt[g,t] <= gen_up_FRR_batt[g] * m.UnitOn[g,t])
        #self.model.RUpDischargeLimit = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReserveBatt[g,t] + m.ActualUpFRReserveBatt[g,t] <= m.MaxPowerDischarge[g] - m.PowerGenerated[g,t] * m.UnitOn[g,t] + m.PowerGenerated[g,t] * (m.UnitOn[g,t] - 1))
        self.model.RUpDischargeLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFCReserveBatt[g,t] + m.ActualUpFRReserveBatt[g,t] <= m.MaxPowerDischarge[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t])
        self.model.RUpCapacityLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFCReserveBatt[g,t] + m.ActualUpFRReserveBatt[g,t] <= m.EfficiencyDischarge[g] * (m.SoC[g,t] - m.MinEnergyCapacity[g]))
        for t in self.model.TimePeriods:
            for g in GENS:
                self.up_reserves_at[t][g] = self.model.ActualUpFCReserveBatt[g,t] + self.model.ActualUpFRReserveBatt[g,t]
                self.up_FCR_reserves_at[t][g] = self.model.ActualUpFCReserveBatt[g,t]
                self.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveBatt[g,t]

        self.model.ActualDownFCReserveBatt = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownFRReserveBatt = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.RDownFCRLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFCReserveBatt[g,t] <= gen_down_FCR_batt[g] * (1 - m.UnitOn[g,t]))
        self.model.RDownFRRLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveBatt[g,t] <= gen_down_FRR_batt[g] * (1 - m.UnitOn[g,t]))
        #self.model.RDownChargeLimit = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReserveBatt[g,t] + m.ActualDownFRReserveBatt[g,t] <= m.MaxPowerCharge[g] - m.PowerGenerated[g,t] * (m.UnitOn[g,t] - 1) + m.PowerGenerated[g,t] * m.UnitOn[g,t])
        self.model.RDownChargeLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFCReserveBatt[g,t] + m.ActualDownFRReserveBatt[g,t] <= m.MaxPowerCharge[g] - m.PowerConsumed[g,t] + m.PowerGenerated[g,t])
        self.model.RDownCapacityLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFCReserveBatt[g,t] + m.ActualDownFRReserveBatt[g,t] <= (m.MaxEnergyCapacity[g] - m.SoC[g,t])/m.EfficiencyCharge[g])
        #20% of the scheduled primary and secondary reserve capacity will be activated 
        #Should multiply these by the minimum duration of time that we must maintain FCR/FRR (right now it is hour) 
        #self.model.RSoCCon = Constraint(
        #    GENS, 
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0[g] if t == 0 else m.SoC[g,t-1]) + m.EfficiencyCharge[g] * m.PowerGenerated[g,t] * (m.UnitOn[g,t]-1) - (1/m.EfficiencyDischarge[g]) * m.PowerGenerated[g,t] * m.UnitOn[g,t] + 
                                                #0.2 * (m.ActualDownFCReserveBatt[g,t] * m.EfficiencyCharge[g]  - m.ActualUpFCReserveBatt[g,t]/m.EfficiencyDischarge[g]) +
                                                #0.2 * (m.ActualDownFRReserveBatt[g,t] * m.EfficiencyCharge[g]  - m.ActualUpFRReserveBatt[g,t]/m.EfficiencyDischarge[g]))

        for t in self.model.TimePeriods:
            for g in GENS:
                self.down_reserves_at[t][g] = self.model.ActualDownFCReserveBatt[g,t] + self.model.ActualDownFRReserveBatt[g,t]
                self.down_FCR_reserves_at[t][g] = self.model.ActualDownFCReserveBatt[g,t]
                self.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveBatt[g,t]

    """
    Sets the constraints for wind power generation using hourly Wind Capacity Factor (this is an approximate formulation)
    """
    def set_wind_power(self, GENS, CF_wind, P_nom):
        self.model.PowerWind = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: P_nom[g])
        self.model.CapacityFactorWind = Param(
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,t: CF_wind[t])
        self.model.PowerCFWindCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorWind[t] * m.PowerWind[g])
        self.model.PowerCFWindCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0)
    
    """
    Sets the equations for wind power production using linearized power curve (this formulation requires hourly wind speeds and turbine specs)
    no wake losses considered
    """
    def set_wind_power_curve(self, GENS, P_nom, wind_speed, cut_in_wind_speed, cut_off_wind_speed, rated_wind_speed, P_max):
        self.model.PowerWindNom = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: P_nom[g]) #nominal power of wind turbine
        self.model.WindSpeed = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: wind_speed[g][t])
        self.model.Cut_In_WindSpeed = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: cut_in_wind_speed[g])
        self.model.Cut_Off_WindSpeed = Param(
            GENS, 
            within=NonNegativeReals, 
            initialize=lambda m,g: cut_off_wind_speed[g])
        self.model.RatedWindSpeed = Param(
            GENS, 
            within=NonNegativeReals, 
            initialize=lambda m,g: rated_wind_speed[g])
        self.model.PmaxWind = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: P_max[g]) #this is the max installed power of the wind park 

        def constraint_equation_wind(m,g,t):
            if wind_speed[g][t] < cut_in_wind_speed[g] or wind_speed[g][t] > cut_off_wind_speed[g]:
                return (m.PowerGenerated[g,t] == 0) #no power is generated because we are either below cut-in or above cut-off wind speed 
            elif wind_speed[g][t] >= rated_wind_speed[g] and wind_speed[g][t] <= cut_off_wind_speed[g]:
                return (0 <= m.PowerGenerated[g,t] <= m.PmaxWind[g]) #maximum power output (we are above rated wind speed but below the cut-off)
            elif wind_speed[g][t] >= cut_in_wind_speed[g] and wind_speed[g][t] < rated_wind_speed[g]:
                return (0 <= m.PowerGenerated[g,t] <= m.PmaxWind[g] * ((m.WindSpeed[g,t] - m.Cut_In_WindSpeed[g]) / (m.RatedWindSpeed[g] - m.Cut_In_WindSpeed[g]))) #we are on the linear part of the power curve (the power generated is non-zero and below nominal power)
            else: 
                raise Exception('Operation impossible with the given inputs')

        self.model.WindPowerPowerCurveCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=constraint_equation_wind) 
        self.model.WindPowerPowerCurveCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0)
             
    """
    Sets the constraints for solar PV by calculating hourly PV Capacity Factor for each power plant from the solar irradiation at the given location
    """   
    def set_pv_power(self, GENS, solar_rad, Pmax_PV, pv_maxrad):
        self.model.CapacityFactorPV = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: solar_rad[g][t]/pv_maxrad)
        self.model.MaxPowerPV = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_PV[g])
        self.model.PowerCFPVCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorPV[g,t] * m.MaxPowerPV[g])
        self.model.PowerCFPVCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0)        
    """
    Sets operational constraints for run-of-river hydro power plants using hourly capacity fctors per power plant
    Turbine efficiency is neglected
    """   
    def set_hydro_power_RoR(self, GENS, CF_Hydro, Pmax_Hydro):  
        self.model.CapacityFactorHydro = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CF_Hydro[g][t])
        self.model.PmaxHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_Hydro[g])
        self.model.PowerHydroCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorHydro[g,t] * m.PmaxHydro[g])
        self.model.PowerHydroCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0)
    """
    Sets operational constraints for hydro dams
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum turbine power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh]
        Inflows [Capacity Factor]
    Turbine efficiency is neglected
    No spillage allowed 
    """
    def set_hydro_power_dam(self, GENS, min_energy_capacity_dam, max_energy_capacity_dam, max_power_turbine_dam, min_power_turbine_dam, capacityT0_dam, hourly_inflows_dam, eff_turbine):
        self.model.MinEnergyCapacityHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_dam[g])
        self.model.MaxEnergyCapacityHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_dam[g])
        self.model.MaxPowerTurbineHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_dam[g])  
        #self.model.MinPowerTurbineHydroDam = Param(
        #    GENS,
        #    within=NonNegativeReals,
        #    initialize=lambda m,g: min_power_turbine_dam[g])
        self.model.CapacityT0HydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_dam[g] * m.MaxEnergyCapacityHydroDam[g]) 
        self.model.HourlyInflowsDam = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: hourly_inflows_dam[g][t])
        
        self.model.MaxPowerGeneratedHydroDamCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroDam[g]) #* m.UnitOn[g,t]) 
        #if we assume that Pmin for the dam power plants is 0, we don't need the constraint below
        #self.model.MinPowerGeneratedHydroDamCon = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MinPowerTurbineHydroDam[g]* m.UnitOn[g,t]) #MinPowerTurbineHydroDam[g] is NonNegative so we can't have negative PowerGenerated
        self.model.PowerConsumedHydroDamCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0) 
        
        
        self.model.MinMaxEnergyStoredHydroDamCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityHydroDam[g] <= m.SoC[g,t] and m.SoC[g,t] <= m.MaxEnergyCapacityHydroDam[g])
        #self.model.SoCHydroDamCon = Constraint(
        #    GENS, 
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0HydroDam[g] if t == 0 else m.SoC[g,t-1]) - m.PowerGenerated[g,t] * m.UnitOn[g,t] + m.MaxPowerTurbineHydroDam[g] * m.HourlyInflowsDam[g,t])
        self.model.SoCHydroDamCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0HydroDam[g] if t == 0 else m.SoC[g,t-1]) - m.PowerGenerated[g,t]/eff_turbine + m.MaxPowerTurbineHydroDam[g] * m.HourlyInflowsDam[g,t])
  
    """
    Sets operational constraints for hydro dams for water dcpf
    Cannot be used in combination with set_hydro_power_dam_reserves
    """
    def set_hydro_power_dam_water_dcpf(self, GENS, max_power_turbine_dam, min_power_turbine_dam):
        self.model.MaxPowerTurbineHydroDamWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_dam[g])  
        self.model.MinPowerTurbineHydroDamWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power_turbine_dam[g])
        self.model.PowerConsumedHydroDamWaterDCPFCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0) 
            
        self.model.MaxPowerGeneratedHydroDamWaterDCPFCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroDamWaterDCPF[g] * m.UnitOn[g,t]) 
        #self.model.MinPowerGeneratedHydroDamWaterDCPFCon = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MinPowerTurbineHydroDamWaterDCPF[g] * m.UnitOn[g,t]) #MinPowerTurbineHydroDam[g] is NonNegative so we can't have negative PowerGenerated
    
    """
    Sets reserve constraints for hydro power dams 
    """   
    def set_hydro_power_dam_reserves(self, GENS, gen_up_FCR_dam, gen_down_FCR_dam, gen_up_FRR_dam, gen_down_FRR_dam, gen_up_RR_dam, gen_down_RR_dam):
        #if not hasattr(self.model, 'MinEnergyCapacityHydroDam'):
        #    raise Exception('Missing call to set_hydro_power_dam')
        #if not hasattr(self.model, 'MaxEnergyCapacityHydroDam'):
        #    raise Exception('Missing call to set_hydro_power_dam')
        if not hasattr(self.model, 'MaxPowerTurbineHydroDam'):
            raise Exception('Missing call to set_hydro_power_dam')
        #if not hasattr(self.model, 'MinPowerTurbineHydroDam'):
        #    raise Exception('Missing call to set_hydro_power_dam')
        #if not hasattr(self.model, 'CapacityT0HydroDam'):
        #    raise Exception('Missing call to set_hydro_power_dam')
        #self.model.ActualUpFCReserveDam = Var(
        #    GENS,
        #    self.model.TimePeriods,
        #    within=NonNegativeReals)
        self.model.ActualUpFRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #self.model.RUpFCRLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReserveDam[g,t] <= gen_up_FCR_dam[g] * m.UnitOn[g,t])
        #self.model.RUpFRRLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFRReserveDam[g,t] <= gen_up_FRR_dam[g] * m.UnitOn[g,t])
        #self.model.RUpRRLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpRRReserveDam[g,t] <= gen_up_RR_dam[g] * m.UnitOn[g,t])
        
        #self.model.RUpTurbineLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReserveDam[g,t] + m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t] <= m.MaxPowerTurbineHydroDam[g] * m.UnitOn[g,t] - m.PowerGenerated[g,t])
        self.model.RUpTurbineLimitDam = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t] <= m.MaxPowerTurbineHydroDam[g] - m.PowerGenerated[g,t]) #this means the turbine can provide upward reserve if it is off
        self.model.RUpTurbineLimitDam1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t] <= m.MaxPowerTurbineHydroDam[g])
        
        #self.model.RUpCapacityLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReserveDam[g,t] + m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t] <= (m.SoC[g,t] - m.MinEnergyCapacityHydroDam[g]))
        for t in self.model.TimePeriods:
            for g in GENS:
                #self.up_reserves_at[t][g] = self.model.ActualUpFRReserveDam[g,t] + self.model.ActualUpRRReserveDam[g,t]
                #self.up_FCR_reserves_at[t][g] = self.model.ActualUpFCReserveDam[g,t]
                self.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveDam[g,t]
                self.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveDam[g,t]

        #self.model.ActualDownFCReserveDam = Var(
        #    GENS,
        #    self.model.TimePeriods,
        #    within=NonNegativeReals)
        self.model.ActualDownFRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        #self.model.RDownFCRLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReserveDam[g,t] <= gen_down_FCR_dam[g] * m.UnitOn[g,t])
        #self.model.RDownFRRLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFRReserveDam[g,t] <= gen_down_FRR_dam[g] * m.UnitOn[g,t])
        #self.model.RDownRRLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownRRReserveDam[g,t] <= gen_down_RR_dam[g] * m.UnitOn[g,t])
        
        #self.model.RDownTurbineLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] - (m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t]) <= m.MaxPowerTurbineHydroDam[g])
        self.model.RDownTurbineLimitDam0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t] <= m.PowerGenerated[g,t])
        self.model.RDownTurbineLimitDam1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t] <= m.MaxPowerTurbineHydroDam[g]) #* m.UnitOn[g,t])
        
        
        #self.model.RDownCapacityLimitDam = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReserveDam[g,t] + m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t] <= (m.MaxEnergyCapacityHydroDam[g] - m.SoC[g,t]))
        for t in self.model.TimePeriods:
            for g in GENS:
                #self.down_reserves_at[t][g] = self.model.ActualDownFCReserveDam[g,t] + self.model.ActualDownFRReserveDam[g,t] + self.model.ActualDownRRReserveDam[g,t]
                #self.down_FCR_reserves_at[t][g] = self.model.ActualDownFCReserveDam[g,t]
                self.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveDam[g,t]
                self.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveDam[g,t]
       
    """
    Sets operational constraints for pumped hydro power plants
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum turbine/pump power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh]
        Inflows [Capacity Factor]
    Turbine efficiency is neglected
    Pump efficiency is neglected 
    Lower Reservoir is considered infinite 
    No spillage allowed 
    """
    def set_hydro_power_Pumped(self, GENS, min_energy_capacity_hydro, max_energy_capacity_hydro, max_power_turbine_hydro, max_power_pump_hydro, capacityT0_hydro, hourly_inflows_hydro, eff_pump, eff_turbine):
        self.model.MinEnergyCapacityHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_hydro[g])
        self.model.MaxEnergyCapacityHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_hydro[g])
        self.model.MaxPowerTurbineHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_hydro[g])
        self.model.MaxPowerPumpHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_pump_hydro[g])        
        self.model.CapacityT0Hydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_hydro[g] * m.MaxEnergyCapacityHydro[g]) 
        self.model.HourlyInflows = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: hourly_inflows_hydro[g][t])
        
        self.model.MaxPowerGeneratedHydroCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydro[g]) #* m.UnitOn[g,t])  
        #self.model.MaxPowerStoredHydroCon =  Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MaxPowerPumpHydro[g] * (m.UnitOn[g,t]-1))
        self.model.MaxPowerStoredHydroCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerPumpHydro[g])# * (1 - m.UnitOn[g,t]))
        self.model.MinMaxEnergyStoredHydroCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityHydro[g] <= m.SoC[g,t] and m.SoC[g,t] <= m.MaxEnergyCapacityHydro[g])
        #self.model.SoCHydroCon1 = Constraint(
        #    GENS, 
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0Hydro[g] if t == 0 else m.SoC[g,t-1]) + m.PowerGenerated[g,t] * (m.UnitOn[g,t]-1) - m.PowerGenerated[g,t] * m.UnitOn[g,t] + m.MaxPowerTurbineHydro[g] * m.HourlyInflows[g,t])        
        self.model.SoCHydroCon1 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0Hydro[g] if t == 0 else m.SoC[g,t-1]) - m.PowerGenerated[g,t]/eff_turbine + m.PowerConsumed[g,t]*eff_pump + m.MaxPowerTurbineHydro[g] * m.HourlyInflows[g,t])        
        #self.model.EqualEnergyCon = Constraint(
        #    GENS,
        #    rule=lambda m,g: m.SoC[g,self.num_snaphots-1] == (m.CapacityT0Hydro[g])) #should multiply this constraint by the time step to get energy (in this case our timestep is hour so everything works out)
    
    """
    Sets operational constraints for pumped hydro power plants for water dcpf
    Cannot be used in combination with set_hydro_power_Pumped_reserves
    """
    def set_hydro_power_Pumped_water_dcpf(self, GENS, max_power_turbine_hydro, max_power_pump_hydro):
        self.model.MaxPowerTurbineHydroWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_hydro[g])
        self.model.MaxPowerPumpHydroWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_pump_hydro[g])        
        
        self.model.MaxPowerGeneratedHydroWaterDCPFCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroWaterDCPF[g] * m.UnitOn[g,t])  
        #self.model.MaxPowerStoredHydroWaterDCPFCon =  Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MaxPowerPumpHydroWaterDCPF[g] * (m.UnitOn[g,t] - 1))  
        self.model.MaxPowerStoredHydroWaterDCPFCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerPumpHydroWaterDCPF[g] * (1 - m.UnitOn[g,t]))             
    """
    Sets reserve constraints for pumped hydro
    """   
    def set_hydro_power_Pumped_reserves(self, GENS, gen_up_FCR_Pumped, gen_down_FCR_Pumped, gen_up_FRR_Pumped, gen_down_FRR_Pumped, gen_up_RR_Pumped, gen_down_RR_Pumped):
        if not hasattr(self.model, 'MinEnergyCapacityHydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')
        if not hasattr(self.model, 'MaxEnergyCapacityHydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')
        if not hasattr(self.model, 'MaxPowerTurbineHydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')
        if not hasattr(self.model, 'MaxPowerPumpHydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')
        if not hasattr(self.model, 'CapacityT0Hydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')
        
        #self.model.ActualUpFCReservePumped = Var(
        #    GENS,
        #    self.model.TimePeriods,
        #    within=NonNegativeReals)
        self.model.ActualUpFRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #self.model.RUpFCRLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReservePumped[g,t] <= gen_up_FCR_Pumped[g] * m.UnitOn[g,t])
        #self.model.RUpFRRLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFRReservePumped[g,t] <= gen_up_FRR_Pumped[g] * m.UnitOn[g,t])
        #self.model.RDownRRLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpRRReservePumped[g,t] <= gen_up_RR_Pumped[g] * m.UnitOn[g,t])
        #self.model.RUpTurbineLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReservePumped[g,t] + m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t] <= m.MaxPowerTurbineHydro[g] - m.PowerGenerated[g,t] * m.UnitOn[g,t] + m.PowerGenerated[g,t] * (m.UnitOn[g,t] - 1))
        self.model.RUpTurbineLimitPumped = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t] <= m.MaxPowerTurbineHydro[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t])
        self.model.RUpTurbineLimitPumped1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t] <= m.MaxPowerTurbineHydro[g])
        #self.model.RUpCapacityLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualUpFCReservePumped[g,t] + m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t] <= (m.SoC[g,t] - m.MinEnergyCapacityHydro[g]))
        for t in self.model.TimePeriods:
            for g in GENS:
                #self.up_reserves_at[t][g] = self.model.ActualUpFCReservePumped[g,t] + self.model.ActualUpFRReservePumped[g,t] + self.model.ActualUpRRReservePumped[g,t]
                #self.up_FCR_reserves_at[t][g] = self.model.ActualUpFCReservePumped[g,t]
                self.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReservePumped[g,t]
                self.up_RR_reserves_at[t][g] = self.model.ActualUpRRReservePumped[g,t]

        #self.model.ActualDownFCReservePumped = Var(
        #    GENS,
        #    self.model.TimePeriods,
        #    within=NonNegativeReals)
        self.model.ActualDownFRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #self.model.RDownFCRLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReservePumped[g,t] <= gen_down_FCR_Pumped[g] * (1 - m.UnitOn[g,t]))
        #self.model.RDownFRRLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFRReservePumped[g,t] <= gen_down_FRR_Pumped[g] * (1 - m.UnitOn[g,t]))
        #self.model.RDownRRRLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownRRReservePumped[g,t] <= gen_down_RR_Pumped[g] * (1 - m.UnitOn[g,t]))
        #self.model.RDownPumpLimit = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReservePumped[g,t] + m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t] <= m.MaxPowerPumpHydro[g] - m.PowerGenerated[g,t] * (m.UnitOn[g,t] - 1) + m.PowerGenerated[g,t] * m.UnitOn[g,t])
        self.model.RDownPumpLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t] <= m.MaxPowerPumpHydro[g] + m.PowerGenerated[g,t] - m.PowerConsumed[g,t])
        self.model.RDownPumpLimit1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t] <= m.MaxPowerPumpHydro[g])
        
        #self.model.RDownCapacityLimitPumped = Constraint(
        #    GENS,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: m.ActualDownFCReservePumped[g,t] + m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t] <= m.MaxEnergyCapacityHydro[g] - m.SoC[g,t])

        for t in self.model.TimePeriods:
            for g in GENS:
                #self.down_reserves_at[t][g] = self.model.ActualDownFCReservePumped[g,t] + self.model.ActualDownFRReservePumped[g,t] + self.model.ActualDownRRReservePumped[g,t]
                #self.down_FCR_reserves_at[t][g] = self.model.ActualDownFCReservePumped[g,t]
                self.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReservePumped[g,t]   
                self.down_RR_reserves_at[t][g] = self.model.ActualDownRRReservePumped[g,t]
       
    """
    Sets CO2 emissions cap - requires a total emissions target
    """  
    def set_CO2_emissions(self, GENS, unit_CO2_emission_rate, carbon_target):
        self.model.UnitEmissionRate = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: unit_CO2_emission_rate[g])
        self.model.CarbonTargetCon = Constraint(
            self.model.TimePeriods,
            rule=lambda m,g,t: sum(m.PowerGenerated[g,t] * m.UnitEmissionRate[g]) <= carbon_target)

    """
    Set all costs including startup, shutdown and operational costs - this cost function is valid for all conventional generators
    This function is not valid for batteries due to the possibility for the variable PowerGenerated to have both positive and negative sign 
    """
    def get_all_costs(self, GENS, start_up_cost_coefficient, shut_down_cost_coefficient, operation_cost_coefficient, no_load_cost_coefficient=None):
        self.model.StartUpCostCoefficient = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_cost_coefficient[g])
        self.model.ShutDownCostCoefficient = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: shut_down_cost_coefficient[g])
        self.model.OperationCostCoefficientAll = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient[g])
        self.model.StartUpCost = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.StartUpCostCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.StartUpCost[g,t] >= m.StartUpCostCoefficient[g] * (m.UnitOn[g,t] - self._uniton_before(g,t)))
        self.model.ShutDownCost = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ShutDownCostCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ShutDownCost[g,t] >= m.ShutDownCostCoefficient[g] * (self._uniton_before(g,t) - m.UnitOn[g,t]))
        m = self.model
        if no_load_cost_coefficient:
            return sum(m.StartUpCost[g,t] + m.ShutDownCost[g,t] + m.OperationCostCoefficientAll[g] * m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)
        else:
            return sum(m.StartUpCost[g,t] + m.ShutDownCost[g,t] + m.OperationCostCoefficientAll[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)
    
    """
    Set operational cost only (simplified objective function)
    This function is not valid for batteries due to the possibility for the variable PowerGenerated to have both positive and negative sign 
    """
    def get_operational_costs(self, GENS, operation_cost_coefficient):
        self.model.OperationCostCoefficient = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient[g])
        m = self.model
        return sum(m.OperationCostCoefficient[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    Set operational cost for hydro RoR
    This function is not valid for pumped hydro
    """
    def get_operational_costs_hydro_RoR(self, GENS, operation_cost_coefficient_RoR):
        self.model.OperationCostCoefficientHydroRoR = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient_RoR[g])
        m = self.model
        return sum(m.OperationCostCoefficientHydroRoR[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)
    
    """
    Set operational cost for hydro Dam
    This function is not valid for pumped hydro
    """
    def get_operational_costs_hydro_Dam(self, GENS, operation_cost_coefficient_Dam):
        self.model.OperationCostCoefficientHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient_Dam[g])
        m = self.model
        return sum(m.OperationCostCoefficientHydroDam[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)
    
    """
    Set operational cost for Pumped Hydro
    Pumping costs are assumed to be zero, only costs in turbine mode are incurred  
    """
    def get_operational_costs_hydro_Pumped(self, GENS, operation_cost_coefficient_pumpedhydro):
        self.model.OperationCostCoefficientPumpedHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient_pumpedhydro[g])

        m = self.model
        #return sum(m.OperationCostCoefficientPumpedHydro[g] * m.PowerGenerated[g,t] * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)
        return sum(m.OperationCostCoefficientPumpedHydro[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    Set costs for batteries (batteries have only operational costs)
    Storage costs are assumed to be zero, only costs in discharge mode are incurred  
    """
    def get_operational_costs_battery(self, GENS, operation_cost_coefficient_battery):
        self.model.OperationCostCoefficientBattery = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient_battery[g])

        m = self.model
        #return sum((m.OperationCostCoefficientBattery[g] * m.PowerGenerated[g,t] * m.UnitOn[g,t]) for t in self.model.TimePeriods for g in GENS) #+ m.OperationCostCoefficientBattery[g] * m.PowerGenerated[g,t] * (m.UnitOn[g,t] - 1) add this term if you want pump cost to be equal to turbine cost
        return sum(m.OperationCostCoefficientBattery[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    Sets that the objective function will be the cost.
    """
    def set_objective_function(self, fn):
        self.model.obj = Objective(expr=fn, sense=minimize)
    
    def pprint(self):
        self.model.pprint()
    
    def solve(self):
        #self.model.solutions.load_from(SolverFactory(self.solver).solve(self.model))
        #results = SolverFactory(self.solver).solve(self.model, tee=True)
        opt = SolverFactory(self.solver)
        results = opt.solve(self.model, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status:"), results.solver.status
        print("    Objective function Value ="), value(self.model.obj)
        
    def print_duals_reserves(self):
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "UpReserveFRR": #"NodalConstraint"
                for index in cobject:
                    print("    "), index, self.model.dual[cobject[index]]
                    
    def print_duals_elprices_simple(self):
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "ProductionEqualsDemandCon":
                for index in cobject:
                    print("    "), index, self.model.dual[cobject[index]]
                    
    def print_duals_elprices(self):
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "NodalConstraint":
                for index in cobject:
                    print("    "), index, self.model.dual[cobject[index]]
    
    def save_electricity_prices(self,baseMVA=None):
        column_ElPrice = []
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "NodalConstraint":
                for index in cobject:
                    if index[0] == 0: #0 is the swiss node
                        if baseMVA:
                            column_ElPrice.append(self.model.dual[cobject[index]]/baseMVA)
                        else:
                            column_ElPrice.append(self.model.dual[cobject[index]])
        column_hours = [int(i+1) for i in range(len(column_ElPrice))]
        df = pd.DataFrame(np.column_stack([column_hours, column_ElPrice]), columns = ['hour','el_price_CHF_per_MWh'])
        writer = pd.ExcelWriter('El_Price_CH.xlsx', engine='xlsxwriter')
        df.to_excel(writer)
        writer.close()
        
    def save_FRRup_prices(self,baseMVA=None):
        column_UpPriceFRR = []
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "UpReserveFRR":
                for index in cobject:
                        if baseMVA:
                            column_UpPriceFRR.append(self.model.dual[cobject[index]]/baseMVA)
                        else:
                            column_UpPriceFRR.append(self.model.dual[cobject[index]])
        column_hours = [int(i+1) for i in range(len(column_UpPriceFRR))]
        df = pd.DataFrame(np.column_stack([column_hours, column_UpPriceFRR]), columns = ['hour','FRRup_price_CHF_per_MWh'])
        writer = pd.ExcelWriter('FRRup_Price_CH.xlsx', engine='xlsxwriter')
        df.to_excel(writer)
        writer.close()
        
    def save_FRRdown_prices(self,baseMVA=None):
        column_DownPriceFRR = []
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "DownReserveFRR":
                for index in cobject:
                        if baseMVA:
                            column_DownPriceFRR.append(self.model.dual[cobject[index]]/baseMVA)
                        else:
                            column_DownPriceFRR.append(self.model.dual[cobject[index]])
        column_hours = [int(i+1) for i in range(len(column_DownPriceFRR))]
        df = pd.DataFrame(np.column_stack([column_hours, column_DownPriceFRR]), columns = ['hour','FRRdown_price_CHF_per_MWh'])
        writer = pd.ExcelWriter('FRRdown_Price_CH.xlsx', engine='xlsxwriter')
        df.to_excel(writer)
        writer.close()
    
    def solve_with_timeout(self, limit = 60):
        self.model.solutions.load_from(SolverFactory(self.solver).solve(self.model, timelimit=limit))        

    def solve_with_gap(self, gap = 0.015, threads = 8):
        #self.model.write('test.mps')
        opt = SolverFactory(self.solver)
        opt.options["MIPGap"] = gap
        opt.options["Threads"] = threads
        opt.options["Method"] = 2
        opt.options["BarConvTol"] = 1e-10
        opt.options["FeasibilityTol"] = 1e-5
        opt.options["Crossover"] = 0
        opt.options["CrossoverBasis"] = 1
        opt.options["Presolve"] = 2
        opt.options["QCPDual"] = 0
        results = opt.solve(self.model, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status:"), results.solver.status
        print("    Objective function Value ="), value(self.model.obj)
             
    def solve_gurobi_iis(self):
        opt = SolverFactory('gurobi', solver_io='nl')
        opt.options['outlev'] = 1
        opt.options['iisfind'] = 1
        self.model.solutions.load_from(opt.solve(self.model))
        
    def get_generator_power(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PowerGenerated[generator,time])
        return results

    def get_generator_power_consumed(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PowerConsumed[generator,time])
        return results

    def get_battery_state(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.SoC[generator,time])
        return results
    
    def unit_on_off(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.UnitOn[generator,time])
        return results

if __name__ == '__main__':
    o = CostOptimization()
    o.solve()
    o.show()

