from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class ConventionalGeneratorsReserves:
    def __init__(self, state, gens):
        self.state = state
        self.model = state.model
        self.generators = gens

        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        """
        variables
        """
        self.model.UnitOn = Var(
            self.generators, 
            self.model.TimePeriods, 
            within=IntegerSet(bounds=(0,1))) #IntegerSet(bounds=(0,1)) on/off status of each generator at each time period 
        
        #secondary and tertiary reserves variables 
        self.model.ActualUpFRReserve = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserve = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownFRReserve = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserve = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        self.model.NoPowerConsumedConv = Constraint(
            self.generators,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    def gens(self):
        return self.generators

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
    set minimum generation levels for each generator (units in MW or p.u.) 
    #used for neighboring countries
    """
    def set_min_power(self, GENS, min_power):
        self.model.MinPower0 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.MinPowerCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MinPower0[g])
        self.model.MinPowerCon1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] == 0)
        
    """
    set maximum generation levels for each generator (units in MW or p.u.)
    #used for neighboring countries
    """
    def set_max_power(self, GENS, max_power):
        self.model.MaxPower0 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.MaxPowerCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPower0[g])
        
    """
    limits for power generated in each time period (1/2)
    """
    def set_genlimits_min_power(self, GENS, min_power):
        self.model.MinPower = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.GenLimitsPerPeriodCon1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinPower[g] * m.UnitOn[g,t] <= m.PowerGenerated[g,t] - (m.ActualDownFRReserve[g,t] + m.ActualDownRRReserve[g,t]))

    """
    limits for power generated in each time period (2/2)
    """
    def set_genlimits_max_power(self, GENS, max_power):
        self.model.MaxPower = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.GenLimitsPerPeriodCon3 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] + (m.ActualUpFRReserve[g,t] + m.ActualUpRRReserve[g,t]) <= m.MaxPower[g]*m.UnitOn[g,t])

    """
    sets ramp-up constraint
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
                    m.PowerGenerated[g,t] + (m.ActualUpFRReserve[g,t] + m.ActualUpRRReserve[g,t]) <= self._pgen_before(g,t) +
                                                    m.NominalRampUpLimit[g] * self._uniton_before(g,t) +
                                                    m.StartUpRampLimit[g] * (m.UnitOn[g,t] - self._uniton_before(g,t)) +
                                                    m.MaxPower[g] * (1 - m.UnitOn[g,t]))
                    
    """
    sets ramp-down constraint
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
                    m.PowerGenerated[g,t] <= m.MaxPower[g] * m.UnitOn[g,t+1] + 
                                                    m.ShutDownRampLimit[g] * (m.UnitOn[g,t] - m.UnitOn[g,t+1]))
        self.model.RampDownPowerOutCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    self._pgen_before(g, t) - m.PowerGenerated[g,t] + (m.ActualDownFRReserve[g,t] + m.ActualDownRRReserve[g,t]) <= m.NominalRampDownLimit[g] * m.UnitOn[g,t] +
                                                                    m.ShutDownRampLimit[g] * (self._uniton_before(g,t) - m.UnitOn[g,t]) + 
                                                                    m.MaxPower[g] * (1 - self._uniton_before(g,t)))
                                                          
    """
    sets min. up time for both initial and non-initial conditions
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
    sets min. down time for both initial and non-initial conditions
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
    set availability constraint (used for refueling of nuclear power plants over the year) 
    """
    def set_refueling(self, GENS, schedule):
        self.model.Schedule = Param(
            GENS, 
            self.model.TimePeriods,
            within=NonNegativeIntegers,
            initialize=lambda m,g,t: schedule[g][t])
        self.model.AvailabilityCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] <= m.Schedule[g,t])

    """
    sets FCR (Frequency Containment Reserve) limits for each generator.
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
    sets FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator
    """
    def set_FRR_RR(self, reserves, GENS):        
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserve[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserve[g,t]
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserve[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserve[g,t]               
    
    """
    sets FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator without binaries
    """
    def set_FRR_RR_linear(self, reserves, GENS,
                gen_up_FCR, gen_down_FCR, gen_up_FRR, gen_down_FRR, gen_up_RR, gen_down_RR):        
        if not hasattr(self.model, 'MaxPower'):
            raise Exception('Missing call to set_genlimits_max_power')
        if not hasattr(self.model, 'MinPower'):
            raise Exception('Missing call to set_genlimits_min_power')   
        
        self.model.ActualUpFRReserveLin = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveLin = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        self.model.RUpGenLimitLin = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveLin[g,t] + m.ActualUpRRReserveLin[g,t] <= m.MaxPower[g] - m.PowerGenerated[g,t]) #this means the generator can provide upward reserve if it's at 0 (off)
        self.model.RUpGenLimitLin1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveLin[g,t] + m.ActualUpRRReserveLin[g,t] <= m.MaxPower[g])
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveLin[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveLin[g,t]

        self.model.ActualDownFRReserveLin = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveLin = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)

        self.model.RDownGenLimitLin = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] - (m.ActualDownFRReserveLin[g,t] + m.ActualDownRRReserveLin[g,t]) <= m.MaxPower[g])
        self.model.RDownGenLimitLin1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveLin[g,t] + m.ActualDownRRReserveLin[g,t] <= m.MaxPower[g])
        
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveLin[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveLin[g,t]       
    
    """
    detailed cost function - linear
    """
    def get_all_costs(self, GENS, start_up_cost_coefficient, operation_cost_coefficient, tpRes, no_load_cost_coefficient=None):
        self.model.StartUpCostCoefficient = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_cost_coefficient[g])
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
        m = self.model
        if no_load_cost_coefficient:
            return (sum(m.StartUpCost[g,t] + m.OperationCostCoefficientAll[g] * m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
        else:
            return (sum(m.StartUpCost[g,t] + m.OperationCostCoefficientAll[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
             
    """
    detailed cost function - linear
    """
    def get_all_costs_lp(self, GENS, start_up_cost_coefficient, operation_cost_coefficient, no_load_cost_coefficient=None):
        self.model.StartUpCostCoefficientSimple = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_cost_coefficient[g])
        self.model.OperationCostCoefficientSimple = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: operation_cost_coefficient[g])
        self.model.StartUpCostSimple = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.StartUpCostConSimple = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.StartUpCostSimple[g,t] >= m.StartUpCostCoefficientSimple[g] * (m.UnitOn[g,t] - self._uniton_before(g,t)))
        m = self.model
        if no_load_cost_coefficient:
            return sum(m.StartUpCostSimple[g,t] + m.OperationCostCoefficientSimple[g] * m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)
        else:
            return sum(m.StartUpCostSimple[g,t] + m.OperationCostCoefficientSimple[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)
             
    """
    post-processing routines
    """             
    def unit_on_off(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.UnitOn[generator,time])
        return results
    
    def frr_up_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserve[generator,time])
        return results
    
    def frr_down_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserve[generator,time])
        return results
            
    def rr_up_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReserve[generator,time])
        return results  
    
    def rr_down_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReserve[generator,time])
        return results     