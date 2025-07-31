from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class ConventionalGeneratorsTight:
    def __init__(self, state, gens):
        self.state = state
        self.model = state.model
        self.generators = gens
        
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        self.model.UnitOn = Var(
            self.generators, 
            self.model.TimePeriods, 
            within=IntegerSet(bounds=(0,1))) #on/off status of each generator at each time period 
        self.model.ShutDown = Var(
            self.generators, 
            self.model.TimePeriods, 
            within=IntegerSet(bounds=(0,1))) #shut-down status of each generator at each time period 
        self.model.StartUp = Var(
            self.generators, 
            self.model.TimePeriods, 
            within=IntegerSet(bounds=(0,1))) #start up status of each generator at each time period 
        self.model.PowerAbovePmin = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        self.model.NoPowerConsumedConvTightCon = Constraint(
            self.generators,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    def gens(self):
        return self.generators

    def select_gen(self, l):
        return SetOf(l)

    def _pabovemin_before(self, g, t):
        if t == 0:
            return self.model.PowerAbovePminT0[g]
        else:
            return self.model.PowerAbovePmin[g, t - 1]
        
    def _uniton_before(self, g, t):
        if t == 0:
            return self.model.UnitOnT0[g]
        else:
            return self.model.UnitOn[g, t - 1]
        
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
    def set_pgen_t0(self, GENS, power_generated_t0, min_powergens):
        if not hasattr(self.model, 'MinPowerTight'):
            raise Exception('Missing call to set_genlimits_min_power_tight')
        self.model.PowerT0 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: power_generated_t0[g]) 
        self.model.PowerAbovePminT0 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: (m.PowerT0[g] - m.MinPowerTight[g]) if m.PowerT0[g] >0 else 0) 
        
    """
    logical constraint 
    """
    def set_logical_order(self, GENS):
        self.model.LogicalCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] - self._uniton_before(g,t) -  m.StartUp[g,t] + m.ShutDown[g,t] == 0)

    """
    set min generation limits (KU LEUVEN 2016 eq.20)
    """
    def set_genlimits_min_power_tight(self, GENS, min_power):
        self.model.MinPowerTight = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.GenLimitsPerPeriodTightCon1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerAbovePmin[g,t])
        self.model.GenLimitsPerPeriodTightCon2 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] == m.UnitOn[g,t] * m.MinPowerTight[g] + m.PowerAbovePmin[g,t])

    """
    set max generation limits for power plants with minimum uptime equal to 1 (eq. 1, 3, 4 and 5 from Morales 2014) 
    """
    def set_genlimits_max_power_tight_UT1(self, GENS, max_power, start_up_ramp_limit, shut_down_ramp_limit):
        if not hasattr(self.model, 'MinPowerTight'):
            raise Exception('Missing call to set_genlimits_min_power_tight')
        self.model.MaxPowerTightUT1 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        if hasattr(self.model, 'MinPowerTight'):
            for i in GENS:
                assert start_up_ramp_limit[i] >= self.model.MinPowerTight[i]
                assert start_up_ramp_limit[i] <= self.model.MaxPowerTightUT1[i]
        self.model.StartUpRampLimitTightUT1 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_ramp_limit[g])
        self.model.ShutDownRampLimitTightUT1 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: shut_down_ramp_limit[g])
        self.model.GenLimitsPerPeriodTightUT1Con1 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t<1 or t > self.num_snaphots-2) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.StartUpRampLimitTightUT1[g]) * m.StartUp[g,t] - 
                                                        max((m.StartUpRampLimitTightUT1[g] - m.ShutDownRampLimitTightUT1[g]),0) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT1Con2 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t<1 or t > self.num_snaphots-2) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.ShutDownRampLimitTightUT1[g]) * m.ShutDown[g,t+1] - 
                                                        max((m.ShutDownRampLimitTightUT1[g] - m.StartUpRampLimitTightUT1[g]),0) * m.StartUp[g,t])
        self.model.GenLimitsPerPeriodTightUT1Con3 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t > 0) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.ShutDownRampLimitTightUT1[g]) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT1Con4 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t < self.num_snaphots - 1) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.StartUpRampLimitTightUT1[g]) * m.StartUp[g,t])

    """
    set max generation limits for power plants with minimum uptime >= 2 (eq. 1, 2 and 3 from Morales 2014)
    """
    def set_genlimits_max_power_tight_UT2(self, GENS, max_power2, start_up_ramp_limit2, shut_down_ramp_limit2):
        if not hasattr(self.model, 'MinPowerTight'):
            raise Exception('Missing call to set_genlimits_min_power_tight')
        self.model.MaxPowerTightUT2 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power2[g])
        if hasattr(self.model, 'MinPowerTight'):
            for i in GENS:
                assert start_up_ramp_limit2[i] >= self.model.MinPowerTight[i]
                assert start_up_ramp_limit2[i] <= self.model.MaxPowerTightUT2[i]
        self.model.StartUpRampLimitTightUT2 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_ramp_limit2[g])
        self.model.ShutDownRampLimitTightUT2 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: shut_down_ramp_limit2[g])
        self.model.GenLimitsPerPeriodTightUT2Con1 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t<1 or t > self.num_snaphots-2) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT2[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT2[g] - m.StartUpRampLimitTightUT2[g]) * m.StartUp[g,t] -
                                                        (m.MaxPowerTightUT2[g] - m.ShutDownRampLimitTightUT2[g]) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT2Con2 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t > 0) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT2[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT2[g] - m.ShutDownRampLimitTightUT2[g]) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT2Con3 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t < self.num_snaphots - 1) else
                    m.PowerAbovePmin[g,t] <= (m.MaxPowerTightUT2[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT2[g] - m.StartUpRampLimitTightUT2[g]) * m.StartUp[g,t])
        
    """
    constraint for min. up time for both initial and non-initial conditions (eq. 7 from Morales 2014)
    """
    def set_minimum_uptime_tight(self, GENS, minimum_uptime_tight):
        if not hasattr(self.model, 'UnitOnT0State'):
            raise Exception('Missing call to set_initial_status') 
        if not hasattr(self.model, 'UnitOnT0'):
            raise Exception('Missing call to set_initial_status') 
        self.model.MinimumUpTimeTight = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: minimum_uptime_tight[g])
        self.model.InitialTimeOnLineTight = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: max(0,(m.MinimumUpTimeTight[g] - m.UnitOnT0State[g])) * m.UnitOnT0[g])
        self.model.UpTimeInitialTightCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] == 1 if t < m.InitialTimeOnLineTight[g] else Constraint.Skip)
        self.model.UpTimeNonInitialTightCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t < m.MinimumUpTimeTight[g] - 1) else
                    m.UnitOn[g,t] >= sum(m.StartUp[g,t1] for t1 in self.model.TimePeriods if t1 <= t and t1 >= t - m.MinimumUpTimeTight[g] + 1))

    """
    constraint for min. down time for both initial and non-initial conditions (eq. 8 from Morales 2014)
    """
    def set_minimum_downtime_tight(self, GENS, minimum_downtime_tight):
        if not hasattr(self.model, 'UnitOnT0State'):
            raise Exception('Missing call to set_initial_status') 
        if not hasattr(self.model, 'UnitOnT0'):
            raise Exception('Missing call to set_initial_status') 
        self.model.MinimumDownTimeTight = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: minimum_downtime_tight[g])
        self.model.InitialTimeOffLineTight = Param(
            GENS,
            within=NonNegativeIntegers,
            initialize=lambda m,g: max(0,(m.MinimumDownTimeTight[g] + m.UnitOnT0State[g])) * (1 - m.UnitOnT0[g]))        
        self.model.DownTimeInitialTightCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] == 0 if t < m.InitialTimeOffLineTight[g] else Constraint.Skip)
        self.model.DownTimeNonInitialCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t < m.MinimumDownTimeTight[g] - 1) else
                    1 - m.UnitOn[g,t] >= sum(m.ShutDown[g,t1] for t1 in self.model.TimePeriods if t1 <= t and t1 >= t - m.MinimumDownTimeTight[g] + 1))

    """
    constraint for ramp up
    """
    def set_rampup_tight(self, GENS, start_up_ramp_limit, nominal_ramp_up_limit):
        if not hasattr(self.model, 'PowerAbovePminT0'):
            raise Exception('Missing call to _pabovemin_before')
        if not hasattr(self.model, 'MinPowerTight'):
            raise Exception('Missing call to set_genlimits_min_power_tight')
        self.model.StartUpRampLimitTight = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: start_up_ramp_limit[g])
        self.model.NominalRampUpLimitTight = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nominal_ramp_up_limit[g])
        self.model.RampUpTightCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerAbovePmin[g,t] - self._pabovemin_before(g,t) <= m.NominalRampUpLimitTight[g] * m.UnitOn[g,t] + (m.StartUpRampLimitTight[g] - m.MinPowerTight[g] - m.NominalRampUpLimitTight[g]) * m.StartUp[g,t])

    """
    constraint for ramp down
    """
    def set_rampdown_tight(self, GENS, shut_down_ramp_limit, nominal_ramp_down_limit):
        if not hasattr(self.model, 'PowerAbovePminT0'):
            raise Exception('Missing call to _pabovemin_before')
        if not hasattr(self.model, 'MinPowerTight'):
            raise Exception('Missing call to set_genlimits_min_power_tight')
        self.model.ShutDownRampLimitTight = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: shut_down_ramp_limit[g])
        self.model.NominalRampDownLimitTight = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nominal_ramp_down_limit[g])
        self.model.RampDownTightCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._pabovemin_before(g,t) - m.PowerAbovePmin[g,t] <= m.NominalRampDownLimitTight[g] * self._uniton_before(g,t) + (m.ShutDownRampLimitTight[g] - m.MinPowerTight[g] - m.NominalRampDownLimitTight[g]) * m.ShutDown[g,t])

    """
    set minimum generation levels for each generator
    #linear formulation for neighboring countries
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
        
    """
    set maximum generation levels for each generator
    #linear formulation for neighboring countries
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
    detailed cost function
        can be further extended to include 1) fuel costs, 2) power plant rated efficiency, 3) CO2 costs (see KU Leuven White Paper)
    """
    def get_all_costs(self, GENS, startup_cost_coefficient, shutdown_cost_coefficient, operation_cost_coefficient, no_load_cost_coefficient=None):
        m = self.model
        if no_load_cost_coefficient:
            return sum(startup_cost_coefficient[g] * m.StartUp[g,t] + 
                       shutdown_cost_coefficient[g] * m.ShutDown[g,t] + 
                       operation_cost_coefficient[g] * m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient[g] * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)
        else: 
            return sum(startup_cost_coefficient[g] * m.StartUp[g,t] + 
                       shutdown_cost_coefficient[g] * m.ShutDown[g,t] + 
                       operation_cost_coefficient[g] * (m.UnitOn[g,t] * m.MinPowerTight[g] + m.PowerAbovePmin[g,t]) for t in self.model.TimePeriods for g in GENS)

    """
    post-processing routine
    """             
    def unit_on_off(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.UnitOn[generator,time])
        return results
            
    #def unit_start(self,generator):
    #    results = np.zeros(self.num_snaphots)
    #    for time in range(self.num_snaphots):
    #        results[time] = value(self.model.StartUp[generator,time])
    #    return results
    
    #def unit_stop(self,generator):
    #    results = np.zeros(self.num_snaphots)
    #    for time in range(self.num_snaphots):
    #        results[time] = value(self.model.ShutDown[generator,time])
    #    return results