from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class ConventionalGeneratorsTightReserves:
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
        
        #secondary and tertiary reserves variables 
        self.model.ActualUpFRReserveTight = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveTight = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownFRReserveTight = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveTight = Var(
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
            initialize=lambda m,g: (m.PowerT0[g] - m.MinPowerTight[g]) if m.PowerT0[g] > 0 else 0) 
        
    """
    logical constraint 
    """
    def set_logical_order(self, GENS):
        self.model.LogicalCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._uniton_before(g,t) - m.UnitOn[g,t] +  m.StartUp[g,t] - m.ShutDown[g,t] == 0)

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
            rule=lambda m,g,t: 0 <= m.PowerAbovePmin[g,t] - (m.ActualDownFRReserveTight[g,t] + m.ActualDownRRReserveTight[g,t]))
        self.model.GenLimitsPerPeriodTightCon2 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] == m.UnitOn[g,t] * m.MinPowerTight[g] + m.PowerAbovePmin[g,t])

    """
    set max generation limits for power plants with minimum uptime equal to 1
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
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.StartUpRampLimitTightUT1[g]) * m.StartUp[g,t] - 
                                                        max((m.StartUpRampLimitTightUT1[g] - m.ShutDownRampLimitTightUT1[g]),0) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT1Con2 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t<1 or t > self.num_snaphots-2) else
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.ShutDownRampLimitTightUT1[g]) * m.ShutDown[g,t+1] - 
                                                        max((m.ShutDownRampLimitTightUT1[g] - m.StartUpRampLimitTightUT1[g]),0) * m.StartUp[g,t])
        self.model.GenLimitsPerPeriodTightUT1Con3 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t > 0) else
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.ShutDownRampLimitTightUT1[g]) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT1Con4 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t < self.num_snaphots - 1) else
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT1[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT1[g] - m.StartUpRampLimitTightUT1[g]) * m.StartUp[g,t])
    
    """
    set max generation limits for power plants with minimum uptime >= 2
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
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT2[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT2[g] - m.StartUpRampLimitTightUT2[g]) * m.StartUp[g,t] -
                                                        (m.MaxPowerTightUT2[g] - m.ShutDownRampLimitTightUT2[g]) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT2Con2 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t > 0) else
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT2[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
                                                        (m.MaxPowerTightUT2[g] - m.ShutDownRampLimitTightUT2[g]) * m.ShutDown[g,t+1])
        self.model.GenLimitsPerPeriodTightUT2Con3 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: Constraint.Skip if (t < self.num_snaphots - 1) else
                    m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) <= (m.MaxPowerTightUT2[g] - m.MinPowerTight[g]) * m.UnitOn[g,t] - 
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
            rule=lambda m,g,t: m.PowerAbovePmin[g,t] + (m.ActualUpFRReserveTight[g,t] + m.ActualUpRRReserveTight[g,t]) - self._pabovemin_before(g,t) <= m.NominalRampUpLimitTight[g] * m.UnitOn[g,t] + (m.StartUpRampLimitTight[g] - m.MinPowerTight[g] - m.NominalRampUpLimitTight[g]) * m.StartUp[g,t])

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
            rule=lambda m,g,t: self._pabovemin_before(g,t) - m.PowerAbovePmin[g,t] + (m.ActualDownFRReserveTight[g,t] + m.ActualDownRRReserveTight[g,t]) <= m.NominalRampDownLimitTight[g] * self._uniton_before(g,t) + (m.ShutDownRampLimitTight[g] - m.MinPowerTight[g] - m.NominalRampDownLimitTight[g]) * m.ShutDown[g,t])

    """
    sets FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator
    """
    def set_FRR_RR(self, reserves, GENS):        
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveTight[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveTight[g,t]
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveTight[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveTight[g,t]         

    """
    set biomass constraints - min
    """
    def set_min_power(self, GENS, min_power_biomass):
        self.model.MinPowerBiomass = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power_biomass[g])
        self.model.MinPowerBiomassCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinPowerBiomass[g] * m.UnitOn[g,t] <= m.PowerGenerated[g,t])
        self.model.MinPowerCon1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ShutDown[g,t] == 0)
        self.model.MinPowerCon2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.StartUp[g,t] == 0)
        
    """
    set biomass constraints - max
    """
    def set_max_power(self, GENS, max_power_biomass):
        self.model.MaxPowerBiomass = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_biomass[g])
        self.model.MaxPowerBiomassCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerBiomass[g] * m.UnitOn[g,t])
    
    """
    simplified cost function
    """
    def get_all_costs(self, GENS, startup_cost_coefficient, operation_cost_coefficient, tpRes, no_load_cost_coefficient=None):
        m = self.model
        if no_load_cost_coefficient:
            return (sum(startup_cost_coefficient[g] * m.StartUp[g,t] + 
                       operation_cost_coefficient[g] * m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient[g] * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
        else: 
            return (sum(startup_cost_coefficient[g] * m.StartUp[g,t] + 
                       operation_cost_coefficient[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes

    """
    detailed cost function
    """
    def get_operational_costs_conv_disagg(self, GENS, startup_cost_coefficient, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, tpRes, baseMVA, no_load_cost_coefficient=None):
        self.model.FuelPriceUC = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t]*baseMVA)
        self.model.FuelEffUC = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceUC = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t]*baseMVA)
        self.model.CO2RateUC = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMUC = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        if no_load_cost_coefficient:
            return (sum(startup_cost_coefficient[g] * m.StartUp[g,t] + 
                       (m.FuelPriceUC[g,t] / m.FuelEffUC[g] + m.CO2PriceUC[g,t] * m.CO2RateUC[g] + m.NonFuelVOMUC[g]) *  m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient[g] * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
        else: 
            return (sum(startup_cost_coefficient[g] * m.StartUp[g,t] + 
                       (m.FuelPriceUC[g,t] / m.FuelEffUC[g] + m.CO2PriceUC[g,t] * m.CO2RateUC[g] + m.NonFuelVOMUC[g]) *  m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes

    """
    simplified cost function - linear re-solve
    """
    def get_all_costs_lp(self, GENS, start_up_cost_coefficient_lp, operation_cost_coefficient_lp, no_load_cost_coefficient_lp=None):
        m = self.model
        if no_load_cost_coefficient_lp:
            return sum(start_up_cost_coefficient_lp[g] * m.StartUp[g,t] + 
                       operation_cost_coefficient_lp[g] * m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient_lp[g] * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)
        else: 
            return sum(start_up_cost_coefficient_lp[g] * m.StartUp[g,t] + 
                       operation_cost_coefficient_lp[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    detailed cost function - linear re-solve
    """
    def get_operational_costs_conv_disagg_LP(self, GENS, startup_cost_coefficient, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, baseMVA, no_load_cost_coefficient=None):
        self.model.StartUpCostUCLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: startup_cost_coefficient[g] * baseMVA)
        self.model.FuelPriceUCLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffUCLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceUCLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateUCLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMUCLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        if no_load_cost_coefficient:
            return sum(m.StartUpCostUCLP[g] * m.StartUp[g,t] + 
                       (m.FuelPriceUCLP[g,t] / m.FuelEffUCLP[g] + m.CO2PriceUCLP[g,t] * m.CO2RateUCLP[g] + m.NonFuelVOMUCLP[g]) *  m.PowerGenerated[g,t] + 
                       no_load_cost_coefficient[g] * m.UnitOn[g,t] for t in self.model.TimePeriods for g in GENS)
        else: 
            return sum(m.StartUpCostUCLP[g] * m.StartUp[g,t] + 
                       (m.FuelPriceUCLP[g,t] / m.FuelEffUCLP[g] + m.CO2PriceUCLP[g,t] * m.CO2RateUCLP[g] + m.NonFuelVOMUCLP[g]) *  m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    post-processing routines
    """             
    def frr_up_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveTight[generator,time])
        return results
    
    def frr_down_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveTight[generator,time])
        return results
            
    def rr_up_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReserveTight[generator,time])
        return results  
    
    def rr_down_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReserveTight[generator,time])
        return results       
    
    def unit_start(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.StartUp[generator,time])
        return results
    
    def unit_stop(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ShutDown[generator,time])
        return results
    
    def unit_on_off(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.UnitOn[generator,time])
        return results
    
    def get_hourly_gencost_conv_LP(self, generator):
        if not hasattr(self.model, 'FuelPriceUCLP'):
            raise Exception('Missing call to get_operational_costs_conv_disagg_LP')
        results = np.zeros(self.num_snaphots)    
        for time in range(self.num_snaphots):
            results[time] = value((self.model.FuelPriceUCLP[generator,time] / self.model.FuelEffUCLP[generator] + self.model.CO2PriceUCLP[generator,time] * self.model.CO2RateUCLP[generator] + self.model.NonFuelVOMUCLP[generator]) * self.model.PowerGenerated[generator,time])
        return results