from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class ConventionalGeneratorsCHDispatchContinuous:
    def __init__(self, state, gens):
        self.state = state
        self.model = state.model
        self.generators = gens

        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        self.model.NoPowerConsumedDispatchContinuousCH = Constraint(
            self.generators,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    def _pgen_before_dispatchcont(self, g, t):
        if t == 0:
            return self.model.PowerGeneratedT0DispatchContinuousCH[g]
        else:
            return self.model.PowerGenerated[g, t - 1]

    """
    set power output of units at t=0 
    """    
    def set_pgen_t0_dispatchcont(self, GENS, power_generated_t0):
        self.model.PowerGeneratedT0DispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: power_generated_t0[g])

    """
        set minimum generation levels only for nuclear units
        """

    def set_min_power_CH_Nuke_dispatchcont(self, GENS, min_power, availability):
        self.model.MinPowerCHNukeDispatchContinuous = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m, g: min_power[g])

        self.model.Availability = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m, g, t: availability[g][t])

        self.model.MinPowerConCHNukeDispatchContinuous = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m, g, t: m.PowerGenerated[g, t] >= m.MinPowerCHNukeDispatchContinuous[g] * m.Availability[g, t])
    """
    set minimum generation levels
    """
    def set_min_power_CH_dispatchcont(self, GENS, min_power):
        self.model.MinPowerDispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.MinPowerConDispatchContinuousCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MinPowerDispatchContinuousCH[g])
        
    """
    set maximum generation levels
    """
    def set_max_power_CH_dispatchcont(self, GENS, max_power):
        self.model.MaxPowerDispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.MaxPowerConDispatchContinuousCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerDispatchContinuousCH[g])

    """
    set ramp up limits
    """
    def set_ramp_linear_CH_dispatchcont(self, GENS, ramp_up, ramp_down):
        if not hasattr(self.model, 'PowerGeneratedT0DispatchContinuousCH'):
            raise Exception('Missing call to set_pgen_t0_dispatchcont')
        #ramp limit parameters
        self.model.RampUpLimitDispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_up[g])
        self.model.RampDownLimitDispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_down[g])
        #ramp down constraint
        self.model.RampDownConDispatchContinuousCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    self._pgen_before_dispatchcont(g, t) - m.PowerGenerated[g,t] <= m.RampDownLimitDispatchContinuousCH[g])
        #ramp up constraint
        self.model.RampUpConDispatchContinuousCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:
                    m.PowerGenerated[g,t] - self._pgen_before_dispatchcont(g, t) <= m.RampUpLimitDispatchContinuousCH[g])

    """
    set ramp limits considering reserve provision
    """   
    def set_ramp_linear_CH_dispatchcont_reserves(self, GENS, ramp_up, ramp_down):
        if not hasattr(self.model, 'PowerGeneratedT0DispatchContinuousCH'):
            raise Exception('Missing call to set_pgen_t0_dispatchcont')
        if not hasattr(self.model, 'MaxPowerConDispatchContinuousCH'):
            raise Exception('Missing call to set_max_power_CH_dispatchcont')

        #secondary and tertiary reserves variables 
        self.model.ActualUpFRReserveDispatchContinuousCH = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveDispatchContinuousCH = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownFRReserveDispatchContinuousCH = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveDispatchContinuousCH = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        #ramp limit parameters
        self.model.RampUpLimitDispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_up[g])
        self.model.RampDownLimitDispatchContinuousCH = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: ramp_down[g])
        #ramp down constraints
        self.model.RampDownConDispatchContinuousCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: self._pgen_before_dispatchcont(g, t) - m.PowerGenerated[g,t] + (m.ActualDownFRReserveDispatchContinuousCH[g,t] + m.ActualDownRRReserveDispatchContinuousCH[g,t]) <= m.RampDownLimitDispatchContinuousCH[g])
        self.model.DownMaxPowerLimitConDispatchContinuousCH2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveDispatchContinuousCH[g,t] + m.ActualDownRRReserveDispatchContinuousCH[g,t] <= m.RampDownLimitDispatchContinuousCH[g])
        self.model.DownMaxPowerLimitConDispatchContinuousCH1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - (m.ActualDownFRReserveDispatchContinuousCH[g,t] + m.ActualDownRRReserveDispatchContinuousCH[g,t]) >= 0) #can not provide down reserve if at 0
        #ramp up constraints
        self.model.RampUpConDispatchContinuousCH = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] - self._pgen_before_dispatchcont(g, t) + (m.ActualUpFRReserveDispatchContinuousCH[g,t] + m.ActualUpRRReserveDispatchContinuousCH[g,t]) <= m.RampUpLimitDispatchContinuousCH[g])
        self.model.UpMaxPowerLimitConDispatchContinuousCH1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveDispatchContinuousCH[g,t] + m.ActualUpRRReserveDispatchContinuousCH[g,t] <= m.RampUpLimitDispatchContinuousCH[g])
        self.model.UpMaxPowerLimitConDispatchContinuousCH2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] + (m.ActualUpFRReserveDispatchContinuousCH[g,t] + m.ActualUpRRReserveDispatchContinuousCH[g,t]) <= m.MaxPowerDispatchContinuousCH[g])
    
    """
    sets FRR (Frequency Restoration Reserve) and RR (Replacement Reserve) limits for each conventional generator
    """
    def set_FRR_RR(self, reserves, GENS):        
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveDispatchContinuousCH[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveDispatchContinuousCH[g,t]
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveDispatchContinuousCH[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveDispatchContinuousCH[g,t]      

    """
    simplified cost function
    """
    def get_all_costs(self, GENS, startup_cost_coefficient, operation_cost_coefficient, tpRes, no_load_cost_coefficient=None):
        m = self.model
        if no_load_cost_coefficient:
            return (sum(operation_cost_coefficient[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
        else: 
            return (sum(operation_cost_coefficient[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes

    """
    simplified cost function - linear re-solve
    """
    def get_all_costs_lp(self, GENS, start_up_cost_coefficient_lp, operation_cost_coefficient_lp, no_load_cost_coefficient_lp=None):
        m = self.model
        if no_load_cost_coefficient_lp:
            return sum(operation_cost_coefficient_lp[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)
        else: 
            return sum(operation_cost_coefficient_lp[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    detailed cost function
    -- startup_cost_coefficient - not used since we don't have UC
    """
    def get_operational_costs_conv_disagg(self, GENS, startup_cost_coefficient, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, tpRes, baseMVA, no_load_cost_coefficient=None):
        self.model.FuelPriceCont = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffCont = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceCont = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateCont = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMCont = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        return (sum((m.FuelPriceCont[g,t] / m.FuelEffCont[g] + m.CO2PriceCont[g,t] * m.CO2RateCont[g] + m.NonFuelVOMCont[g]) *  m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
    
    """
    detailed cost function - linear re-solve
    -- startup_cost_coefficient - not used since we don't have UC
    """
    def get_operational_costs_conv_disagg_LP(self, GENS, startup_cost_coefficient, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, baseMVA, no_load_cost_coefficient=None):
        self.model.FuelPriceContLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffContLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceContLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateContLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMContLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        return sum((m.FuelPriceContLP[g,t] / m.FuelEffContLP[g] + m.CO2PriceContLP[g,t] * m.CO2RateContLP[g] + m.NonFuelVOMContLP[g]) *  m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)
       
    """
    post-processing routines
    """             
    def frr_up_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveDispatchContinuousCH[generator,time])
        return results
    
    def frr_down_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveDispatchContinuousCH[generator,time])
        return results
            
    def rr_up_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReserveDispatchContinuousCH[generator,time])
        return results  
    
    def rr_down_conv(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReserveDispatchContinuousCH[generator,time])
        return results           

    def get_hourly_gencost_conv_LP(self, generator):
        if not hasattr(self.model, 'FuelPriceContLP'):
            raise Exception('Missing call to get_operational_costs_conv_disagg_LP')
        results = np.zeros(self.num_snaphots)    
        for time in range(self.num_snaphots):
            results[time] = value((self.model.FuelPriceContLP[generator,time] / self.model.FuelEffContLP[generator] + self.model.CO2PriceContLP[generator,time] * self.model.CO2RateContLP[generator] + self.model.NonFuelVOMContLP[generator]) * self.model.PowerGenerated[generator,time])
        return results