from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import math
import numpy as np

class RenewableGenerators:
    def __init__(self, state, gens):
        self.state = state
        self.model = state.model
        self.generators = gens
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        
        self.model.NoPowerConsumedRenewable = Constraint(
            self.generators,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)
        
        #self.model.UnitOnRESCon = Constraint(
        #    self.generators,
        #    self.model.TimePeriods,
        #    rule=lambda m,g,t: state.unit_on(g,t) == 0)
    """
    sets operational constraints for run-of-river hydro power plants using hourly capacity factors per power plant
    turbine efficiency is neglected
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

    """
    sets the constraints for solar PV using hourly PV Capacity Factor for each power plant
    """   
    def set_pv_power(self, GENS, CF_PV, Pmax_PV):
        self.model.CapacityFactorPV = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CF_PV[g][t])
        self.model.MaxPowerPV = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_PV[g])
        self.model.PowerCFPVCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorPV[g,t] * m.MaxPowerPV[g])
        
    """
    sets the constraints for solar PV using hourly production profile
    """   
    def set_pv_power_production(self, GENS, Ppv_prod, baseMVA):
        self.model.PowerProductionPV = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: Ppv_prod[g][t]/baseMVA)
        self.model.PowerProdPVCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.PowerProductionPV[g,t])
    
    """
    sets the constraints for wind power generation using using hourly Capacity Factor for each power plant
    """
    def set_wind_power(self, GENS, CF_wind, Pmax_wind):
        self.model.CapacityFactorWind = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CF_wind[g][t])
        self.model.MaxPowerWind = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_wind[g])
        self.model.PowerCFWindCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorWind[g,t] * m.MaxPowerWind[g])

    """
    sets the constraints for wind power generation using hourly production profile
    """
    def set_wind_power_production(self, GENS, Pwind_prod, baseMVA):
        self.model.PowerProductionWind = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: Pwind_prod[g][t]/baseMVA)
        self.model.PowerProdWindCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.PowerProductionWind[g,t])
    
    """
    sets the equations for wind power production using linearized power curve (this formulation requires hourly wind speeds and turbine specs)
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
    
    """
    sets new wind power curve from Philipp
    """
    def set_wind_power_curve_new(self, GENS, P_max, wind_speed):
        self.model.PmaxWind1 = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: P_max[g]) #nominal power of wind turbine
        self.model.WindSpeed1 = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: wind_speed[g][t]*(math.log(120/0.1)/math.log(10/0.1)) if wind_speed[g][t] <= 15.0 else 15.0)
        # here it is assumed power curve fit from Enercon E-92 with following data
        # x = x = 1:15 % windspeed in m/s
        # y = [0 3.6 29.9 98.2 208 384 637 975 1403 1817 2088 2237 2300 2350 2350]/2350 normalized output
        # fitting model a*x^3
        self.model.CapacityFactorWind = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals, 
            initialize=lambda m,g,t: 0.0007901*m.WindSpeed1[g,t]**3 if 0.0007901*m.WindSpeed1[g,t]**3 <= 1.0 else 1.0)
        self.model.PowerCFWindNewCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorWind[g,t] * m.PmaxWind1[g])
                  
    """
    post-processing routines
    """
    def get_solar_curtailments(self, solar_gen):
        if not hasattr(self.model, 'PowerProductionPV'):
            raise Exception('Missing call to set_pv_power_production')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PowerProductionPV[solar_gen,time] - self.model.PowerGenerated[solar_gen,time])
        return results

    def get_wind_curtailments(self, wind_gen):
        if not hasattr(self.model, 'PowerProductionWind'):
            raise Exception('Missing call to set_wind_power_production')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PowerProductionWind[wind_gen,time] - self.model.PowerGenerated[wind_gen,time])
        return results
        
    def get_ROR_curtailments(self, ror_gen):
        if not hasattr(self.model, 'CapacityFactorHydro'):
            raise Exception('Missing call to set_hydro_power_RoR')
        if not hasattr(self.model, 'PmaxHydro'):
            raise Exception('Missing call to set_hydro_power_RoR')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CapacityFactorHydro[ror_gen,time] * self.model.PmaxHydro[ror_gen]- self.model.PowerGenerated[ror_gen,time])
        return results