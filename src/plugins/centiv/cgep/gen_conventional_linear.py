from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class ConventionalGeneratorsLinear:
    def __init__(self, state, gens):
        self.state = state
        self.model = state.model
        self.generators = gens

        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        self.model.NoPowerConsumedLinear = Constraint(
            self.generators,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    """
    set minimum generation levels for each generator (units in MW or p.u.) 
    #used for neighboring countries
    """
    def set_min_power(self, GENS, min_power):
        self.model.MinPowerLinear = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power[g])
        self.model.MinPowerCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] >= m.MinPowerLinear[g])
        
    """
    set maximum generation levels for each generator (units in MW or p.u.)
    #used for neighboring countries
    """
    def set_max_power(self, GENS, max_power):
        self.model.MaxPowerLinear = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power[g])
        self.model.MaxPowerCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerLinear[g])

    """
    sets operational constraints for CHP power plants using hourly capacity factors per power plant
    used for neighboring countries
    """   
    def set_CHP_production(self, GENS, CF_CHP, Pmax_CHP):  
        self.model.CapacityFactorCHP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CF_CHP[g][t])
        self.model.PmaxCHP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_CHP[g])
        self.model.PowerCHPCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] <= m.CapacityFactorCHP[g,t] * m.PmaxCHP[g])