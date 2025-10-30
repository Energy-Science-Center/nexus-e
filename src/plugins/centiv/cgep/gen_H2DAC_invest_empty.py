from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class P2XInvest_Empty:
    def __init__(self, state, cand_P2X, cand_H2notconn):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model

    """
    Generates an expression for the investment costs of all candidate P2X units in the system
    """
    def get_investment_cost_P2X(self, investment_cost_H2_reconv, investment_cost_H2_EL, investment_cost_H2_stor, investment_cost_CH4DAC, fixedcost_H2EL = None, fixedcost_H2storage = None, fixedcost_CH4DAC = None, fixedcost_H2gen = None):
        return 0  #no investment costs added to the objective f-n

    """
    Generates an expression for the generation costs of all candidate P2X units in the system
    -- includes the VOM cost of the meth+dac unit in addition to the VOM for reconversion
    """
    def get_operational_costs_P2G2P_disagg(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, methdac_VOM, tpRes, baseMVA):
        return 0  #no investment costs added to the objective f-n
    
    """
    Generates an expression for the import costs of all candidate P2X units in the system
    """
    def get_import_costs_P2G2P_disagg(self, GENS, H2import_price, CH4import_price, tpRes, baseMVA):
        return 0
    
    """
    Generates an expression for the generation costs of all candidate P2X units in the system - linear resolve
    -- includes the VOM cost of the meth+dac unit in addition to the VOM for reconversion
    """
    def get_operational_costs_P2G2P_disagg_LP(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, methdac_VOM, baseMVA):
        return 0
    
    """
    Generates an expression for the import costs of all candidate P2X units in the system - linear re-solve
    """
    def get_import_costs_P2G2P_disagg_LP(self, GENS, H2import_price, CH4import_price, baseMVA):
        return 0

    """
    Generates an expression for the revenue from selling hydrogen
    -- H2 market price is a single value (no time series)
    -- needs to be subtracted in the objective f-n
    """
    def get_H2_revenue(self, GENS, H2_market_price_sell, tpRes, baseMVA):
        return 0
    
    def get_H2_revenue_LP(self, GENS, H2_market_price_sell, baseMVA):
        return 0
        
    def get_CH4_revenue(self, GENS, CH4_market_price_sell, tpRes, baseMVA):
        return 0
    
    def get_CH4_revenue_LP(self, GENS, CH4_market_price_sell, baseMVA):
        return 0
    """
    Generates an expression for the revenue from capturing CO2
    -- CO2 price is a single value (no time series)
    -- needs to be subtracted in the objective f-n
    """
    def get_CO2_revenue(self, GENS, CO2_price_sell, CH4toCO2_factor, tpRes, baseMVA):
        return 0
    
    def get_CO2_revenue_LP(self, GENS, CO2_price_sell, CH4toCO2_factor, baseMVA):
        return 0