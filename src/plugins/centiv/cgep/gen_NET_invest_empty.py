from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class NETInvest_Empty:
    def __init__(self, state, cand_NET):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        
    """
    Generates an expression for the investment costs of all NET technologies in the system
    """
    def get_investment_cost_NET(self, max_power_cand, investment_cost_gens, fixed_cost_gens):
        return 0             
                   
    """
    Generates an expression for the operational costs of all candidate NET units in the system

    """                                      
    def get_operational_costs_NET_disagg(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, tpRes, baseMVA):
        return 0 

    def get_operational_costs_NET_disagg_LP(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, baseMVA):
        return 0                    
                  
            

            
            
            
            
            
            