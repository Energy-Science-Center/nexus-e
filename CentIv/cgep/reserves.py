from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

class Reserves:
    def __init__(self, state, gens):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.generators = gens

        self.up_FRR_reserves_at = [{} for _ in range(num_snaphots)] #holder for RR up reserves provided by each generator at each time period
        self.down_FRR_reserves_at = [{} for _ in range(num_snaphots)] #holder for FCR down reserves provided by each generator at each time period
        self.up_RR_reserves_at = [{} for _ in range(num_snaphots)] #holder for FRR down reserves provided by each generator at each time period
        self.down_RR_reserves_at = [{} for _ in range(num_snaphots)] #holder for RR down reserves provided by each generator at each time period

    def gens(self):
        return self.generators

    def select_gen(self, l):
        return SetOf(l)
    
    #needs to be called for all generator types which do not provide reserves      
    def set_no_reserves(self, GENS):  
        for t in self.model.TimePeriods:
            for g in GENS:
                self.up_FRR_reserves_at[t][g] = 0
                self.up_RR_reserves_at[t][g] = 0

                self.down_FRR_reserves_at[t][g] = 0
                self.down_RR_reserves_at[t][g] = 0
                
    #needs to be called for all generator types which do not provide tertiary reserves           
    def set_no_RRreserves(self, GENS):  
        for t in self.model.TimePeriods:
            for g in GENS:
                self.up_RR_reserves_at[t][g] = 0
                self.down_RR_reserves_at[t][g] = 0
                
    def set_system_reserve_constraints(self,
                                       system_up_requirements_FRR, system_down_requirements_FRR,
                                       system_up_requirements_RR, system_down_requirements_RR,
                                       system_up_additional_RR=None, system_down_additional_RR=None):                                      
        for g in self.generators:
            if g not in self.up_FRR_reserves_at[0]:
                raise Exception('FRR up reserves not set for generator {}'.format(g))
            if g not in self.up_RR_reserves_at[0]:
                raise Exception('RR up reserves not set for generator {}'.format(g))
            if g not in self.down_FRR_reserves_at[0]:
                raise Exception('FRR down reserves not set for generator {}'.format(g))
            if g not in self.down_RR_reserves_at[0]:
                raise Exception('RR down reserves not set for generator {}'.format(g))
        
        self.model.UpReserveFRR = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(self.up_FRR_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_FRR[t])
        self.model.DownReserveFRR = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(self.down_FRR_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_FRR[t])
        
        if system_up_additional_RR is not None:
            self.model.UpReserveRR = Constraint(
                self.model.TimePeriods,
                rule=lambda m,t: sum(self.up_RR_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_RR[t] + system_up_additional_RR)
        else:
            self.model.UpReserveRR = Constraint(
                self.model.TimePeriods,
                rule=lambda m,t: sum(self.up_RR_reserves_at[t][g] for g in self.model.Generators) >= system_up_requirements_RR[t])
            
        if system_down_additional_RR is not None:
            self.model.DownReserveRR = Constraint(
                self.model.TimePeriods,
                rule=lambda m,t: sum(self.down_RR_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_RR[t] + system_down_additional_RR)
        else:
            self.model.DownReserveRR = Constraint(
                self.model.TimePeriods,
                rule=lambda m,t: sum(self.down_RR_reserves_at[t][g] for g in self.model.Generators) >= system_down_requirements_RR[t])