from __future__ import division
from pyomo.environ import * # @UnusedWildImport

import numpy as np
import math
from value_format import ValueFormatter

class InvestGenerators(object):
    def __init__(self, state, candidates, candidates_nondisp):
        self.state = state
        self.model = state.model
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots

        self.model.CandGenerators = candidates #These are only conv units which use UnitOn
        self.model.CandGenerators_nondisp = candidates_nondisp #These are RES units 
        
        """
        variables
        """
        self.model.UnitBuild = Var(
            self.model.CandGenerators, 
            within=IntegerSet(bounds=(0,1))) #within=IntegerSet(bounds=(0,1)) binary variable to indicate whether investment in a candidate unit is made
        self.model.CandCapacityNonDisp = Var(
            self.model.CandGenerators_nondisp, 
            within=NonNegativeReals) #continuous variable for investments in generation capacity  

        self.model.OnOnlyBuild = Constraint(
            self.model.CandGenerators,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.UnitOn[g,t] <= m.UnitBuild[g]) #Unit can be operated (on/off) only if it is built (link between operational and investment constraints) 
        
        #constraints for numertical stability 
        self.model.GenPowerBuild = Constraint(
            self.model.CandGenerators,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.UnitBuild[g] * 1e+4) # should be set to the value of the largest UC candidate generator's installed capacity
        
        #powerconsumed for all CandGenerators_nondisp should be 0 
        self.model.NoPowerConsumedCandNonDisp = Constraint(
            self.model.CandGenerators_nondisp,
            self.model.TimePeriods,
            rule=lambda m,g,t: state.power_consumed(g,t) == 0)

    """
    defines the additional required tertiary up reserve in case RES investments (wind and solar PV) are made 
    - this function considers also distIv investments
    """
    def additional_up_RR(self, list_wind_cand, coeff_wind_up, list_pv_cand, distIv_solar_investments, coeff_solar_up):        
        return sum(self.model.CandCapacityNonDisp[cand] for cand in list_wind_cand) * coeff_wind_up + (sum(self.model.CandCapacityNonDisp[cand] for cand in list_pv_cand) + distIv_solar_investments) * coeff_solar_up

    """
    defines the additional required tertiary up reserve in case RES investments (wind and solar PV) are made 
    - this function considers also distIv investments
    """
    def additional_down_RR(self, list_wind_cand, coeff_wind_down, list_pv_cand, distIv_solar_investments, coeff_solar_down):
        return sum(self.model.CandCapacityNonDisp[cand] for cand in list_wind_cand) * coeff_wind_down + (sum(self.model.CandCapacityNonDisp[cand] for cand in list_pv_cand) + distIv_solar_investments) * coeff_solar_down

    """
    Generates an expression for the investment costs of all generators in the system as long as we have binary decisions on investments 
    valid for both the dispatchable and nondispatchable gens (in case we want binary investments in nondispatchable gens we need to have an empty list for candidates_nondisp)
    tested in test15inv (test_trivial)
    """
    def get_investment_cost(self, max_power, investment_cost, fixed_cost = None, investment_cost_energy = None, max_energy_capacity = None):
        self.model.MaxPowerInv = Param(
                self.model.CandGenerators,
                within=NonNegativeReals,
                initialize=lambda m,g: max_power[g])
        self.model.InvCost = Param(
                self.model.CandGenerators,
                within=NonNegativeReals,
                initialize=lambda m,g: investment_cost[g])
        if fixed_cost and investment_cost_energy:# and max_energy_capacity:
            return sum((self.model.InvCost[g] * self.model.MaxPowerInv[g] + fixed_cost[g] * self.model.MaxPowerInv[g] + investment_cost_energy[g] * max_energy_capacity[g]) * self.model.UnitBuild[g] for g in self.model.CandGenerators) #we need to multiply by max installed power because values in the tables are in CHF/MW
        elif fixed_cost:
            return sum((self.model.InvCost[g] * self.model.MaxPowerInv[g] + fixed_cost[g] * self.model.MaxPowerInv[g]) * self.model.UnitBuild[g] for g in self.model.CandGenerators) #we need to multiply by max installed power because values in the tables are in CHF/MW
        else:    
            return sum(self.model.InvCost[g] * self.model.MaxPowerInv[g] * self.model.UnitBuild[g] for g in self.model.CandGenerators) #we need to multiply by max installed power because values in the tables are in CHF/MW
     
    """
    Generates the operational constraints and investment costs for all non-dispatchable candidate units together
    -uses as input time series of max production per candidate unit (Pres_prod)
    -Only for Wind and PV power plants (time series for RoR are in terms of CF, not production profile)
    """
    def set_operation_nondisp_simple(self, max_power_nondisp, Pres_prod, baseMVA):
        self.model.MaxPowerInvNonDisp = Param(
                self.model.CandGenerators_nondisp,
                within=NonNegativeReals,
                initialize=lambda m,g: max_power_nondisp[g])
        self.model.PowerProductionRES = Param(
                self.model.CandGenerators_nondisp,
                self.model.TimePeriods,
                within=NonNegativeReals,
                initialize=lambda m,g,t: (Pres_prod[g][t]/baseMVA)/m.MaxPowerInvNonDisp[g])
        self.model.MaxInstCapCon = Constraint(
                self.model.CandGenerators_nondisp,
                rule=lambda m,g: m.CandCapacityNonDisp[g] <= m.MaxPowerInvNonDisp[g]) 
        self.model.PowerProductionCandidateCon2 = Constraint(
                self.model.CandGenerators_nondisp, 
                self.model.TimePeriods,
                rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.PowerProductionRES[g,t] * m.CandCapacityNonDisp[g])
                
    def get_invcost_nondisp_simple(self, investment_cost_nondisp, fixed_cost_nondisp):
        self.model.InvCostNonDisp = Param(
                self.model.CandGenerators_nondisp,
                within=NonNegativeReals,
                initialize=lambda m,g: investment_cost_nondisp[g])
        if fixed_cost_nondisp:
            return sum((self.model.InvCostNonDisp[g] + fixed_cost_nondisp[g]) * self.model.CandCapacityNonDisp[g] for g in self.model.CandGenerators_nondisp) 
        else:
            return sum(self.model.InvCostNonDisp[g] * self.model.CandCapacityNonDisp[g] for g in self.model.CandGenerators_nondisp) 
                

    #----------------------------------------------------------------------
    #THESE FUNCTIONS ARE NOT COMPATIBLE WITH THE CURRENT DATABASE
    #----------------------------------------------------------------------
    """
    Condensed way to generate the operational constraints and investment costs for all non-dispatchable candidate units together
    tested in test22invRES (test_trivial)
    Slow because for every constraint we loop over a list in order to decide whether we want to build a constraint or not (faster if input is a set instead of a list)
    Requires that you have already declared set_pv_power, set_hydro_power_RoR and set_wind_power_curve for all existing and candidate generators
    """
    def get_investment_cost_nondisp(self, max_power_nondisp, investment_cost_nondisp, list_pv_cand, list_RoR_cand, list_wind_cand, fixed_cost_nondisp = None):
        self.model.MaxPowerInvNonDisp = Param(
                self.model.CandGenerators_nondisp,
                within=NonNegativeReals,
                initialize=lambda m,g: max_power_nondisp[g])
        self.model.InvCostNonDisp = Param(
                self.model.CandGenerators_nondisp,
                within=NonNegativeReals,
                initialize=lambda m,g: investment_cost_nondisp[g])
        self.model.MaxInstCapCon = Constraint(
                self.model.CandGenerators_nondisp,
                rule=lambda m,g: m.CandCapacityNonDisp[g]*m.MaxPowerInvNonDisp[g] <= m.MaxPowerInvNonDisp[g]) 
        self.model.PowerCFPVCandidateCon = Constraint(
                self.model.CandGenerators_nondisp,
                self.model.TimePeriods,
                rule=lambda m,g,t: 
                    Constraint.Skip if (g not in list_pv_cand) else m.PowerGenerated[g,t] <= m.CapacityFactorPV[g,t] * m.CandCapacityNonDisp[g])
        self.model.PowerCFHydroCandidateCon = Constraint(
                self.model.CandGenerators_nondisp,
                self.model.TimePeriods,
                rule=lambda m,g,t: 
                    Constraint.Skip if (g not in list_RoR_cand) else m.PowerGenerated[g,t] <= m.CapacityFactorHydro[g,t] * m.CandCapacityNonDisp[g])

        def constraint_equation_wind(m,g,t):
            if m.WindSpeed[g,t] < m.Cut_In_WindSpeed[g] or m.WindSpeed[g,t] > m.Cut_Off_WindSpeed[g]:
                return (m.PowerGenerated[g,t] == 0) #no power is generated because we are either below cut-in or above cut-off wind speed 
            elif self.model.WindSpeed[g,t] >= self.model.RatedWindSpeed[g] and self.model.WindSpeed[g,t] <= self.model.Cut_Off_WindSpeed[g]:
                return (m.PowerGenerated[g,t] <= m.CandCapacityNonDisp[g]) #maximum power output (we are above rated wind speed but below the cut-off)
            elif self.model.WindSpeed[g,t] >= m.Cut_In_WindSpeed[g] and self.model.WindSpeed[g,t] < self.model.RatedWindSpeed[g]:
                return (m.PowerGenerated[g,t] <= m.CandCapacityNonDisp[g] * ((m.WindSpeed[g,t] - m.Cut_In_WindSpeed[g]) / (m.RatedWindSpeed[g] - m.Cut_In_WindSpeed[g]))) #we are on the linear part of the power curve (the power generated is non-zero and below nominal power)
       
        self.model.PowerWindPowerCurveCandidateCon = Constraint(
            self.model.CandGenerators_nondisp, 
            self.model.TimePeriods,
            rule=lambda m,g,t: 
                Constraint.Skip if (g not in list_wind_cand) else constraint_equation_wind(m,g,t)) 
        
        if fixed_cost_nondisp:
            return sum((self.model.InvCostNonDisp[g] + fixed_cost_nondisp[g]) * self.model.CandCapacityNonDisp[g] for g in self.model.CandGenerators_nondisp) 
        else:
            return sum(self.model.InvCostNonDisp[g] * self.model.CandCapacityNonDisp[g] for g in self.model.CandGenerators_nondisp) 
    
    """
    Generates the operational constraints and an expression for the investment costs for all candidate pv units
    tested in test23invRES_separatefunctions (test_trivial)
    """
    def get_investment_cost_pv(self, GENS, solar_rad_cand, Pmax_PV_cand, pv_maxrad_cand, investment_cost_candpv, fixed_cost_candpv = None):
        self.model.CapacityFactorCandidatePV = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: solar_rad_cand[g][t]/pv_maxrad_cand)
        self.model.MaxPowerCandidatePV = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_PV_cand[g])
        self.model.PowerCFPVCandidateCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.CapacityFactorCandidatePV[g,t] * m.CandCapacityNonDisp[g])
        if fixed_cost_candpv:
            return sum((investment_cost_candpv[g] + fixed_cost_candpv[g]) * self.model.CandCapacityNonDisp[g] for g in GENS) 
        else:
            return sum(investment_cost_candpv[g] * self.model.CandCapacityNonDisp[g] for g in GENS) 
    
    """
    Generates the operational constraints and an expression for the investment costs for all candidate RoR units
    not tested in test_trivial
    """
    def get_investment_cost_RoR(self, GENS, CF_Hydro_cand, Pmax_Hydro_cand, investment_cost_candRoR, fixed_cost_candRoR = None):  
        self.model.CapacityFactorCandidateHydro = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CF_Hydro_cand[g][t])
        self.model.PmaxCandidateHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: Pmax_Hydro_cand[g])
        self.model.PowerHydroCandidateCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.CapacityFactorCandidateHydro[g,t] * m.CandCapacityNonDisp[g])
        if fixed_cost_candRoR:
            return sum((investment_cost_candRoR[g] + fixed_cost_candRoR[g]) * self.model.CandCapacityNonDisp[g] for g in GENS) 
        else:
            return sum(investment_cost_candRoR[g] * self.model.CandCapacityNonDisp[g] for g in GENS) 
    
    """
    Generates the operational constraints and an expression for the investment costs for all candidate wind units
    tested in test23invRES_separatefunctions (test_trivial)
    """
    def get_investment_cost_wind(self, GENS, P_nom_wind_cand, wind_speed_cand, cut_in_wind_speed_cand, cut_off_wind_speed_cand, rated_wind_speed_cand, P_max_cand, investment_cost_candwind, fixed_cost_candwind = None):
        self.model.PowerWindCandidateNom = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: P_nom_wind_cand[g]) #nominal power of wind turbine
        self.model.WindSpeedCandaidate = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: wind_speed_cand[g][t])
        self.model.Cut_In_WindSpeedCandidate = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: cut_in_wind_speed_cand[g])
        self.model.Cut_Off_WindSpeedCandidate = Param(
            GENS, 
            within=NonNegativeReals, 
            initialize=lambda m,g: cut_off_wind_speed_cand[g])
        self.model.RatedWindSpeedCandidate = Param(
            GENS, 
            within=NonNegativeReals, 
            initialize=lambda m,g: rated_wind_speed_cand[g])
        self.model.PmaxWindCandidate = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: P_max_cand[g]) #this is the max installed power of the wind park 

        def constraint_equation_wind_candidate(m,g,t):
            if wind_speed_cand[g][t] < cut_in_wind_speed_cand[g] or wind_speed_cand[g][t] > cut_off_wind_speed_cand[g]:
                return (m.PowerGenerated[g,t] == 0) 
            elif wind_speed_cand[g][t] >= rated_wind_speed_cand[g] and wind_speed_cand[g][t] <= cut_off_wind_speed_cand[g]:
                return (m.PowerGenerated[g,t] <= m.CandCapacityNonDisp[g]) 
            elif wind_speed_cand[g][t] >= cut_in_wind_speed_cand[g] and wind_speed_cand[g][t] < rated_wind_speed_cand[g]:
                return (m.PowerGenerated[g,t] <= m.CandCapacityNonDisp[g] * ((m.WindSpeedCandaidate[g,t] - m.Cut_In_WindSpeedCandidate[g]) / (m.RatedWindSpeedCandidate[g] - m.Cut_In_WindSpeedCandidate[g]))) 

        self.model.WindPowerPowerCurveCandidateCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=constraint_equation_wind_candidate)
         
        if fixed_cost_candwind:
            return sum((investment_cost_candwind[g] + fixed_cost_candwind[g]) * self.model.CandCapacityNonDisp[g] for g in GENS) 
        else:
            return sum(investment_cost_candwind[g] * self.model.CandCapacityNonDisp[g] for g in GENS) 
    
    """
    post-processing routines
    """
    def get_units_built(self):
        units = {}
        for unit in self.model.CandGenerators:
            units[unit] = int(value(self.model.UnitBuild[unit]))
        return units

    def get_capacity_built(self,max_power_nondisp,baseMVA=None):
        capacities = {}
        for unit in self.model.CandGenerators_nondisp:
            if baseMVA:
                capacities[unit] = (value(self.model.CandCapacityNonDisp[unit])*baseMVA)
            else:
                original_capacity = ValueFormatter(float(value(self.model.CandCapacityNonDisp[unit])))
                capacities[unit] = (
                    original_capacity
                    .truncate(decimal=3)
                    .round_up(decimal=2)
                    .get_formatted_value()
                )
            capacities[unit] = min(capacities[unit], max_power_nondisp[unit])
        return capacities

    def get_curtailments_newRES(self, cand_RES):
        if not hasattr(self.model, 'PowerProductionRES'):
            raise Exception('Missing call to get_investment_cost_nondisp_simple')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CandCapacityNonDisp[cand_RES] * self.model.PowerProductionRES[cand_RES,time] - self.model.PowerGenerated[cand_RES,time])
        return results