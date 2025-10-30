from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np
from .value_format import ValueFormatter

class P2XInvest:
    def __init__(self, state, cand_P2X, cand_H2notconn):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.model.CandP2X = cand_P2X #all candidate P2X units

        """
        investment variables
        """
        #H2 storage
        self.model.H2StorageInvest = Var(
            self.model.CandP2X,
            within=NonNegativeReals) #size of the H2 storage (tonnes H2 divided by baseMVA)
        #electrolyzer
        self.model.H2ElectrolizerInvest = Var(
            self.model.CandP2X, 
            within=NonNegativeReals, bounds=(0,1)) #cont. variable indicating how much of the candidate electrolyzer is built
        #fuel cell/igcc - reconversion technology (H2 to electricity)
        self.model.H2GenInvest = Var(
            self.model.CandP2X, 
            within=NonNegativeReals, bounds=(0,1)) #cont. variable indicating how much of the candidate H2-fueled reconversion technology (i.e. fuel cell , IGCC, etc) is built 
        #methanation reactor with direct air capture (DAC) unit 
        self.model.DACCH4Invest = Var(
            self.model.CandP2X, 
            within=NonNegativeReals, bounds=(0,1)) #cont. variable indicating how much of the DAC+Methanation candidate unit is built 
        
        """
        operation variables
        """
        #H2
        self.model.H2SoC = Var(
            self.model.CandP2X, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #state of charge of candidate H2 storage in each time period (tonnes H2 divided by baseMVA)
        self.model.H2ForMarket = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #contribution towards hydrogen demand/market of candidate H2 storage in each time period (tonnes H2 divided by baseMVA)
        #for Id in cand_H2notconn:
        #    for t in range(self.num_snaphots):
        #        self.model.H2ForMarket[Id,t].fix(0) #fix the value of H2ForMarket variable to 0 for all P2G2P units which are not on the "European H2 backbone" 
        self.model.H2ForMethanation = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #H2 which leaves the storage and undergoes methanation in each time period (tonnes H2 divided by baseMVA)
        self.model.H2ForReconversion = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #H2 used by the reconversion technology (fuel cell, IGCC turbine, etc.) in each time period (tonnes H2 divided by baseMVA)
        self.model.H2FromElectrolysis = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #H2 produced by electrolysis in each time period (tonnes H2 divided by baseMVA)
        #CH4
        self.model.CH4ForMarket = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #CH4 (in MWh_th divided by baseMVA) that goes in the gas grid
        #MWh-equivalent variables
        self.model.PconDACCH4 = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #electricity consumed by the DAC+methanation unit, in MWh-el divided by baseMVA
        self.model.PconEL = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #electricity consumed by the electrolyzer, in MWh-el divided by baseMVA
        self.model.PgenReconversion = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #electricity produced by the reconversion technology (i.e. fuel cell , IGCC, etc), in MWh-el divided by baseMVA

        """
        import variables
        """
        self.model.H2Import = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #units are tonnes H2 divided by baseMVA
        # TODO: remove hard-coded limit in rule, fetch it from input data instead
        self.model.H2ImportLimitCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2Import[g,t] <= 0.5) #unit are tonne-H2 per hour divided by baseMVA because RHS already divided by basemva #we choose a very high value so we don't have free variables
        for Id in cand_H2notconn:
            for t in range(self.num_snaphots):
                self.model.H2Import[Id,t].fix(0) #fix the value of H2Import variable to 0 for all P2G2P units which are not on the "European H2 backbone" - can't import H2 because no infrastructure
        self.model.CH4Import = Var(
            self.model.CandP2X,
            self.model.TimePeriods, 
            within=NonNegativeReals) #units are MWh-LHV divided by baseMVA
        # TODO: remove hard-coded limit in rule, fetch it from input data instead
        self.model.CH4ImportLimitCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.CH4Import[g,t] <= 402.4) #units are MWh-LHV per hour divided by baseMVA because RHS already divided by basemva #we choose a very high value so we don't have free variables

        #limit PowerGenerated[g,t] and PowerConsumed[g,t] of the whole P2X system
        self.model.PgenerateP2XSystemCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] == m.PgenReconversion[g,t]) 
        self.model.PconsumeP2XSystemCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == m.PconDACCH4[g,t] + m.PconEL[g,t])

    def _H2SoC_before(self, g, t):
        if t == 0:
            return self.model.CapacityT0CandH2[g]
        else:
            return self.model.H2SoC[g,t-1]

    """
    sets investment constraints
    """
    def set_P2X_invest(self, max_power_discharge_reconversion, max_power_charge_electrolyzer, max_power_gen_CH4DAC, max_capacity_H2Stor):
        #define the maximum size of each technology to invest in (H2 reconversion, electrolyzer, H2Storage, DAC+Methanation)
        self.model.GenMaxH2Reconversion = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_discharge_reconversion[g]) #MW divided by baseMVA
        self.model.ChargeMaxEL = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_charge_electrolyzer[g]) #MW divided by baseMVA
        self.model.GenMaxDACCH4 = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_gen_CH4DAC[g]) #MW_th divided by baseMVA
        self.model.H2StoreMax = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: max_capacity_H2Stor[g]) #tonnes H2 divided by baseMVA
     
        #electrolyzer consumption limited by Pmax of candidate electrolyzer unit
        self.model.ElectrolyzerOnOnlyBuildCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PconEL[g,t] <= m.H2ElectrolizerInvest[g] * m.ChargeMaxEL[g])
        #reconversion technology (i.e. fuel cell , IGCC, etc) limited by Pmax of candidate unit
        self.model.ReconversionOnOnlyBuildCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PgenReconversion[g,t] <= m.H2GenInvest[g] * m.GenMaxH2Reconversion[g])
        #H2 storage size limited by Emax of candidate storage unit
        self.model.H2SoCOnOnlyBuildCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2SoC[g,t] <= m.H2StorageInvest[g]) #max SoC constraint (tonnes H2 divided by baseMVA)
        self.model.H2CapInvestCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2StorageInvest[g] <= m.H2StoreMax[g]) #max H2 storage constraint (tonnes H2 divided by baseMVA)
        #Methanation + DAC size limited by Pmax of candidate unit
        self.model.CH4OnOnlyBuildCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.CH4ForMarket[g,t] <= m.DACCH4Invest[g] * m.GenMaxDACCH4[g]) #max methanation constraint (MWh_th divided by baseMVA)

    """
    sets H2 storage balance
    """
    def set_H2_balance(self, tpResolution, maxdailywithdrawal, maxadailyinjection):
        #energy level @ T0
        self.model.CapacityT0CandH2= Var(
            self.model.CandP2X,
            within=NonNegativeReals) #tonnes H2
        self.model.CapacityT0CandH2Con = Constraint(
            self.model.CandP2X,
            rule=lambda m,g: m.CapacityT0CandH2[g] <= m.H2StorageInvest[g]) 
        #energy balance of hydrogen storage candidates (tonnes of H2)
        self.model.SoCH2CandCon = Constraint(
            self.model.CandP2X, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2SoC[g,t] == self._H2SoC_before(g,t) - m.H2ForMarket[g,t] * tpResolution - m.H2ForMethanation[g,t] * tpResolution - m.H2ForReconversion[g,t] * tpResolution + m.H2FromElectrolysis[g,t] * tpResolution + m.H2Import[g,t] * tpResolution)       
        #H2 storage level can't increase/decrease more than 5% per day (ONLY FOR LRC)
        #index_con_H2storage = range(int(num_days))
        #self.model.DailyShiftChangeH2storCon = Constraint(
        #    self.model.CandP2X,
        #    index_con_H2storage,
        #    rule=lambda m,g,i: m.H2SoC[g,24*i] - m.H2SoC[g,24*i+23] <= maxdailyH2storagechange * m.H2StorageInvest[g])
        #self.model.DailyShiftChangeH2storCon2 = Constraint(
        #    self.model.CandP2X,
        #    index_con_H2storage,
        #    rule=lambda m,g,i: - m.H2SoC[g,24*i] + m.H2SoC[g,24*i+23] <= maxdailyH2storagechange * m.H2StorageInvest[g])
        self.model.HourlyWithdrawalInjectionH2storCon = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2SoC[g,t] - self._H2SoC_before(g,t) <= (maxadailyinjection / 24) * m.H2StorageInvest[g] * tpResolution)
        self.model.HourlyWithdrawalInjectionH2storCon2 = Constraint(
            self.model.CandP2X,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2SoC[g,t] - self._H2SoC_before(g,t) >= - (maxdailywithdrawal / 24) * m.H2StorageInvest[g] * tpResolution)
        self.model.EqualEnergyH2storageCon = Constraint(
            self.model.CandP2X, 
            rule=lambda m,g: m.H2SoC[g,self.num_snaphots-1] >= (m.CapacityT0CandH2[g]))

    """
    link H2 and electricity flows
    """
    def set_H2_constraints(self, electricity2H2_rate, H2toelectricity_rate):
        self.model.ElectricityToH2 = Constraint(
            self.model.CandP2X, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2FromElectrolysis[g,t] == m.PconEL[g,t] * electricity2H2_rate[g])
        self.model.H2ToElectricity = Constraint(
            self.model.CandP2X, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PgenReconversion[g,t] == m.H2ForReconversion[g,t] * H2toelectricity_rate[g])
    
    """
    set methanation constraints
    """
    def set_CH4_constraints(self, H2toCH4_rate, EltoCH4_rate, pmin):
        self.model.H2toCH4Con = Constraint(
            self.model.CandP2X, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.H2ForMethanation[g,t] * H2toCH4_rate[g] == m.CH4ForMarket[g,t])
        self.model.EltoCh4Con = Constraint(
            self.model.CandP2X, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PconDACCH4[g,t] * EltoCH4_rate[g] == m.CH4ForMarket[g,t])
        self.model.CH4PminCon = Constraint(
            self.model.CandP2X, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.CH4ForMarket[g,t] >= pmin * m.GenMaxDACCH4[g] * m.DACCH4Invest[g])

    """
    Generates an expression for the investment costs of all candidate P2X units in the system
    """
    def get_investment_cost_P2X(self, investment_cost_H2_reconv, investment_cost_H2_EL, investment_cost_H2_stor, investment_cost_CH4DAC, fixedcost_H2EL = None, fixedcost_H2storage = None, fixedcost_CH4DAC = None, fixedcost_H2gen = None):
        if not hasattr(self.model, 'GenMaxH2Reconversion'):
            raise Exception('Missing call to set_P2X_invest')
        if not hasattr(self.model, 'ChargeMaxEL'):
            raise Exception('Missing call to set_P2X_invest')
        if not hasattr(self.model, 'H2StoreMax'):
            raise Exception('Missing call to set_P2X_invest')
        if not hasattr(self.model, 'GenMaxDACCH4'):
            raise Exception('Missing call to set_P2X_invest')

        self.model.InvCostH2Reconversion = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_H2_reconv[g])
        self.model.InvCostElectrolyzer = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_H2_EL[g])
        self.model.InvCostH2Storage = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_H2_stor[g])
        self.model.InvCostDACCH4 = Param(
            self.model.CandP2X,
            within=NonNegativeReals,
            initialize=lambda m,g: investment_cost_CH4DAC[g])
        if fixedcost_H2EL and fixedcost_H2storage and fixedcost_CH4DAC and fixedcost_H2gen:
            return sum((self.model.InvCostH2Reconversion[g] + fixedcost_H2gen[g]) * self.model.H2GenInvest[g] * self.model.GenMaxH2Reconversion[g]  + 
                   (self.model.InvCostElectrolyzer[g] + fixedcost_H2EL[g]) * self.model.H2ElectrolizerInvest[g] * self.model.ChargeMaxEL[g] +
                   (self.model.InvCostH2Storage[g] + fixedcost_H2storage[g]) * self.model.H2StorageInvest[g] +
                   (self.model.InvCostDACCH4[g] + fixedcost_CH4DAC[g]) * self.model.DACCH4Invest[g] * self.model.GenMaxDACCH4[g] for g in self.model.CandP2X) 
        else:
            return sum(self.model.InvCostH2Reconversion[g] * self.model.H2GenInvest[g] * self.model.GenMaxH2Reconversion[g]  + 
                   self.model.InvCostElectrolyzer[g] * self.model.H2ElectrolizerInvest[g] * self.model.ChargeMaxEL[g] +
                   self.model.InvCostH2Storage[g] * self.model.H2StorageInvest[g] +
                   self.model.InvCostDACCH4[g] * self.model.DACCH4Invest[g] * self.model.GenMaxDACCH4[g] for g in self.model.CandP2X)

    """
    Generates an expression for the generation costs of all candidate P2X units in the system
    -- includes the VOM cost of the meth+dac unit in addition to the VOM for reconversion
    """
    def get_operational_costs_P2G2P_disagg(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, methdac_VOM, tpRes, baseMVA):
        self.model.FuelPriceP2G2P = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffP2G2P = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceP2G2P = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateP2G2P = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMP2G2P = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        self.model.MethDACVOMP2G2P = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: methdac_VOM[g] * baseMVA)
        m = self.model
        return (sum((m.FuelPriceP2G2P[g,t] / m.FuelEffP2G2P[g] + m.CO2PriceP2G2P[g,t] * m.CO2RateP2G2P[g] + m.NonFuelVOMP2G2P[g]) * m.PowerGenerated[g,t] + m.MethDACVOMP2G2P[g] * m.CH4ForMarket[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
    
    """
    Generates an expression for the import costs of all candidate P2X units in the system
    """
    def get_import_costs_P2G2P_disagg(self, GENS, H2import_price, CH4import_price, tpRes, baseMVA):
        self.model.H2ImportPriceP2G2P = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: H2import_price[g][t] * baseMVA)
        self.model.CH4ImportPriceP2G2P = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CH4import_price[g][t] * baseMVA)
        m = self.model
        return (sum(m.H2ImportPriceP2G2P[g,t] * m.H2Import[g,t] + m.CH4ImportPriceP2G2P[g,t] * m.CH4Import[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes
    
    """
    Sets H2 import limit
    -- limit hourly H2 import for all import locations (tonnes)
    -- limit is already divided by baseMVA
    """
    def set_H2_importlimit_inequality(self, GENS):
        self.model.H2ImportLimitCon = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(m.H2Import[g,t] for g in GENS) <= 0.469) #value based on EP2050+ 49.2853 PJ of H2 in Zero-B scenario (maybe later include this number in the db), unit are tonne-H2 per hour divided by 100 because RHS already divided by basemva 

    """
    Sets CH4 import limit
    -- limit hourly CH4 import for all import locations (tonnes)
    -- ??limit is already divided by baseMVA
    """
    def set_CH4_importlimit_inequality(self, GENS):
        # TODO: remove hard-coded limit in rule, fetch it from input data instead
        self.model.CH4ImportLimitCon = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: sum(m.CH4Import[g,t] for g in GENS) <= 402.4) #value based on current infrastructure than can import 965.9 GWh-LHV/day (maybe later include this number in the db), unit are MWh-LHV per hour divided by 100 because RHS already divided by basemva

    """
    Generates an expression for the generation costs of all candidate P2X units in the system - linear resolve
    -- includes the VOM cost of the meth+dac unit in addition to the VOM for reconversion
    """
    def get_operational_costs_P2G2P_disagg_LP(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, methdac_VOM, baseMVA):
        self.model.FuelPriceP2G2PLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffP2G2PLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceP2G2PLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateP2G2PLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMP2G2PLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        self.model.MethDACVOMP2G2PLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: methdac_VOM[g] * baseMVA)
        m = self.model
        return sum((m.FuelPriceP2G2PLP[g,t] / m.FuelEffP2G2PLP[g] + m.CO2PriceP2G2PLP[g,t] * m.CO2RateP2G2PLP[g] + m.NonFuelVOMP2G2PLP[g]) * m.PowerGenerated[g,t] + m.MethDACVOMP2G2PLP[g] * m.CH4ForMarket[g,t] for t in self.model.TimePeriods for g in GENS)
    
    """
    Generates an expression for the import costs of all candidate P2X units in the system - linear re-solve
    """
    def get_import_costs_P2G2P_disagg_LP(self, GENS, H2import_price, CH4import_price, baseMVA):
        self.model.H2ImportPriceP2G2PLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: H2import_price[g][t] * baseMVA)
        self.model.CH4ImportPriceP2G2PLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CH4import_price[g][t] * baseMVA)
        m = self.model
        return sum((m.H2ImportPriceP2G2PLP[g,t] * m.H2Import[g,t] + m.CH4ImportPriceP2G2PLP[g,t] * m.CH4Import[g,t] for t in self.model.TimePeriods for g in GENS))

    def set_H2_hourly_demand_inequality(self, GENS, H2_demand_hourly):
        """
        Sets H2 demand constraint
        -- H2 demand is an hourly value (tonnes) and must be already divided by baseMVA to be consistent
        """
        self.model.H2DemandCon = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t:
                sum(m.H2ForMarket[g,t] for g in GENS) == H2_demand_hourly[t])

    def set_H2_annually_demand_inequality(self, GENS, H2_demand_annually, tpResolution):
        """
        Sets H2 demand constraint
        -- H2 demand is an annual value (tonnes) and must be already divided by baseMVA to be consistent
        """
        self.model.H2DemandCon = Constraint(
            rule=lambda m: 
                Constraint.Skip if H2_demand_annually == 0 
                else sum(m.H2ForMarket[g,t] * tpResolution for g in GENS for t in self.model.TimePeriods) >= H2_demand_annually)

    def set_CH4_hourly_demand_inequality(self, GENS, CH4_demand_hourly):
        """
        Sets CH4 demand constraint
        -- CH4 demand is an hourly value (MWh_th) and must be already divided by baseMVA to be consistent
        """
        self.model.CH4DemandCon = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: 
                sum(m.CH4ForMarket[g,t] + m.CH4Import[g,t] for g in GENS) == CH4_demand_hourly[t])

    def set_CH4_annually_demand_inequality(self, GENS, CH4_demand_annually, tpResolution):
        """
        Sets CH4 demand constraint
        -- CH4 demand is an annual value (MWh_th) and must be already divided by baseMVA to be consistent
        """
        self.model.CH4DemandCon = Constraint(
            rule=lambda m: 
                Constraint.Skip if CH4_demand_annually == 0 
                else sum(m.CH4ForMarket[g,t] * tpResolution + m.CH4Import[g,t] * tpResolution for g in GENS for t in self.model.TimePeriods) >= CH4_demand_annually)
    
    """
    Generates an expression for the revenue from selling hydrogen
    -- H2 market price is a single value (no time series)
    -- needs to be subtracted in the objective f-n
    """
    def get_H2_revenue(self, GENS, H2_market_price_sell, tpRes, baseMVA):
        self.model.H2PriceP2G2PSell = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: H2_market_price_sell[g][t] * baseMVA)
        m = self.model
        return sum(m.H2ForMarket[g,t] * tpRes * m.H2PriceP2G2PSell[g,t] for g in GENS for t in m.TimePeriods)
    
    def get_H2_revenue_LP(self, GENS, H2_market_price_sell, baseMVA):
        self.model.H2PriceP2G2PSellLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: H2_market_price_sell[g][t] * baseMVA)
        m = self.model
        return sum(m.H2ForMarket[g,t] * m.H2PriceP2G2PSellLP[g,t] for g in GENS for t in m.TimePeriods)

    """
    Generates an expression for the revenue from selling CH4
    -- CH4 market price is a single value (no time series)
    -- needs to be subtracted in the objective f-n
    """
    def get_CH4_revenue(self, GENS, CH4_market_price_sell, tpRes, baseMVA):
        self.model.CH4PriceP2G2PSell = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CH4_market_price_sell[g][t] * baseMVA)
        m = self.model
        return sum(m.CH4ForMarket[g,t] * tpRes * m.CH4PriceP2G2PSell[g,t] for g in GENS for t in m.TimePeriods)
    
    def get_CH4_revenue_LP(self, GENS, CH4_market_price_sell, baseMVA):
        self.model.CH4PriceP2G2PSellLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CH4_market_price_sell[g][t] * baseMVA)
        m = self.model
        return sum(m.CH4ForMarket[g,t] * m.CH4PriceP2G2PSellLP[g,t] for g in GENS for t in m.TimePeriods)

    """
    Generates an expression for the revenue from capturing CO2
    -- CO2 price is a single value (no time series)
    -- needs to be subtracted in the objective f-n
    """
    def get_CO2_revenue(self, GENS, CO2_price_sell, CH4toCO2_factor, tpRes, baseMVA):
        self.model.CO2PriceP2G2PSell = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CO2_price_sell[g][t] * baseMVA)
        m = self.model
        return sum((m.CH4ForMarket[g,t] / CH4toCO2_factor[g]) * tpRes * m.CO2PriceP2G2PSell[g,t] for g in GENS for t in m.TimePeriods)
    
    def get_CO2_revenue_LP(self, GENS, CO2_price_sell, CH4toCO2_factor, baseMVA):
        self.model.CO2PriceP2G2PSellLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: CO2_price_sell[g][t] * baseMVA)
        m = self.model
        return sum((m.CH4ForMarket[g,t] / CH4toCO2_factor[g]) * m.CO2PriceP2G2PSellLP[g,t] for g in GENS for t in m.TimePeriods)

    #deactivate constraints
    def deactivateStorageConstraint1(self, Id, t):
        self.model.HourlyWithdrawalInjectionH2storCon[Id, t].deactivate()
    def deactivateStorageConstraint2(self, Id, t):
        self.model.HourlyWithdrawalInjectionH2storCon2[Id, t].deactivate()
    def deactivateStorageConstraint3(self, Id):
        self.model.EqualEnergyH2storageCon[Id].deactivate()

    """
    post-processing routines
    """
    #-----------------------------------------
    # H2 - related variables
    #-----------------------------------------
    #get state-of-charge of H2 storages (hourly)
    def get_H2_SoC_inv(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2SoC[GENS,time])
        return results
    #get H2 storage capacity built
    def get_H2_storage_inv(self):
        storages = {}
        for unit in self.model.CandP2X:
            original_investment = ValueFormatter(float(value(self.model.H2StorageInvest[unit])))
            storages[unit] = (
                original_investment
                .truncate(decimal=3)
                .round_up(decimal=2)
                .get_formatted_value()
            )
        return storages
    #get reconversion newly installed power
    def get_H2_gen_inv(self):
        reconversion = {}
        for unit in self.model.CandP2X:
            original_investment = ValueFormatter(float(value(self.model.H2GenInvest[unit])))
            reconversion[unit] = (
                original_investment
                .truncate(decimal=3)
                .round_up(decimal=2)
                .get_formatted_value()
            )
        return reconversion
    #get electrolyzer newly installed power
    def get_H2_EL_inv(self):
        electrolyzers = {}
        for unit in self.model.CandP2X:
            original_investment = ValueFormatter(float(value(self.model.H2ElectrolizerInvest[unit])))
            electrolyzers[unit] = (
                original_investment
                .truncate(decimal=3)
                .round_up(decimal=2)
                .get_formatted_value()
            )
        return electrolyzers
    #get H2 for market/demand (hourly)
    def get_H2ForMarket_inv(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2ForMarket[GENS,time])
        return results
    #get electrolyzer consumption hourly (MWh_el)
    def get_consumption_EL_inv(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PconEL[GENS,time])
        return results
    #get reconversion generation hourly (MWh_el)
    def get_generation_reconv_inv(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PgenReconversion[GENS,time])
        return results
    #get H2 production by electrolyzer hourly (tonnes H2)
    def get_H2production_EL(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2FromElectrolysis[GENS,time])
        return results
    #get H2 for methanation hourly (tonnes H2)
    def get_H2production_4Meth(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2ForMethanation[GENS,time])
        return results
    #get H2 for reconversion hourly (tonnes H2)
    def get_H2production_GEN(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2ForReconversion[GENS,time])
        return results
    #-----------------------------------------
    # CH4 - related variables
    #-----------------------------------------
    #get how much CO2 is sequestered from air by the DAC unit (hourly) - back calculate from the variable CH4ForMarket[g,t]
    def get_CO2captured_CH4DAC_inv(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CH4ForMarket[GENS,time] / 5.066)
        return results
    #get the size of the CH4+DAC unit
    def get_CH4DAC_inv(self):
        meth_units = {}
        for unit in self.model.CandP2X:
            original_investment = ValueFormatter(float(value(self.model.DACCH4Invest[unit])))
            meth_units[unit] = (
                original_investment
                .truncate(decimal=3)
                .round_up(decimal=2)
                .get_formatted_value()
            )
        return meth_units
    #get CH4 for market/demand (hourly)
    def get_CH4ForMarket_inv(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CH4ForMarket[GENS,time])
        return results
    #get CH4 + DAC unit electricity consumption (hourly) 
    def get_CH4DAC_consumption_inv(self, GENS):
        results = np.zeros(self.num_snaphots)    
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PconDACCH4[GENS,time])
        return results
    #-----------------------------------------
    # Misc.
    #-----------------------------------------
    #get hourly operational costs
    #used for all p2g2p units after linear re-solve
    def get_hourly_gencost_P2G2P_LP(self, GENS):
        if not hasattr(self.model, 'FuelPriceP2G2PLP'):
            raise Exception('Missing call to get_operational_costs_P2G2P_disagg_LP')
        results = np.zeros(self.num_snaphots)    
        for time in range(self.num_snaphots):
            results[time] = value((self.model.FuelPriceP2G2PLP[GENS,time] / self.model.FuelEffP2G2PLP[GENS] + self.model.CO2PriceP2G2PLP[GENS,time] * self.model.CO2RateP2G2PLP[GENS] + self.model.NonFuelVOMP2G2PLP[GENS]) * self.model.PowerGenerated[GENS,time] + self.model.MethDACVOMP2G2PLP[GENS] * self.model.CH4ForMarket[GENS,time])
        return results
    #get imports and costs
    def get_H2imports(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2Import[GENS,time])
        return results
    def get_H2imports_costs(self, GENS):
        if not hasattr(self.model, 'H2ImportPriceP2G2PLP'):
            raise Exception('Missing call to get_import_costs_P2G2P_disagg_LP')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2Import[GENS,time] * self.model.H2ImportPriceP2G2PLP[GENS,time])
        return results
    def get_CH4imports(self, GENS):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CH4Import[GENS,time])
        return results
    def get_CH4imports_costs(self, GENS):
        if not hasattr(self.model, 'CH4ImportPriceP2G2PLP'):
            raise Exception('Missing call to get_import_costs_P2G2P_disagg_LP')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CH4Import[GENS,time] * self.model.CH4ImportPriceP2G2PLP[GENS,time])
        return results

    #get revenues from selling H2 and CH4
    def get_H2sell_revenue(self, GENS):
        if not hasattr(self.model, 'H2PriceP2G2PSellLP'):
            raise Exception('Missing call to get_H2_revenue_LP')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.H2ForMarket[GENS,time] * self.model.H2PriceP2G2PSellLP[GENS,time])
        return results
    def get_CH4sell_revenue(self, GENS):
        if not hasattr(self.model, 'CH4PriceP2G2PSellLP'):
            raise Exception('Missing call to get_CH4_revenue_LP')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CH4ForMarket[GENS,time] * self.model.CH4PriceP2G2PSellLP[GENS,time])
        return results
    def get_CO2store_revenue(self, GENS):
        if not hasattr(self.model, 'CO2PriceP2G2PSellLP'):
            raise Exception('Missing call to get_CO2_revenue_LP')
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value((self.model.CH4ForMarket[GENS,time] / 5.066) * self.model.CO2PriceP2G2PSellLP[GENS,time])
        return results