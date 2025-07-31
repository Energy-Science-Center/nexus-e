from __future__ import division
from pyomo.environ import *  # @UnusedWildImport

import numpy as np

class HydroGenerators:
    def __init__(self, state, gens):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.generators = gens
        self.model.Months = RangeSet(0, 11) #for SoC validation constraint
        
        """
        variables
        """
        self.model.Spill = Var(
            self.generators,
            self.model.TimePeriods,
            within=NonNegativeReals) #Spill for a generic energy storage [MWh]
        self.model.SoC = Var(
            self.generators, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #reservoir level of hydro dam/pump power plant [MWh]
        
    def gens(self):
        return self.generators

    def select_gen(self, l):
        return SetOf(l)

    """
    sets operational constraints for hydro dams
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum turbine power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh]
        Inflows [Capacity Factor]
        Turbine efficiency
    spillage is allowed 
    """
    def set_hydro_power_dam(self, GENS, min_energy_capacity_dam, max_energy_capacity_dam, max_power_turbine_dam, min_power_turbine_dam, capacityT0_dam, hourly_inflows_dam, eff_turbine, tpResolution):
        self.model.MinEnergyCapacityHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_dam[g])
        self.model.MaxEnergyCapacityHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_dam[g])
        self.model.MaxPowerTurbineHydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_dam[g])  
        self.model.CapacityT0HydroDam = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_dam[g] * m.MaxEnergyCapacityHydroDam[g]) 
        self.model.HourlyInflowsDam = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: hourly_inflows_dam[g][t])
        
        self.model.MaxPowerGeneratedHydroDamCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroDam[g])
        self.model.PowerConsumedHydroDamCon0 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0) 
        self.model.MinMaxEnergyStoredHydroDamCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityHydroDam[g] <= m.SoC[g,t] and m.SoC[g,t] <= m.MaxEnergyCapacityHydroDam[g])
        self.model.SoCHydroDamCon = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0HydroDam[g] if t == 0 else m.SoC[g,t-1]) - tpResolution * m.PowerGenerated[g,t]/eff_turbine[g] + tpResolution * m.MaxPowerTurbineHydroDam[g] * m.HourlyInflowsDam[g,t] - m.Spill[g,t]) #should multiply this constraint by the time step to get energy (in this case our timestep is hour so everything works out)
        self.model.SpillDamCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.Spill[g,t] <= m.MaxPowerTurbineHydroDam[g])
        self.model.EqualEnergyDamCon = Constraint(
            GENS,
            rule=lambda m,g: m.SoC[g,self.num_snaphots-1] >= (m.CapacityT0HydroDam[g]))

      
    """
    sets operational constraints for pumped hydro power plants
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum turbine/pump power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh]
        Inflows [Capacity Factor]
        Pump efficiency
        Turbine efficiency
    spillage is allowed 
    """
    def set_hydro_power_Pumped(self, GENS, min_energy_capacity_hydro, max_energy_capacity_hydro, max_power_turbine_hydro, max_power_pump_hydro, capacityT0_hydro, hourly_inflows_hydro, eff_pump, eff_turbine, tpResolution):
        self.model.MinEnergyCapacityHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_hydro[g])
        self.model.MaxEnergyCapacityHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_hydro[g])
        self.model.MaxPowerTurbineHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_hydro[g])
        self.model.MaxPowerPumpHydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_pump_hydro[g])        
        self.model.CapacityT0Hydro = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_hydro[g] * m.MaxEnergyCapacityHydro[g]) 
        self.model.HourlyInflows = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: hourly_inflows_hydro[g][t])
        
        self.model.MaxPowerGeneratedHydroCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydro[g])
        self.model.MaxPowerStoredHydroCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerPumpHydro[g])
        self.model.MinMaxEnergyStoredHydroCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityHydro[g] <= m.SoC[g,t] and m.SoC[g,t] <= m.MaxEnergyCapacityHydro[g])
        self.model.SoCHydroCon1 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0Hydro[g] if t == 0 else m.SoC[g,t-1]) - tpResolution * m.PowerGenerated[g,t]/eff_turbine[g] + tpResolution * m.PowerConsumed[g,t]*eff_pump[g] + tpResolution * m.MaxPowerTurbineHydro[g] * m.HourlyInflows[g,t] - m.Spill[g,t]) #should multiply this constraint by the time step to get energy (in this case our timestep is hour so everything works out)        
        self.model.SpillPumpCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.Spill[g,t] <= m.MaxPowerTurbineHydro[g])
        self.model.EqualEnergyCon = Constraint(
            GENS,
            rule=lambda m,g: m.SoC[g,self.num_snaphots-1] >= (m.CapacityT0Hydro[g]))
    
    """
    sets operational constraints for pumped hydro power plants which operate on a daily cycle
    inputs are: 
        Minimum/Maximum storage capacity for energy storage unit [MWh]
        Maximum turbine/pump power for energy storage unit [MW]
        Initial capacity of storage unit at T0 [MWh]
        Inflows [Capacity Factor]
        Pump efficiency
        Turbine efficiency
    spillage is allowed 
    """
    def set_hydro_power_Pumped_daily(self, GENS, min_energy_capacity_hydro_day, max_energy_capacity_hydro_day, max_power_turbine_hydro_day, max_power_pump_hydro_day, capacityT0_hydro_day, hourly_inflows_hydro_day, eff_pump_day, eff_turbine_day, tpResolution):
        self.model.MinEnergyCapacityHydroDay = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_energy_capacity_hydro_day[g])
        self.model.MaxEnergyCapacityHydroDay = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_energy_capacity_hydro_day[g])
        self.model.MaxPowerTurbineHydroDay = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_hydro_day[g])
        self.model.MaxPowerPumpHydroDay = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_pump_hydro_day[g])        
        self.model.CapacityT0HydroDay = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: capacityT0_hydro_day[g] * m.MaxEnergyCapacityHydroDay[g] * tpResolution) 
        self.model.HourlyInflowsDay = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: hourly_inflows_hydro_day[g][t])
        
        self.model.MaxPowerGeneratedHydroDayCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroDay[g])
        self.model.MaxPowerStoredHydroDayCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerPumpHydroDay[g])
        self.model.MinMaxEnergyStoredHydroDayCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.MinEnergyCapacityHydroDay[g] * tpResolution <= m.SoC[g,t] and m.SoC[g,t] <= m.MaxEnergyCapacityHydroDay[g] * tpResolution)
        self.model.SoCHydroDayCon1 = Constraint(
            GENS, 
            self.model.TimePeriods,
            rule=lambda m,g,t: m.SoC[g,t] == (m.CapacityT0HydroDay[g] if t == 0 else m.SoC[g,t-1]) - tpResolution * m.PowerGenerated[g,t] / eff_turbine_day[g] + tpResolution * m.PowerConsumed[g,t] * eff_pump_day[g] + tpResolution * m.MaxPowerTurbineHydroDay[g] * m.HourlyInflowsDay[g,t] - m.Spill[g,t]) #should multiply this constraint by the time step to get energy (in this case our timestep is hour so everything works out)        
        self.model.SpillPumpDayCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.Spill[g,t] <= m.MaxPowerTurbineHydroDay[g])
      
    """
    sets reserve constraints for hydro power dams 
    """   
    def set_hydro_power_dam_reserves(self, reserves, GENS, tpResolution):
        if not hasattr(self.model, 'MaxPowerTurbineHydroDam'):
            raise Exception('Missing call to set_hydro_power_dam')

        #up reserves
        self.model.ActualUpFRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #power limit with up reserves
        self.model.RUpTurbineLimitDam = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] + (m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t]) <= m.MaxPowerTurbineHydroDam[g]) #this means the dam can provide upward reserve if it's at 0 (off)
        self.model.RUpTurbineLimitDam1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t] <= m.MaxPowerTurbineHydroDam[g])
        #soc limit with up reserves
        self.model.reserve_up_constraint_with_SoC_dam_1 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t])*tpResolution <=
                                  m.SoC[g, t] - m.MinEnergyCapacityHydroDam[g]))
        self.model.reserve_up_constraint_with_SoC_dam_2 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualUpFRReserveDam[g,t] + m.ActualUpRRReserveDam[g,t])*tpResolution <=
                                  ((m.SoC[g, t - 1] - m.MinEnergyCapacityHydroDam[g]) if t > 0
                                   else (m.CapacityT0HydroDam[g] - m.MinEnergyCapacityHydroDam[g]))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReserveDam[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReserveDam[g,t]

        #down reserves
        self.model.ActualDownFRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReserveDam = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)

        #power limit with down reserves
        self.model.RDownTurbineLimitDam = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: 0 <= m.PowerGenerated[g,t] - (m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t]) <= m.MaxPowerTurbineHydroDam[g])
        self.model.RDownTurbineLimitDam2 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t] <= m.MaxPowerTurbineHydroDam[g])
        #soc limit with down reserves
        self.model.reserve_down_constraint_with_SoC_dam_1 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t])*tpResolution <=
                                  - m.SoC[g, t] + m.MaxEnergyCapacityHydroDam[g]))
        self.model.reserve_down_constraint_with_SoC_dam_2 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualDownFRReserveDam[g,t] + m.ActualDownRRReserveDam[g,t])*tpResolution <=
                                  ((-m.SoC[g, t - 1] + m.MaxEnergyCapacityHydroDam[g]) if t > 0
                                   else (- m.CapacityT0HydroDam[g] + m.MaxEnergyCapacityHydroDam[g]))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReserveDam[g,t]
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReserveDam[g,t]       
                
    """
    sets reserve constraints for pumped hydro
    """   
    def set_hydro_power_Pumped_reserves(self, reserves, GENS, tpResolution):
        if not hasattr(self.model, 'MaxPowerTurbineHydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')
        if not hasattr(self.model, 'MaxPowerPumpHydro'):
            raise Exception('Missing call to set_hydro_power_Pumped')

        #up reserves
        self.model.ActualUpFRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #power limit with up reserves
        self.model.RUpTurbineLimitPumped = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t] <= m.MaxPowerTurbineHydro[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t]) #this means the pumped hydro power plant can provide upward reserve if it's at 0 (off)
        self.model.RUpTurbineLimitPumped1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t] <= m.MaxPowerTurbineHydro[g] + m.MaxPowerPumpHydro[g]) #this is a non binding constraint 
        # soc limit with up reserves
        self.model.reserve_up_constraint_with_SoC_pump_1 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t])*tpResolution <=
                                  m.SoC[g, t] - m.MinEnergyCapacityHydro[g]))
        self.model.reserve_up_constraint_with_SoC_pump_2 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualUpFRReservePumped[g,t] + m.ActualUpRRReservePumped[g,t])*tpResolution <=
                                  ((m.SoC[g, t - 1] - m.MinEnergyCapacityHydro[g]) if t > 0
                                  else (m.CapacityT0Hydro[g] - m.MinEnergyCapacityHydro[g]))))

        # link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReservePumped[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReservePumped[g,t]

        #down reserves
        self.model.ActualDownFRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReservePumped = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #power limit with down reserves
        self.model.RDownPumpLimit = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:  m.PowerConsumed[g,t] - m.PowerGenerated[g,t] + (m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t]) <= m.MaxPowerPumpHydro[g])
        self.model.RDownPumpLimit1 = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t] <= m.MaxPowerPumpHydro[g] + m.MaxPowerTurbineHydro[g]) #this is a non binding constraint 
        # soc limit with down reserves
        self.model.reserve_down_constraint_with_SoC_pump_1 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t])*tpResolution <=
                                  - m.SoC[g, t] + m.MaxEnergyCapacityHydro[g]))
        self.model.reserve_down_constraint_with_SoC_pump_2 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualDownFRReservePumped[g,t] + m.ActualDownRRReservePumped[g,t])*tpResolution <=
                                  ((-m.SoC[g, t - 1] + m.MaxEnergyCapacityHydro[g]) if t > 0
                                  else (- m.CapacityT0Hydro[g] + m.MaxEnergyCapacityHydro[g]))))

        # link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReservePumped[g,t]   
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReservePumped[g,t]
                
    """
    sets reserve constraints for daily pumped hydro
    """   
    def set_hydro_power_Pumped_daily_reserves(self, reserves, GENS, tpResolution):
        if not hasattr(self.model, 'MaxPowerTurbineHydroDay'):
            raise Exception('Missing call to set_hydro_power_Pumped_daily')
        if not hasattr(self.model, 'MaxPowerPumpHydroDay'):
            raise Exception('Missing call to set_hydro_power_Pumped_daily')

        #up reserves
        self.model.ActualUpFRReservePumpedDaily = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualUpRRReservePumpedDaily = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #power limit with up reserves
        self.model.RUpTurbineLimitPumpedDaily = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReservePumpedDaily[g,t] + m.ActualUpRRReservePumpedDaily[g,t] <= m.MaxPowerTurbineHydroDay[g] - m.PowerGenerated[g,t] + m.PowerConsumed[g,t]) #this means the daily pumped hydro power plant can provide upward reserve if it's at 0 (off)
        self.model.RUpTurbineLimitPumped1Daily = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualUpFRReservePumpedDaily[g,t] + m.ActualUpRRReservePumpedDaily[g,t] <= m.MaxPowerTurbineHydroDay[g] + m.MaxPowerPumpHydroDay[g]) #this is a non binding constraint 
        #soc limit with up reserves
        self.model.reserve_up_constraint_with_SoC_pump_daily_1 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualUpFRReservePumpedDaily[g,t] + m.ActualUpRRReservePumpedDaily[g,t])*tpResolution <=
                                  m.SoC[g, t] - m.MinEnergyCapacityHydroDay[g] * tpResolution))
        self.model.reserve_up_constraint_with_SoC_pump_daily_2 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: ((m.ActualUpFRReservePumpedDaily[g,t] + m.ActualUpRRReservePumpedDaily[g,t])*tpResolution <=
                                  ((m.SoC[g, t - 1] - m.MinEnergyCapacityHydroDay[g] * tpResolution) if t > 0
                                  else (m.CapacityT0HydroDay[g] - m.MinEnergyCapacityHydroDay[g] * tpResolution))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.up_FRR_reserves_at[t][g] = self.model.ActualUpFRReservePumpedDaily[g,t]
                reserves.up_RR_reserves_at[t][g] = self.model.ActualUpRRReservePumpedDaily[g,t]

        #down reserves
        self.model.ActualDownFRReservePumpedDaily = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        self.model.ActualDownRRReservePumpedDaily = Var(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals)
        
        #power limit with down reserves
        self.model.RDownPumpLimitDaily = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t:  m.PowerConsumed[g,t] - m.PowerGenerated[g,t] + (m.ActualDownFRReservePumpedDaily[g,t] + m.ActualDownRRReservePumpedDaily[g,t]) <= m.MaxPowerPumpHydroDay[g])
        self.model.RDownPumpLimit1Daily = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.ActualDownFRReservePumpedDaily[g,t] + m.ActualDownRRReservePumpedDaily[g,t] <= m.MaxPowerPumpHydroDay[g] + m.MaxPowerTurbineHydroDay[g]) #this is a non binding constraint 
        #soc limit with down reserves
        self.model.reserve_down_constraint_with_SoC_pump_daily_1 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: (m.ActualDownFRReservePumpedDaily[g,t] + m.ActualDownRRReservePumpedDaily[g,t] <=
                                  - m.SoC[g, t] + m.MaxEnergyCapacityHydroDay[g] * tpResolution))
        self.model.reserve_down_constraint_with_SoC_pump_daily_2 = Constraint(
            self.model.TimePeriods,
            GENS,
            rule=lambda m, t, g: (m.ActualDownFRReservePumpedDaily[g,t] + m.ActualDownRRReservePumpedDaily[g,t] <=
                                  ((-m.SoC[g, t - 1] + m.MaxEnergyCapacityHydroDay[g] * tpResolution) if t > 0
                                  else (- m.CapacityT0HydroDay[g] + m.MaxEnergyCapacityHydroDay[g] * tpResolution))))

        #link to reserves in reserves.
        for t in self.model.TimePeriods:
            for g in GENS:
                reserves.down_FRR_reserves_at[t][g] = self.model.ActualDownFRReservePumpedDaily[g,t]   
                reserves.down_RR_reserves_at[t][g] = self.model.ActualDownRRReservePumpedDaily[g,t]
     
    """
    sets operational constraints for hydro dams for water dcpf
    cannot be used in combination with set_hydro_power_dam_reserves
    """
    def set_hydro_power_dam_water_dcpf(self, GENS, max_power_turbine_dam, min_power_turbine_dam):
        self.model.MaxPowerTurbineHydroDamWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_dam[g])  
        self.model.MinPowerTurbineHydroDamWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: min_power_turbine_dam[g])        
        self.model.PowerConsumedHydroDamWaterDCPFCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] == 0) 
        self.model.MaxPowerGeneratedHydroDamWaterDCPFCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroDamWaterDCPF[g])  
    
    """
    sets operational constraints for pumped hydro power plants for water dcpf
    cannot be used in combination with set_hydro_power_Pumped_reserves
    """
    def set_hydro_power_Pumped_water_dcpf(self, GENS, max_power_turbine_hydro, max_power_pump_hydro):
        self.model.MaxPowerTurbineHydroWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_turbine_hydro[g])
        self.model.MaxPowerPumpHydroWaterDCPF = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: max_power_pump_hydro[g])        
        
        self.model.MaxPowerGeneratedHydroWaterDCPFCon = Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerGenerated[g,t] <= m.MaxPowerTurbineHydroWaterDCPF[g])  
        self.model.MaxPowerStoredHydroWaterDCPFCon =  Constraint(
            GENS,
            self.model.TimePeriods,
            rule=lambda m,g,t: m.PowerConsumed[g,t] <= m.MaxPowerPumpHydroWaterDCPF[g])      
     
    """
    set monthly reservoir levels (@ end of each month) for validation purposes
    """ 
    #only over Swiss hydro GENS (dam + pump incl. daily pump)
    def set_SoC_validation(self, GENS, end_hour, monthly_level):
        self.model.EndHourOfMonth = Param(
            self.model.Months,
            within=NonNegativeReals,
            initialize=lambda m,f: end_hour[f])
        self.model.MonthlyLevel = Param(
            self.model.Months,
            within=NonNegativeReals,
            initialize=lambda m,f: monthly_level[f])
        self.model.MonthlySoCCon = Constraint(
            self.model.Months,
            rule=lambda m,f: sum(m.SoC[g,m.EndHourOfMonth[f]] for g in GENS) == m.MonthlyLevel[f])

    """
    set minimum pumping from pumps - only for validation
    """
    def set_min_pumping(self, GENS, tpRes):
        self.model.PumpConsumptionCon = Constraint(
            rule=lambda m: sum(m.PowerConsumed[g,t] * tpRes for g in GENS for t in self.model.TimePeriods) >= 30000)


    """
    set objective function penalty for validation purposes 
    """ 
    def set_hydro_storage_incentive(self, GENS, tpResolution, eta):
        m = self.model
        return (sum(m.SoC[g,t] * tpResolution for g in GENS for t in self.model.TimePeriods)) * eta

    """
    post-processing routines
    """
    #get reservoir levels 
    def get_battery_state(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.SoC[generator,time])
        return results
    
    #get spill
    def get_spill(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.Spill[generator,time])
        return results
    
    #get reserve contributions of dam and pumped hydro (including daiy cycle)
    def frr_up_hydro_dam(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReserveDam[generator,time])
        return results
    
    def frr_down_hydro_dam(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReserveDam[generator,time])
        return results
            
    def rr_up_hydro_dam(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReserveDam[generator,time])
        return results  
    
    def rr_down_hydro_dam(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReserveDam[generator,time])
        return results  
    
    def frr_up_hydro_pumped(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReservePumped[generator,time])
        return results
    
    def frr_down_hydro_pumped(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReservePumped[generator,time])
        return results
            
    def rr_up_hydro_pumped(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReservePumped[generator,time])
        return results  
    
    def rr_down_hydro_pumped(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReservePumped[generator,time])
        return results  
    
    def frr_up_hydro_pumped_daily(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpFRReservePumpedDaily[generator,time])
        return results
    
    def frr_down_hydro_pumped_daily(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownFRReservePumpedDaily[generator,time])
        return results
            
    def rr_up_hydro_pumped_daily(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualUpRRReservePumpedDaily[generator,time])
        return results  
    
    def rr_down_hydro_pumped_daily(self,generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActualDownRRReservePumpedDaily[generator,time])
        return results