from __future__ import division
from pyomo.environ import *  # @UnusedWildImport
from pyomo.opt import SolverFactory,SolverStatus,TerminationCondition  # @Reimport

import logging
logging.getLogger('pyomo.core').setLevel(logging.ERROR)

import numpy as np

class SystemState:
    def __init__(self, num_generators, num_snaphots):
        self.num_generators = num_generators
        self.num_snaphots = num_snaphots
        self.solver = "gurobi"
        self.model = ConcreteModel()
        self.model.dual = Suffix(direction=Suffix.IMPORT)
        self.model.Generators = RangeSet(0, num_generators - 1)
        self.model.TimePeriods = RangeSet(0, num_snaphots - 1)
        self.results = None  # Store results from the solver, including status and termination condition.

        """
        variables
        """
        self.model.PowerConsumed = Var(
            self.model.Generators, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #power consumed by each generator at each time period
        self.model.PowerGenerated = Var(
            self.model.Generators, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #power produced by each generator at each time period

    """
    state
    """
    def power_consumed(self, gen, time):
        return self.model.PowerConsumed[gen, time]
    def power_generated(self, gen, time):
        return self.model.PowerGenerated[gen, time]

    def all_gen(self):
        return self.model.Generators
    def select_gen(self, l):
        return SetOf(l)

    """
    sets the objective function
    """
    def set_objective_function(self, fn):
        self.model.obj1 = Objective(expr=fn, sense=minimize)

    def set_objective_function_LP(self, fn):
        self.model.obj2 = Objective(expr=fn, sense=minimize)

    def deactivate_obj1(self):
        self.model.obj1.deactivate()
    
    def activate_obj2(self):
        self.model.obj2.activate()
    
    """
    sets cost functions
    """
    def get_operational_costs(self, GENS, operation_cost_coefficient, tpRes):
        m = self.model
        return (sum(operation_cost_coefficient[g] * m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes

    def get_operational_costs_disagg(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, tpRes, baseMVA):
        self.model.FuelPrice = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEff = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2Price = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2Rate = Param(
            GENS,
            within=Reals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOM = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        return (sum((m.FuelPrice[g,t] / m.FuelEff[g] + m.CO2Price[g,t] * m.CO2Rate[g] + m.NonFuelVOM[g]) *  m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)) * tpRes

    def get_operational_costs_disagg_LP(self, GENS, fuel_price, fuel_eff, co2_price, co2_rate, nonfuel_VOM, baseMVA):
        self.model.FuelPriceLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: fuel_price[g][t] * baseMVA)
        self.model.FuelEffLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: fuel_eff[g])
        self.model.CO2PriceLP = Param(
            GENS,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,g,t: co2_price[g][t] * baseMVA)
        self.model.CO2RateLP = Param(
            GENS,
            within=Reals,
            initialize=lambda m,g: co2_rate[g])
        self.model.NonFuelVOMLP = Param(
            GENS,
            within=NonNegativeReals,
            initialize=lambda m,g: nonfuel_VOM[g] * baseMVA)
        m = self.model
        return sum((m.FuelPriceLP[g,t] / m.FuelEffLP[g] + m.CO2PriceLP[g,t] * m.CO2RateLP[g] + m.NonFuelVOMLP[g]) *  m.PowerGenerated[g,t] for t in self.model.TimePeriods for g in GENS)

    """
    set production equals demand 
    """ 
    def set_demand(self, demand):
        self.model.Demand = Param(
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,t: demand[t])
 
        def production_constraint(m,t):
            result = sum(m.PowerGenerated[g,t] for g in m.Generators)
            if hasattr(self.model, 'PowerConsumed'):
                result -= sum(m.PowerConsumed[g,t] for g in m.Generators)
            return result == m.Demand[t]
        
        self.model.ProductionEqualsDemandCon = Constraint(
            self.model.TimePeriods,
            rule=production_constraint)
            
    """
    sets co2 limits
    """
    def set_co2limit(self, SWISSGENS, SWISSNETGENS, co2_rate, co2_target, tpRes, baseMVA):
        m = self.model
        self.model.CO2Limit = Constraint(
            rule=lambda m: sum(co2_rate[g] * m.PowerGenerated[g,t] * tpRes * baseMVA for t in self.model.TimePeriods for g in SWISSGENS) + sum(co2_rate[g] * m.PowerConsumed[g,t] * tpRes * baseMVA for t in self.model.TimePeriods for g in SWISSNETGENS) <= co2_target)             

    """
    solver routines
    """    
    def pprint(self):
        self.model.pprint()

    def solve(self, threads = 8):
        opt = SolverFactory(self.solver)
        opt.options["Threads"] = threads
        opt.options["BarConvTol"] = 1e-6
        opt.options["Method"] = 2
        opt.options["Aggregate"] = 0
        opt.options["Crossover"] = 0
        opt.options["BarHomogeneous"] = 1
        #opt.options["NumericFocus"] = 2
        opt.options['ResultFile'] = "test_p2g2p_2.ilp" #diagnoses infeasibilities
        results = opt.solve(self.model, symbolic_solver_labels=True, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status: %s" % results.solver.status)
        print("    Objective function Value = %s" % (value(self.model.obj2)))

    def solve_with_timeout(self, limit = 60):
        self.model.solutions.load_from(SolverFactory(self.solver).solve(self.model, timelimit=limit))        

    def solve_2050_tpRes8(self, gap = 0.015, threads = 8):
        opt = SolverFactory(self.solver)
        opt.options["Threads"] = threads
        opt.options["BarConvTol"] = 1e-10
        opt.options["Method"] = 2
        opt.options["Aggregate"] = 0
        #opt.options["Heuristics"] = 0
        opt.options["Crossover"] = 0
        results = opt.solve(self.model, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status: %s" % results.solver.status)
        print("    Objective function Value = %s" % (value(self.model.obj)))

    def solve_with_gap(self, gap = 0.025, threads = 8):
        opt = SolverFactory(self.solver)
        opt.options["MIPGap"] = gap
        opt.options["Threads"] = threads
        opt.options["BarConvTol"] = 1e-10
        opt.options["Method"] = 2
        opt.options["Aggregate"] = 0
        opt.options["Heuristics"] = 0
        opt.options["CrossoverBasis"] = 1
        opt.options["Crossover"] = 3
        opt.options["ScaleFlag"] = 2
        #opt.options["DegenMoves"] = 0
        results = opt.solve(self.model, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status: %s" % results.solver.status)
        print("    Objective function Value = %s" % (value(self.model.obj)))


    def solve_quadratic(self, gap = 0.0015, threads = 8):
        opt = SolverFactory(self.solver)
        opt.options["MIPGap"] = gap
        opt.options["Threads"] = threads
        opt.options["BarConvTol"] = 1e-10
        opt.options["Method"] = 2
        opt.options["Aggregate"] = 0
        opt.options["Crossover"] = 0
        opt.options["NonConvex"] = 2
        opt.options["BarHomogeneous"] = 1
        #opt.options["DegenMoves"] = 0
        #opt.options["Heuristics"] = 0
        results = opt.solve(self.model, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status: %s" % results.solver.status)
        print("    Objective function Value = %s" % (value(self.model.obj)))


    def solve_linear(self, threads = 8):
        opt = SolverFactory(self.solver)
        opt.options["Threads"] = threads
        opt.options["BarConvTol"] = 1e-4
        opt.options["Method"] = 2
        opt.options["Aggregate"] = 1 # Presolve: use moderate constraint aggregation
        opt.options["Crossover"] = 0
        opt.options["BarHomogeneous"] = -1 # Use the homogeneous self-dual variant or not. -1 is the default, allowing Gurobi to decide automatically.
        opt.options["NumericFocus"] = 1 # Slightly increased numerical robustness vs. default
        opt.options['ResultFile'] = "test_2018.ilp" #diagnoses infeasibilities

        # Add scaling parameters
        opt.options["ScaleFlag"] = 2      # Geometric scaling for constraint matrix
        opt.options["ObjScale"] = -1      # Scale objective by 10^-1

        results = opt.solve(self.model, symbolic_solver_labels=True, tee = True)
        self.model.solutions.load_from(results)
        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status: %s" % results.solver.status)
        print("    Objective function Value = %s" % (value(self.model.obj1)))


    def solve_linear_LP(self, threads = 8):
        opt = SolverFactory(self.solver)
        opt.options["Threads"] = threads
        opt.options["BarConvTol"] = 1e-4
        opt.options["Method"] = 2
        opt.options["Aggregate"] = 1 # Presolve: use moderate constraint aggregation
        opt.options["Crossover"] = 0
        opt.options["BarHomogeneous"] = - 1 # Use the homogeneous self-dual variant or not. -1 is the default, allowing Gurobi to decide automatically.
        opt.options["NumericFocus"] = 1 # Slightly increased numerical robustness vs. default
        opt.options['ResultFile'] = "test_2018_LP.ilp" #diagnoses infeasibilities
        # Solve the model using the specified solver and store the results object as a class attribute.
        # This new approach allows us to access solver status and termination condition later from self.results,
        # which is not possible if results is only a local variable. This also makes it easier to use solver
        # information in post-processing or error handling routines elsewhere in the class.
        # Note: self.results is accessible from any method of this SystemState instance (possibly named opt).
        self.results = opt.solve(self.model, symbolic_solver_labels=True, tee=True)        
        self.model.solutions.load_from(self.results)
        if (self.results.solver.status == SolverStatus.ok) and (self.results.solver.termination_condition == TerminationCondition.optimal):
            print("    Solver Status: %s" % self.results.solver.status)
        print("    Objective function Value = %s" % (value(self.model.obj2)))

    def write_lp(self):
        self.model.write('file_test.lp', io_options={'symbolic_solver_labels': True})
       
    def write_model_mst(self):
        self.model.write('out.mst') 

    """
    post-processing routines
    """ 
    def get_generator_power(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PowerGenerated[generator,time])
        return results

    def get_generator_power_consumed(self, generator):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.PowerConsumed[generator,time])
        return results

    #used for all non-p2g2p units after linear re-solve
    def get_hourly_gencost_LP(self, generator):
        if not hasattr(self.model, 'FuelPriceLP'):
            raise Exception('Missing call to get_operational_costs_disagg_LP')
        results = np.zeros(self.num_snaphots)    
        for time in range(self.num_snaphots):
            results[time] = value((self.model.FuelPriceLP[generator,time] / self.model.FuelEffLP[generator] + self.model.CO2PriceLP[generator,time] * self.model.CO2RateLP[generator] + self.model.NonFuelVOMLP[generator]) * self.model.PowerGenerated[generator,time])
        return results
    
    """
    get duals
    """ 
    def get_nodal_dual(self, nodes, baseMVA=None, single_electric_node=False):
        result = {n:[] for n in nodes}
        for c in self.model.component_objects(Constraint):
            cobject = getattr(self.model, str(c))
            if str(c) == "NodalConstraint":
                for index in cobject:
                    node = index[0]
                    if node in result:
                        if baseMVA:
                            result[node].append(self.model.dual[cobject[index]]/baseMVA)
                        else:
                            result[node].append(self.model.dual[cobject[index]])
        # if a key in  result has no value, remove it (this is useful, specially if single_electric_node is True and therefore normal CH nodes have no nodal balance)
        for key in list(result.keys()):
            if not result[key]:
                del result[key]
        
        
        if single_electric_node == True:
            result = {"CH00":[]}
            for c in self.model.component_objects(Constraint):
                cobject = getattr(self.model, str(c))
                if str(c) == "NodalConstraint_one_CH":
                    for index in cobject:
                        if baseMVA: # now I have hardcoded it to 1.0 if single_electric_node is True, so this line is not needed
                            baseMVA = 1.0  # now I have hardcoded it to 1.0 if single_electric_node is True, so this line is not needed
                            result["CH00"].append(self.model.dual[cobject[index]]/baseMVA)
                        else:
                            result["CH00"].append(self.model.dual[cobject[index]])
        return result
            
    def get_FRRup_dual(self, baseMVA=None):
        result = []
        for c in self.model.component_objects(Constraint):
            cobject = getattr(self.model, str(c))
            if str(c) == "UpReserveFRR":
                for index in cobject:
                    if baseMVA:
                        result.append(self.model.dual[cobject[index]]/baseMVA)
                    else:
                        result.append(self.model.dual[cobject[index]])
        return result
        
    def get_FRRdown_dual(self, baseMVA=None):
        result = []
        for c in self.model.component_objects(Constraint):
            cobject = getattr(self.model, str(c))
            if str(c) == "DownReserveFRR":
                for index in cobject:
                        if baseMVA:
                            result.append(self.model.dual[cobject[index]]/baseMVA)
                        else:
                            result.append(self.model.dual[cobject[index]])
        return result
    
    def get_RRup_dual(self, baseMVA=None):
        result = []
        for c in self.model.component_objects(Constraint):
            cobject = getattr(self.model, str(c))
            if str(c) == "UpReserveRR":
                for index in cobject:
                    if baseMVA:
                        result.append(self.model.dual[cobject[index]]/baseMVA)
                    else:
                        result.append(self.model.dual[cobject[index]])
        return result
    
    def get_RRdown_dual(self, baseMVA=None):
        result = []
        for c in self.model.component_objects(Constraint):
            cobject = getattr(self.model, str(c))
            if str(c) == "DownReserveRR":
                for index in cobject:
                    if baseMVA:
                        result.append(self.model.dual[cobject[index]]/baseMVA)
                    else:
                        result.append(self.model.dual[cobject[index]])
        return result
    
    def get_REStarget_dual(self, baseMVA=None):
        print("Dual variable for the RES target constraint")
        result = []
        if hasattr(self.model, "RESCon"):
            constraint = self.model.RESCon
        elif hasattr(self.model, "RESConDistIv"):
            constraint = self.model.RESConDistIv
        else:
            return result

        for index in constraint:
            if baseMVA:
                print("RES target dual ", self.model.dual[constraint[index]] / baseMVA)
                result.append(self.model.dual[constraint[index]] / baseMVA)
            else:
                print("RES target dual ", self.model.dual[constraint[index]])
                result.append(self.model.dual[constraint[index]])

        return result
        
    """
    print duals
    """
    def print_duals_reserves(self):
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "UpReserveFRR": #"NodalConstraint"
                for index in cobject:
                    print("    "), index, self.model.dual[cobject[index]]   #should change printing to be visible in python 3.x

    def print_duals_elprices_simple(self):
        for c in self.model.component_objects(Constraint):
            #print "    Constraint", c
            cobject = getattr(self.model, str(c))
            if str(c) == "ProductionEqualsDemandCon":
                for index in cobject:
                    print("    "), index, self.model.dual[cobject[index]]   #should change printing to be visible in python 3.x

    def print_duals_net_winter_import(self, baseMVA=None):
        print("Dual variable for the net winter import constraint")
        constraint = self.model.EqualWinterExportImportCon
        for index in constraint:
            if baseMVA:
                print("EqualWinterExportImportCon ", -self.model.dual[constraint[index]] / baseMVA)
            else:
                print("EqualWinterExportImportCon ", -self.model.dual[constraint[index]])
