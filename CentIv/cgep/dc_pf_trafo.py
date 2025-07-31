from pyomo.environ import *
import numpy as np

class DC_PowerFlow_Trafo(object):
    def __init__(self, state, num_nodes, num_lines):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.num_nodes = num_nodes
        self.num_lines = num_lines
        self.model.Buses = RangeSet(0, num_nodes - 1)
        self.model.Lines = RangeSet(0, num_lines - 1)
        
        """
        optimization variables
        """
        self.model.V_angle = Var(
            self.model.Buses, 
            self.model.TimePeriods, 
            within=Reals) #voltage angle at each bus 
        self.model.ActivePower = Var(
            self.model.Lines, 
            self.model.TimePeriods, 
            within=Reals) #active power flow through each line
        self.model.LostLoad = Var(
            self.model.Buses, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #load shedding at each bus #NonNegativeReals
        
    def set_dc_bus(self, bus_demand, slack_bus, baseMVA):
        #voltage angles at any bus are limited to +/-45deg
        #voltage angle at slack bus is 0 deg 
        self.model.Demand = Param(
            self.model.Buses,
            self.model.TimePeriods,
            #within=NonNegativeReals,
            initialize=lambda m,b,t: bus_demand[b][t]/baseMVA) #demand is only positive
        self.model.V_angleCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: -np.pi/4 <= m.V_angle[b,t] <= np.pi/4) #From "Transmission Expansion Planning: A mixed-integer approach" --> max angle diff between sending and receiving node is 20 deg (pi/9)
        self.model.V_angle_0 = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: m.V_angle[slack_bus,t] == 0)
        self.model.DemandVar0Con = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LostLoad[b,t] <= m.Demand[b,t]) #m.LostLoad[b,t] <= m.Demand[b,t] Constraint.Skip if (m.Demand[b,t] <= 0.0) else m.LostLoad[b,t] <= m.Demand[b,t]
     
    def set_dc_bus_distIv_injection(self, bus_demand, slack_bus, baseMVA):
        #voltage angles at any bus are limited to +/-45deg
        #voltage angle at slack bus is 0 deg 
        self.model.Demand = Param(
            self.model.Buses,
            self.model.TimePeriods,
            #within=NonNegativeReals,
            initialize=lambda m,b,t: bus_demand[b][t]/baseMVA) #demand can be negative depending on the inputs from DistIv
        self.model.V_angleCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: -np.pi/4 <= m.V_angle[b,t] <= np.pi/4) #From "Transmission Expansion Planning: A mixed-integer approach" --> max angle diff between sending and receiving node is 20 deg (pi/9)
        self.model.V_angle_0 = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: m.V_angle[slack_bus,t] == 0)
        self.model.DemandVar0Con = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LostLoad[b,t] <= max(0, m.Demand[b,t])) #Constraint.Skip if (m.Demand[b,t] <= 0.0) else m.LostLoad[b,t] <= m.Demand[b,t]
        self.model.CurtaildistIvInj = Var(
            self.model.Buses, 
            self.model.TimePeriods, 
            within=Reals) #curtailment of RES from distIv at each bus
        self.model.CurtailmentCon1 = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.CurtaildistIvInj[b,t] <= 0)#m.CurtaildistIvInj[b,t] == 0 if (m.Demand[b,t] >= 0.0) else m.CurtaildistIvInj[b,t] >= m.Demand[b,t])
        self.model.CurtailmentCon2 = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.CurtaildistIvInj[b,t] >= min(0.0,m.Demand[b,t]))#m.CurtaildistIvInj[b,t] == 0 if (m.Demand[b,t] >= 0.0) else m.CurtaildistIvInj[b,t] >= m.Demand[b,t])
        
    def set_dc_line(self, reactance, tap_ratio, line_limit): 
        #reactance is the x value from mpc.branch struct
        #line limit is rateA (apparent power) value from mpc.branch struct
        self.model.B = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: 1/(reactance[l] * tap_ratio[l])) # 1/reactance is series susceptance
        self.model.Limit = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: line_limit[l]) #line thermal rating 
        self.model.ActivePowerCon = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: -m.Limit[l] <= m.ActivePower[l,t] <= m.Limit[l])
            
    def connect_buses(self, line_start, line_end, gen_nodes):
        #this function sets the constraint for Nodal Balance so we use it instead of set_demand when incorporating the DC constraints
        node_start_lines = [[] for _ in range(self.num_nodes)]
        node_end_lines = [[] for _ in range(self.num_nodes)]
        gens_at_node = [[] for _ in range(self.num_nodes)]
        for l in range(self.num_lines):
            node_start_lines[line_start[l]].append(l)
            node_end_lines[line_end[l]].append(l)
        for gen in range(self.state.num_generators):
            gens_at_node[gen_nodes[gen]].append(gen)     
        
        self.model.PowerFlowCon = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.ActivePower[l,t] == m.B[l]*(m.V_angle[line_start[l],t] - m.V_angle[line_end[l],t]))
       
        self.model.NodalConstraint = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,n,t: - sum(self.model.ActivePower[l,t] for l in node_start_lines[n]) +
                sum(self.model.ActivePower[l,t] for l in node_end_lines[n])                    -
                self.model.Demand[n,t]                                                         -
                sum(m.PowerConsumed[gen,t] for gen in gens_at_node[n])                         +
                self.model.LostLoad[n,t]                                                       +
                sum(m.PowerGenerated[gen,t] for gen in gens_at_node[n]) == 0)

    def connect_buses_distIv_injection(self, line_start, line_end, gen_nodes):
        #this constraint is identical to connect_buses BUT also includes the capability to curtail injections from DistIv/ABM
        node_start_lines = [[] for _ in range(self.num_nodes)]
        node_end_lines = [[] for _ in range(self.num_nodes)]
        gens_at_node = [[] for _ in range(self.num_nodes)]
        for l in range(self.num_lines):
            node_start_lines[line_start[l]].append(l)
            node_end_lines[line_end[l]].append(l)
        for gen in range(self.state.num_generators):
            gens_at_node[gen_nodes[gen]].append(gen)     
        
        self.model.PowerFlowCon = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.ActivePower[l,t] == m.B[l]*(m.V_angle[line_start[l],t] - m.V_angle[line_end[l],t]))
       
        self.model.NodalConstraint = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,n,t: - sum(self.model.ActivePower[l,t] for l in node_start_lines[n]) +
                sum(self.model.ActivePower[l,t] for l in node_end_lines[n]) -
                self.model.Demand[n,t] -
                sum(m.PowerConsumed[gen,t] for gen in gens_at_node[n]) +  #positive sign because CurtaildistIvInj is negative by definition
                self.model.CurtaildistIvInj[n,t] +
                self.model.LostLoad[n,t] +
                sum(m.PowerGenerated[gen,t] for gen in gens_at_node[n]) == 0)
    
    def get_lossload_cost(self, ll_cost, tpRes):
        m = self.model
        return (sum(ll_cost * m.LostLoad[n,t] for t in m.TimePeriods for n in m.Buses)) * tpRes #cost term to be added to the objective function
    
    def get_cost_distIv_injection(self, cost_injection, tpRes):
        m = self.model
        return (sum(cost_injection*(-(self.model.Demand[n,t]-self.model.CurtaildistIvInj[n,t])) for t in m.TimePeriods for n in m.Buses if (m.Demand[n,t] < 0.0))) * tpRes

    def set_equal_annual_exportimport(self, BORDERLINES, tpRes):
        self.model.EqualAnnualExportImportCon = Constraint(
            rule=lambda m: sum(m.ActivePower[l,t] for l in BORDERLINES for t in m.TimePeriods) == 0)
    
    """
    Generates an expression for the costs of all transmission lines and trasformers in the system
    -- returns 0
    -- this function is only relevant in cases with expansion
    """
    def get_investment_cost_trafoline(self, investment_cost_trafo, investment_cost_line, length, MVA_rating, ids_trafo_cand, ids_line_cand):
        m = self.model
        return 0  #no investment costs added to the objective f-n

    """
    post-processing routines
    """     
    def get_voltage_angle(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.V_angle[bus,time])
        return results
    
    def get_branch_flows(self, line):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActivePower[line,time])
        return results
    
    def get_trafo_flows(self, line):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActivePowerTrafos[line,time])
        return results
    
    def get_distIv_curtailment(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.CurtaildistIvInj[bus,time])
        return results
    
    def get_load_shedding(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.LostLoad[bus,time])
        return results