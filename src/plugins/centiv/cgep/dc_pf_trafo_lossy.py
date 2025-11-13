from pyomo.environ import *
import numpy as np

class DC_PowerFlow_Trafo_Lossy(object):
    def __init__(self, state, num_nodes, num_lines):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.num_nodes = num_nodes
        self.num_lines = num_lines

        self.model.Buses = RangeSet(0, num_nodes - 1)
        self.model.Lines = RangeSet(0, num_lines - 1)
        
        #initialize breakpoints for piecewise linear active power loss function
        PIECEWISE_NUM_POINTS = 10 #from "Improved Transmission Representations in Oligopolistic Market Models: Quadratic Losses, Phase Shifters and DC Lines" --> in plexos 10 segments are used 
        self.break_interval = 0.5 * np.pi / (PIECEWISE_NUM_POINTS)
        self.model.Breakpoints = RangeSet(0, PIECEWISE_NUM_POINTS - 1)
        
        """
        optimization Variables
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
            within=NonNegativeReals) #loss of load at each bus
        
        """
        additional variables for active power losses
        """
        self.model.V_angle_dif_pos = Var(
            self.model.Lines, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #used for linearization of abs(V_angle[line_start[l],t] - m.V_angle[line_end[l],t])
        self.model.V_angle_dif_neg = Var(
            self.model.Lines, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #used for linearization of abs(V_angle[line_start[l],t] - m.V_angle[line_end[l],t])
        self.model.ActivePowerLosses = Var(
            self.model.Lines, 
            self.model.TimePeriods, 
            within=NonNegativeReals) #active power loss for each line

    def define_square_loss(self):
        self.model.Deltas = Var(
            self.model.Lines,
            self.model.TimePeriods,
            self.model.Breakpoints,
            bounds=(0, self.break_interval))
        self.model.Alpha = Param(
            self.model.TimePeriods,
            self.model.Breakpoints,
            initialize=lambda m,t,b: (2*b + 1)*self.break_interval) #this is the slope of each angle block

    def set_v_angle_diff(self, line_start, line_end):
        #To avoid the use of the absolute value function abs(V_angle[line_start[l],t] - m.V_angle[line_end[l],t]) 
        #take the difference of two positive variables to represent a variable not restricted in sign
        self.model.V_angleDifCon1 = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.V_angle_dif_pos[l,t] + m.V_angle_dif_neg[l,t] == sum(self.model.Deltas[l,t,b] for b in self.model.Breakpoints))
        self.model.V_angleDifCon2 = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.V_angle[line_start[l],t] - m.V_angle[line_end[l],t] == m.V_angle_dif_pos[l,t] - m.V_angle_dif_neg[l,t])

    def set_dc_bus(self, bus_demand, slack_bus, baseMVA):
        #voltage angles at any bus are limited to +/-45deg
        #voltage angle at slack bus is 0 deg 
        self.model.Demand = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: bus_demand[b][t]/baseMVA) 
        self.model.V_angleCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: -np.pi/18 <= m.V_angle[b,t] <= np.pi/18) #From "Transmission Expansion Planning: A mixed-integer approach" --> max angle diff between sending and receiving node is 20 deg
        self.model.V_angle_0 = Constraint(
            self.model.TimePeriods,
            rule=lambda m,t: m.V_angle[slack_bus,t] == 0)
        self.model.DemandVarCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LostLoad[b,t] <= m.Demand[b,t]) #"no demand buses" can't shed load
        
    def set_dc_line(self, reactance, tap_ratio, line_limit, resistance): 
        #reactance is the x value from mpc.branch struct
        #line limit is rateA (apparent power) value from mpc.branch struct
        self.model.B = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: round(reactance[l]/((resistance[l]*resistance[l] + reactance[l]*reactance[l]) * tap_ratio[l]),2))
        self.model.G = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: round(resistance[l]/((resistance[l]*resistance[l] + reactance[l]*reactance[l]) * tap_ratio[l]) if resistance[l] != 0 else 0,2)) # 1/resistance is line conductance
        self.model.Limit = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: line_limit[l]) #line thermal rating 
        self.model.Resistance = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: resistance[l])
        self.model.LineLimitsCon1 = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.ActivePowerLosses[l,t] <= m.Limit[l])
        self.model.LineLimitsCon2 = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: -m.Limit[l] <= m.ActivePower[l,t] <= m.Limit[l])
        self.model.LineLimitsCon3 = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.ActivePower[l,t] + 0.5*m.ActivePowerLosses[l,t] <= m.Limit[l])
        self.model.LineLimitsCon4 = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: -m.ActivePower[l,t] + 0.5*m.ActivePowerLosses[l,t] <= m.Limit[l])
        #self.model.ActivePowerCon0 = Constraint(
        #    self.model.Lines,
        #    self.model.TimePeriods,
        #    rule=lambda m,l,t: 0.5 * m.ActivePowerLosses[l,t] + m.B[l] * (m.V_angle_dif_pos[l,t] + m.V_angle_dif_neg[l,t]) <= m.Limit[l])

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

        self.model.ActivePowerLossesCon = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.ActivePowerLosses[l,t] == (self.model.G[l] * sum(self.model.Deltas[l,t,b]*self.model.Alpha[t,b] for b in self.model.Breakpoints) if self.model.Resistance[l] != 0 else 0))

        self.model.ActivePowerCon = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: m.ActivePower[l,t] == self.model.B[l] * (m.V_angle_dif_pos[l,t] - m.V_angle_dif_neg[l,t]))
        
        self.model.NodalConstraint = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,n,t: - sum(self.model.ActivePower[l,t] for l in node_start_lines[n]) +
                sum(self.model.ActivePower[l,t] for l in node_end_lines[n]) -
                self.model.Demand[n,t] -
                sum(m.PowerConsumed[gen,t] for gen in gens_at_node[n]) +
                self.model.LostLoad[n,t] +
                sum(m.PowerGenerated[gen,t] for gen in gens_at_node[n]) -
                0.5 * sum(m.ActivePowerLosses[l,t] for l in node_start_lines[n]) == 0)

    def get_lossload_cost(self, ll_cost):
        m = self.model
        return sum(ll_cost * m.LostLoad[n,t] for t in m.TimePeriods for n in m.Buses) #cost term to be added to the objective function
    
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
    
    def get_branch_losses(self, line):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.ActivePowerLosses[line,time])
        return results