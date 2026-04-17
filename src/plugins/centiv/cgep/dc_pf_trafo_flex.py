from pyomo.environ import *
import numpy as np
import math 

class DC_PowerFlow_TrafoFlex(object):
    """
    Class for DC power flow model with flexible load shifting capabilities.
    """
    
    def __init__(self, state, num_nodes, lines, no_load_shift_buses):
        num_snaphots = state.num_snaphots
        self.num_snaphots = num_snaphots
        self.state = state
        self.model = state.model
        self.num_nodes = num_nodes
        self.num_lines = len(lines)
        self.model.Buses = RangeSet(0, num_nodes - 1)
        self.model.Lines = Set(initialize=lines)
        
        # optimization variables
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
        #for up / down load shifting
        self.model.LoadShiftUp = Var(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals) #variable load increase at each bus
        self.model.LoadShiftDown = Var(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals) #variable load decrease at each bus 
        self.model.eMobLoadShiftUp = Var(
             self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals) #variable emobility load increase at each bus
        self.model.eMobLoadShiftDown = Var(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals) #variable emobility load decrease at each bus
        self.model.HeatPumpFlexLoad = Var(
             self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals) # heatpump (flexible load) consumption power.
        
        #no loadshifting at these buses
        self.model.NoLoadShiftUpFixCon = Constraint(
            no_load_shift_buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LoadShiftUp[b,t] == 0)   
        self.model.NoLoadShiftDownFixCon = Constraint(
            no_load_shift_buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LoadShiftDown[b,t] == 0)
        self.model.NoeMobShiftDownFixCon = Constraint(
            no_load_shift_buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.eMobLoadShiftDown[b,t] == 0)  
        self.model.NoeMobShiftUpFixCon = Constraint(
            no_load_shift_buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.eMobLoadShiftUp[b,t] == 0) 
        self.model.NoHeatPumpShift = Constraint(
            no_load_shift_buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.HeatPumpFlexLoad[b,t] == 0) 
        
    def set_dc_bus(self, bus_demand, slack_bus, baseMVA, single_electric_node=False):
        # voltage angles at any bus are limited to +/-45deg
        # voltage angle at slack bus is 0 deg 
        self.model.Demand = Param(
            self.model.Buses,
            self.model.TimePeriods,
            initialize=lambda m,b,t: bus_demand[b][t]/baseMVA)
        self.model.DemandVar0Con2 = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LostLoad[b,t] <= m.Demand[b,t])
        if single_electric_node == False:
            self.model.V_angleCon = Constraint(
                self.model.Buses,
                self.model.TimePeriods,
                rule=lambda m,b,t: -np.pi/4 <= m.V_angle[b,t] <= np.pi/4)
            self.model.V_angle_0 = Constraint(
                self.model.TimePeriods,
                rule=lambda m,t: m.V_angle[slack_bus,t] == 0)
     
    def set_dc_bus_distIv_injection(self, bus_demand, slack_bus, baseMVA):
        # voltage angles at any bus are limited to +/-45deg
        # voltage angle at slack bus is 0 deg
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
    
    def set_line(self, reactance, tap_ratio, line_power_limit_forward_direction, line_power_limit_backward_direction, type): 
        # reactance is the x value from mpc.branch struct
        # line limit is rateA (apparent power) value from mpc.branch struct
        self.model.B = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: (
                1/(reactance[l] * tap_ratio[l]) if type[l] == 'AC' else None) 
        )      
        self.model.line_power_limit_forward_direction = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: line_power_limit_forward_direction[l]) #line thermal rating 

        self.model.line_power_limit_backward_direction = Param(
            self.model.Lines,
            within=NonNegativeReals,
            initialize=lambda m,l: line_power_limit_backward_direction[l] if type[l] == 'NTC' else line_power_limit_forward_direction[l]) 
                
        self.model.ActivePowerCon = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t: -m.line_power_limit_backward_direction[l] <= m.ActivePower[l,t] <= m.line_power_limit_forward_direction[l])

    def set_flexibility(self, max_shift_hourly, max_shift_daily, max_up_shift_hourly_emob, max_down_shift_hourly_emob, max_shift_daily_emob, emob_demand, max_up_shift_hourly_heatpump, max_down_shift_hourly_heatpump, max_p_hourly_heatpump, heatpump_demand, baseMVA, num_days, tpRes):
        # sets constraints for load shifting
        index_con = range(int(num_days))
        # a range with the number of days on a week
        index_con_weeks = np.arange(0, int(num_days), 7/tpRes)[:-1]

        # shifting limits - parameter definition
        self.model.MaxHourlyShift = Param(
            self.model.Buses,
            within=NonNegativeReals,
            initialize=lambda m,b: max_shift_hourly[b])  
        self.model.MaxDailyShift = Param(
            self.model.Buses,
            within=NonNegativeReals,
            initialize=lambda m,b: max_shift_daily[b]) 
        # to differentiate between the up and down flexibility 
        self.model.MaxUpHourlyShifteMob = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: max_up_shift_hourly_emob[b][t])
        self.model.MaxDownHourlyShifteMob = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: max_down_shift_hourly_emob[b][t])
        # to include the differentiation per day type
        self.model.MaxDailyShifteMob = Param(
            self.model.Buses,
            index_con, 
            within=NonNegativeReals,
            initialize=lambda m,b,i: max_shift_daily_emob[b][i])
        # emobility demand
        self.model.DemandeMob = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: emob_demand[b][t]/baseMVA)
        # to differentiate between the up and down flexibility (Heatpump)
        self.model.MaxUpHourlyShifteHP = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: max_up_shift_hourly_heatpump[b][t])
        self.model.MaxDownHourlyShifteHP = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: max_down_shift_hourly_heatpump[b][t])
        # to include the differentiation per day type(Heatpump)
        self.model.MaxHourlyPHP = Param(
            self.model.Buses, 
            within=NonNegativeReals,
            initialize=lambda m,b: max_p_hourly_heatpump[b])
        # Heatpump demand 
        self.model.NotflexDemandHP = Param(
            self.model.Buses,
            self.model.TimePeriods,
            within=NonNegativeReals,
            initialize=lambda m,b,t: heatpump_demand[b][t]/baseMVA)
        
        # constraints for shifting
        # hourly
        self.model.MaxHourlyUpCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LoadShiftUp[b,t] <= m.MaxHourlyShift[b])
        self.model.MaxHourlyDownCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: m.LoadShiftDown[b,t] <= m.MaxHourlyShift[b])
        # the constraint is modified since the MaxUpHourlyShifteMob variable now contains the upper limit (not the amount of MW to go up)
        self.model.MaxHourlyUpeMobCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: (m.DemandeMob[b,t] + m.eMobLoadShiftUp[b,t]) <= m.MaxUpHourlyShifteMob[b, t])
        # the constraint is modified since the MaxDownHourlyShifteMob variable now contains the lower limit (not the amount of MW to go down). The constrain MaxHourlyDowneMobCon1 is no longer necessary. 
        self.model.MaxHourlyDowneMobCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: (m.DemandeMob[b,t] - m.eMobLoadShiftDown[b,t]) >= m.MaxDownHourlyShifteMob[b, t])
        
        # Maximum and Minimum consumption power for heat pumps at all times
        self.model.MaxHourlyPHPCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: (m.NotflexDemandHP[b,t] + m.HeatPumpFlexLoad[b,t]) <= m.MaxHourlyPHP[b])
        self.model.MinHourlyPHPCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: (m.NotflexDemandHP[b,t] + m.HeatPumpFlexLoad[b,t]) >= 0)
        
        # Maximum cummulative consumed energy hourly by Heatpumps
        self.model.MaxHourlyECumulHPCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: sum( m.HeatPumpFlexLoad[b,ti] for ti in range(int(math.floor(t/24)*24), t+1) ) <= m.MaxUpHourlyShifteHP[b, t])
        
        # Minimum cummulative consumed energy hourly by Heatpumps
        self.model.MinHourlyECumulHPCon = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: sum( m.HeatPumpFlexLoad[b,ti] for ti in range(int(math.floor(t/24)*24), t+1) ) >= m.MaxDownHourlyShifteHP[b, t])

        # daily
        self.model.MaxDailyShiftCon = Constraint(
            self.model.Buses,
            index_con,
            rule=lambda m,b,i: sum(m.LoadShiftUp[b,24*i+ti] + m.LoadShiftDown[b,24*i+ti] for ti in range(24)) <= m.MaxDailyShift[b]) 
        self.model.DailyShiftZeroCon = Constraint(
            self.model.Buses,
            index_con,
            rule=lambda m,b,i: sum(m.LoadShiftUp[b,24*i+ti] - m.LoadShiftDown[b,24*i+ti] for ti in range(24)) == 0) 
        # Emob
        # modified so that the MaxDailyShifteMob is specific to the day that's being simulated. 
        self.model.MaxDailyShifteMobCon = Constraint(
            self.model.Buses,
            index_con,
            rule=lambda m,b,i: sum(m.eMobLoadShiftUp[b,24*i+ti] + m.eMobLoadShiftDown[b,24*i+ti] for ti in range(24)) <= m.MaxDailyShifteMob[b, i])
        
        # weekly (energy conservation)
        # all energy charged weekly must be the same in the unshifted, and shifted profiles. 
        self.model.WeeklyShiftZeroeMobCon = Constraint(
            self.model.Buses,
            index_con_weeks,
            rule=lambda m,b,i: sum(m.eMobLoadShiftUp[b,24*i+ti] * tpRes - m.eMobLoadShiftDown[b,24*i+ti] * tpRes for ti in range(int(168/tpRes))) == 0)
        self.model.WeeklyShiftZeroeMobCon_finalday = Constraint(
            self.model.Buses,
            rule=lambda m,b: sum(m.eMobLoadShiftUp[b,24*(num_days-1)+ti] - m.eMobLoadShiftDown[b,24*(num_days-1)+ti] for ti in range(24)) == 0) # to complete the constraint for the last day of the year 

    def limit_tso_dso_flows(self, GENS, gen_nodes, trafo_limit_mf):
        # Sets the constraints on power flow through the TSO-DSO interconnecting trafos

        # define the DSO generation units - node connection
        gens_at_node = {node: [gen for gen in GENS if gen_nodes[gen] == node] for node in range(self.num_nodes)}

        # set the parameter defining the power flow limit on the TSO-DSO trafos
        self.model.TrafoPLimit = Param(
            self.model.Buses,
            within=NonNegativeReals,
            initialize=lambda m,b: max(m.Demand[b, :]) * trafo_limit_mf)

        # Define the buses with nonzero load
        self.model.LoadBuses = Set(initialize=[b for b in self.model.Buses if sum(self.model.Demand[b, :]) > 0])

        self.model.tso_dso_trafo_power_flow_upper_limit = Constraint(
            self.model.LoadBuses,
            self.model.TimePeriods,
            rule=lambda m, b, t: sum(m.PowerGenerated[gen, t] for gen in gens_at_node[b])
                                 - m.Demand[b, t]
                                 - m.LoadShiftUp[b, t]
                                 - m.eMobLoadShiftUp[b, t]
                                 - m.HeatPumpFlexLoad[b, t]
                                 + m.LoadShiftDown[b, t]
                                 + m.eMobLoadShiftDown[b, t]
                                 + m.LostLoad[b, t] <= m.TrafoPLimit[b])

        self.model.tso_dso_trafo_power_flow_lower_limit = Constraint(
            self.model.LoadBuses,
            self.model.TimePeriods,
            rule=lambda m, b, t: sum(m.PowerGenerated[gen, t] for gen in gens_at_node[b])
                                 - m.Demand[b, t]
                                 - m.LoadShiftUp[b, t]
                                 - m.eMobLoadShiftUp[b, t]
                                 - m.HeatPumpFlexLoad[b, t]
                                 + m.LoadShiftDown[b, t]
                                 + m.eMobLoadShiftDown[b, t]
                                 + m.LostLoad[b, t] >= - m.TrafoPLimit[b])


    def connect_buses(self, line_start, line_end, type, gen_nodes, loss_factor, single_electric_node=False, buses_CH=[], bus_neighbor_NTC_rep=[], bus_neighbor_no_NTC_rep=[]):        
        # sets the constraint for nodal balance so we use it instead of set_demand (see system_state.py) when incorporating the DC constraints

        # Power flow constraints applies only in normal case (when CH is not a single electric node)
        if single_electric_node == False:
            self.model.PowerFlowCon = Constraint(
                self.model.Lines,
                self.model.TimePeriods,
                rule=lambda m,l,t: (
                    m.ActivePower[l,t] == m.B[l]*(m.V_angle[line_start[l],t] - m.V_angle[line_end[l],t]) if type[l] == 'AC' 
                    else Constraint.Skip)
            )

        # Nodal balance constraints: it applies to ...
        # 1. if CH has multiple electric nodes, nodal balance applies to all busses
        # 2. if CH has a single electric node, 
        #        2.1 nodal balance applies to neighbors of CH like before, 
        #        2.2 with one extra constraint for the CH bus (which requires special mapping of assets, therefore we treat it separately)
        #  Given that 1 and 2.1 are the same, we can use the same code for both cases. We just define the set of busses (model.BusesWithTypicalBalance) properly.

        # 1 and 2.1 ---------------------------------------------------------------------
        if single_electric_node == False: 
            buses_with_typical_nodal_balance = [b for b in self.model.Buses]
        elif single_electric_node == True: # 
            buses_with_typical_nodal_balance = bus_neighbor_no_NTC_rep  # CH00 is the aggregate CH node
        
        self.model.BusesWithTypicalBalance = Set(initialize=buses_with_typical_nodal_balance, within=self.model.Buses)

        node_start_lines = [[] for _ in range(self.num_nodes)]
        node_end_lines = [[] for _ in range(self.num_nodes)]
        gens_at_node = [[] for _ in range(self.num_nodes)]
        for l in self.model.Lines:
            node_start_lines[line_start[l]].append(l)
            node_end_lines[line_end[l]].append(l)
        for gen in range(self.state.num_generators):
            gens_at_node[gen_nodes[gen]].append(gen)        
        
        self.model.NodalConstraint = Constraint(
            self.model.BusesWithTypicalBalance,
            self.model.TimePeriods,
            rule=lambda m,b,t: - sum(m.ActivePower[l,t] for l in node_start_lines[b]) +
                sum((1 -loss_factor[l]) * m.ActivePower[l,t] if type[l] == 'DC' 
                    else m.ActivePower[l,t] for l in node_end_lines[b])               -
                m.Demand[b,t]                                                         -
                sum(m.PowerConsumed[gen,t] for gen in gens_at_node[b])                -
                m.LoadShiftUp[b,t]                                                    -
                m.HeatPumpFlexLoad[b,t]                                               -
                m.eMobLoadShiftUp[b,t]                                                +
                m.LostLoad[b,t]                                                       +
                m.LoadShiftDown[b,t]                                                  +
                m.eMobLoadShiftDown[b,t]                                              +
                sum(m.PowerGenerated[gen,t] for gen in gens_at_node[b]) == 0)
        
        # 2.2 ---------------------------------------------------------------------
        if single_electric_node == True:
            # Nodes that need to have nodal balance in this setup: CH00 (one node for CH) plus bus_neighbor_no_NTC_rep (busses of the neighbours) 
            node_start_lines = [[] for _ in range(1)]  # only one node, CH00, node_start_lines[0] gives the list of lines starting at CH00 (and going to neighbors via NTC)
            node_end_lines = [[] for _ in range(1)]    # only one node, CH00
            gens_at_node = [[] for _ in range(1)]      # only one node, CH00

            # find lines starting at the CH00 bus, i.e., lines starting from buses_CH and ending in bus_neighbor_NTC_rep
            for l in self.model.Lines:
                # if line starts at buses_CH and ends at bus_neighbor_NTC_rep
                if line_start[l] in bus_neighbor_NTC_rep and line_end[l] in bus_neighbor_no_NTC_rep:
                    node_start_lines[0].append(l)

            # find lines ending at the CH00 bus, i.e., lines starting from bus_neighbor_NTC_rep and ending in buses_CH
            for l in self.model.Lines:
                # if line starts at bus_neighbor_NTC_rep and ends at buses_CH
                if line_start[l] in bus_neighbor_no_NTC_rep and line_end[l] in bus_neighbor_NTC_rep:
                    node_end_lines[0].append(l)

            for node in buses_CH:
                # find generators at this node
                for gen in range(self.state.num_generators):
                    if gen_nodes[gen] == node:
                        gens_at_node[0].append(gen)

            self.model.NodalConstraint_one_CH = Constraint( # it is like self.model.NodalConstraint, excep it is defined over sum of all nodes in CH, buses_CH
                self.model.TimePeriods,
                rule=lambda m,t: - sum(m.ActivePower[l,t] for l in node_start_lines[0]) +
                    sum((1 -loss_factor[l]) * m.ActivePower[l,t] if type[l] == 'DC'
                        else m.ActivePower[l,t] for l in node_end_lines[0])               -
                    sum(m.Demand[b,t] for b in buses_CH)                                                         -
                    sum(m.PowerConsumed[gen,t] for gen in gens_at_node[buses_CH[0]])                -
                    sum(m.LoadShiftUp[b,t] for b in buses_CH)                                                    -
                    sum(m.HeatPumpFlexLoad[b,t] for b in buses_CH)                                               -
                    sum(m.eMobLoadShiftUp[b,t] for b in buses_CH)                                                +
                    sum(m.LostLoad[b,t] for b in buses_CH)                                                       +
                    sum(m.LoadShiftDown[b,t] for b in buses_CH)                                                  +
                    sum(m.eMobLoadShiftDown[b,t] for b in buses_CH)                                              +
                    sum(m.PowerGenerated[gen,t] for gen in gens_at_node[buses_CH[0]]) == 0) #sum of all buses in CH must be equal to zero          


    def connect_buses_distIv_injection(self, line_start, line_end, type, gen_nodes, loss_factor):
        """
        This constraint is identical to connect_buses BUT also includes the capability to curtail injections from DistIv/ABM
        """
        node_start_lines = [[] for _ in range(self.num_nodes)]
        node_end_lines = [[] for _ in range(self.num_nodes)]
        gens_at_node = [[] for _ in range(self.num_nodes)]
        for l in self.model.Lines:
            node_start_lines[line_start[l]].append(l)
            node_end_lines[line_end[l]].append(l)
        for gen in range(self.state.num_generators):
            gens_at_node[gen_nodes[gen]].append(gen)

        self.model.PowerFlowCon_AClines = Constraint(
            self.model.Lines,
            self.model.TimePeriods,
            rule=lambda m,l,t:( 
                m.ActivePower[l,t] == m.B[l]*(m.V_angle[line_start[l],t] - m.V_angle[line_end[l],t]) if type[l] == 'AC'  
                else Constraint.Skip)
        )
       
        self.model.NodalConstraint = Constraint(
            self.model.Buses,
            self.model.TimePeriods,
            rule=lambda m,b,t: - sum(m.ActivePower[l,t] for l in node_start_lines[b]) +
                sum((1 -loss_factor[l]) * m.ActivePower[l,t] if type[l] == 'DC' 
                    else m.ActivePower[l,t] for l in node_end_lines[b])               -
                m.Demand[b,t]                                                         -
                sum(m.PowerConsumed[gen,t] for gen in gens_at_node[b])                -
                m.LoadShiftUp[b,t]                                                    -
                m.HeatPumpFlexLoad[b,t]                                            -
                m.eMobLoadShiftUp[b,t]                                                +  #positive sign because CurtaildistIvInj is negative by definition
                m.CurtaildistIvInj[b,t]                                               +
                m.LostLoad[b,t]                                                       +
                m.LoadShiftDown[b,t]                                                  +
                m.eMobLoadShiftDown[b,t]                                              +                                              
                sum(m.PowerGenerated[gen,t] for gen in gens_at_node[b]) == 0)
    
    def get_lossload_cost(self, ll_cost, tpRes):
        m = self.model
        return (sum(ll_cost * m.LostLoad[b,t] for t in m.TimePeriods for b in m.Buses)) * tpRes #cost term to be added to the objective function
    
    def get_cost_distIv_injection(self, cost_injection, tpRes):
        self.model.InjectionCost = Param(
            self.model.Buses,
            within=NonNegativeReals,
            initialize=lambda m,b: cost_injection[b])
        m = self.model
        return (sum(m.InjectionCost[b] * (-(self.model.Demand[b,t]-self.model.CurtaildistIvInj[b,t])) for t in m.TimePeriods for b in m.Buses if (m.Demand[b,t] < 0.0))) * tpRes

    def set_equal_annual_exportimport(self, BORDERLINES, tpRes):
        self.model.EqualAnnualExportImportCon = Constraint(
            rule=lambda m: sum(m.ActivePower[l,t] * tpRes for l in BORDERLINES for t in m.TimePeriods) == 0)
    
    def set_net_winter_import_limit(self, BORDERLINES, tpRes, netImportLimit, baseMVA):
        self.model.JFMTimePeriods = RangeSet(0, (round((90/tpRes))*24)-1)  #Jan + Feb + Mar = 90 days
        self.model.ONDTimePeriods = RangeSet(self.num_snaphots-(round((92/tpRes))*24), self.num_snaphots-1)  #Oct + Nov + Dec = 92 days
        self.model.EqualWinterExportImportCon = Constraint(
            rule=lambda m:
                sum(m.ActivePower[l,t] * tpRes for l in BORDERLINES for t in m.JFMTimePeriods)
                + sum(m.ActivePower[l,t] * tpRes for l in BORDERLINES for t in m.ONDTimePeriods)
                >= (-(1e6 / baseMVA) * netImportLimit))

    def set_RES_target_DistIv(self, GENS_biomass_and_geothermal, GENS_candidates, GENS_PV_existing, GENS_wind_existing, res_target, tpResolution, baseMVA):
        """
        sets RES target across all time periods with DistIv participation

        -- res_target in MWh

        sum of potential power generation for wind and pv + actual generated biomass and geothermal over all time periods
        for both candidate and existing >= RES target
        [MWh]
        """
        if not hasattr(self.model, 'CurtaildistIvInj'):
            raise Exception('Missing call to set_dc_bus_distIv_injection')
        if res_target == 0:
            constraintRule = lambda m: Constraint.Skip
        else:
            constraintRule = lambda m: (
                    (sum(m.PowerGenerated[g, t] * tpResolution for g in GENS_biomass_and_geothermal for t in self.model.TimePeriods)
                     + sum(m.PowerProductionRES[g, t] * m.CandCapacityNonDisp[g] * tpResolution for g in GENS_candidates
                           for t in self.model.TimePeriods)
                     + sum(m.PowerProductionPV[g, t] * tpResolution for g in GENS_PV_existing for t in
                           self.model.TimePeriods)
                     + sum(m.PowerProductionWind[g, t] * tpResolution for g in GENS_wind_existing for t in
                           self.model.TimePeriods))
                    >= (res_target / baseMVA))
            # PowerGenerated is the actual generation. PowerProduction is the potential generation. PowerGenerated for PV from DistIv corresponds to the potential generation.
            # RES target is reduced by the DistIv injections
        self.model.RESConDistIv = Constraint(rule=constraintRule)

    def set_RES_target(self, GENS_biomass_and_geothermal, GENS_candidates, GENS_PV_existing, GENS_wind_existing, res_target, tpResolution, baseMVA):
        """
        sets RES target across all time periods

        -- res_target in MWh

        sum of potential power generation for wind and pv + actual generated biomass and geothermal over all time periods
        for both candidate and existing >= RES target
        [MWh]
        """
        if res_target == 0:
            constraintRule = lambda m: Constraint.Skip
        else:
            constraintRule = lambda m: (
                    (sum(m.PowerGenerated[g, t] * tpResolution for g in GENS_biomass_and_geothermal for t in self.model.TimePeriods)
                     + sum(m.PowerProductionRES[g, t] * m.CandCapacityNonDisp[g] * tpResolution for g in GENS_candidates
                           for t in self.model.TimePeriods)
                     + sum(m.PowerProductionPV[g, t] * tpResolution for g in GENS_PV_existing for t in
                           self.model.TimePeriods)
                     + sum(m.PowerProductionWind[g, t] * tpResolution for g in GENS_wind_existing for t in
                           self.model.TimePeriods))
                    >= (res_target / baseMVA))
            # PowerGenerated is the actual generation. PowerProduction is the potential generation. PowerGenerated for PV from DistIv corresponds to the potential generation.
            # RES target is reduced by the DistIv injections
        self.model.RESCon = Constraint(rule=constraintRule)

    def set_PROD_target(self, GENS_biomass_and_geothermal, GENS_candidates, GENS_nuclear_candidates, GENS_PV_existing, GENS_wind_existing, res_target, tpResolution, baseMVA):
        """
        sets RES target across all time periods, including nuclear as a RES generation

        -- res_target in MWh

        sum of potential power generation for wind and pv + actual generated biomass and geothermal + nuclear generation over all time periods
        for both candidate and existing >= RES target
        [MWh]
        """
        if res_target == 0:
            constraintRule = lambda m: Constraint.Skip
        else:
            constraintRule = lambda m: (
                (sum(m.PowerGenerated[g, t] * tpResolution for g in GENS_biomass_and_geothermal for t in self.model.TimePeriods)
                + sum(m.PowerProductionRES[g, t] * m.CandCapacityNonDisp[g] * tpResolution for g in GENS_candidates
                    for t in self.model.TimePeriods)
                + sum(m.PowerProductionPV[g, t] * tpResolution for g in GENS_PV_existing for t in
                    self.model.TimePeriods)
                + sum(m.PowerProductionWind[g, t] * tpResolution for g in GENS_wind_existing for t in
                    self.model.TimePeriods)
                + sum(m.PowerGenerated[g, t] * tpResolution for g in GENS_nuclear_candidates for t in
                    self.model.TimePeriods)) >= (res_target / baseMVA))
            # PowerGenerated is the actual generation. PowerProduction is the potential generation. PowerGenerated for PV from DistIv corresponds to the potential generation.
            # RES target is reduced by the DistIv injections
        self.model.RESCon = Constraint(rule=constraintRule)

    def set_rooftop_PV_target(self, GENS, pv_target, tpResolution, baseMVA, oversize_factor: float = 1.0):
        """Sets RES target 
        - rooftop PV across all time periods
        - pv_target in MWh
        """
        self.model.rooftop_PV_target_constraint = Constraint(
            rule=lambda m: 
                Constraint.Skip if pv_target == 0 else sum(m.PowerGenerated[g,t] * tpResolution for g in GENS for t in self.model.TimePeriods) >= (pv_target/baseMVA) * oversize_factor)
    
    def set_rooftop_PV_target_potential(self, GENS_PV_exist, GENS_PV_cand, pv_target, tpResolution, baseMVA, oversize_factor: float = 1.0):
        """Sets RES target
        - rooftop PV across all time periods
        - pv_target in MWh
        - Based on POTENTIAL generation from the PV
        - oversize_factor: this factor is used in the version of the constraint 
            when CentIv runs first then it's not used in the resolve of CentIv. 
            The reason behind this is so to avoid issues of infeasibility in the
            resolve if the invested PV capacity is slightly rounded during the 
            resolve of CentIv and this slightly lower capacity cannot produce 
            enough to meet the PV target. It was an issue that came up in the 
            past, so we added an oversizing (essentially of .9%) to eliminate 
            the possible infeasibility.
        """

        self.model.rooftop_PV_target_constraint = Constraint(
            rule=lambda m: 
                Constraint.Skip if pv_target == 0 else sum(m.PowerProductionRES[g, t] * m.CandCapacityNonDisp[g] * tpResolution for g in GENS_PV_cand for t in self.model.TimePeriods) 
                + sum(m.PowerProductionPV[g, t] * tpResolution for g in GENS_PV_exist for t in self.model.TimePeriods) >= (pv_target/baseMVA) * oversize_factor)

    def deactivate_PV_target(self):
        """Deactivates the rooftop PV target constraint"""
        self.model.rooftop_PV_target_constraint.deactivate()
    def activate_PV_target(self):
        """Activates the rooftop PV target constraint"""
        self.model.rooftop_PV_target_constraint.activate()
    def deactivate_RES_target(self):
        """Deactivates the RES target constraint"""
        self.model.RESCon.deactivate()
    
    """
    Generates an expression for the costs of all transmission lines and trasformers in the system
    -- returns 0
    -- this function is only relevant in cases with expansion
    """
    def get_investment_cost_trafoline(self, investment_cost_trafo, investment_cost_line, length, MVA_rating, ids_trafo_cand, ids_line_cand):
        m = self.model
        return 0  # no investment costs added to the objective f-n

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

    #get the results for the load shifting variables
    def get_loadshift_up(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.LoadShiftUp[bus,time])
        return results

    def get_loadshift_down(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.LoadShiftDown[bus,time])
        return results
    
    def get_emobloadshift_up(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.eMobLoadShiftUp[bus,time])
        return results

    def get_emobloadshift_down(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.eMobLoadShiftDown[bus,time])
        return results
    
    def get_hpflexload(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.HeatPumpFlexLoad[bus,time])
        return results
    
    def get_emobload_beforeshift(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.DemandeMob[bus,time])
        return results
    
    def get_emobload_aftershift(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.DemandeMob[bus,time]) + value(self.model.eMobLoadShiftUp[bus,time]) - value(self.model.eMobLoadShiftDown[bus,time])
        return results
    
    def get_emobload_PmaxHourlyLimit(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.MaxUpHourlyShifteMob[bus,time])
        return results
    
    def get_emobload_PminHourlyLimit(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.MaxDownHourlyShifteMob[bus,time])
        return results
    
    # TO DO, need to figure out proper indexing to use instead of 'self.num_snaphots'
    # def get_emobload_EmaxDailyLimit(self, bus):
    #     results = np.zeros(self.num_snaphots) #find the reference to similar but for days not hours
    #     for day in range(self.num_snaphots): 
    #         results[time] = value(self.model.MaxDailyShifteMob[bus,day])
    #     return results

    def get_hpload_EmaxHourlyCumulativeLimit(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.MaxUpHourlyShifteHP[bus,time])
        return results
    
    def get_hpload_EminHourlyCumulativeLimit(self, bus):
        results = np.zeros(self.num_snaphots)
        for time in range(self.num_snaphots):
            results[time] = value(self.model.MaxDownHourlyShifteHP[bus,time])
        return results