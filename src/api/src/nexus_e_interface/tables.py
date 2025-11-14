from sqlalchemy import Column, Integer, String, Float, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BusConfiguration(Base):
    __tablename__ = "busconfiguration"

    idNetworkConfig = Column(
        Integer,
        ForeignKey("networkconfiginfo.idNetworkConfig"),
        primary_key=True,
        comment="primary identifier for network configurations",
    )
    idBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        primary_key=True,
        comment="primary identifier for nodes",
    )
    BusName = Column(String(45), default=None, comment="unique name for nodes")
    Vmax = Column(Float, default=1.1, comment="p.u.")
    Vmin = Column(Float, default=0.9, comment="p.u.")
    WindShare = Column(
        Float,
        default=None,
        comment="fraction, portion of each country's Wind generation assigned to a given node",
    )
    SolarShare = Column(
        Float,
        default=None,
        comment="fraction, portion of each country's PV generation assigned to a given node",
    )
    idDistProfile = Column(
        Integer,
        default=None,
        comment="identifier for profile of DistIv’s generation at each node, this table is updated after each scenario-year simulation is completed so the next year will use the previous year’s DistIv generation as an assumption in CentIv",
    )

    # Relationships
    bus = relationship("BusData", back_populates="bus_configurations")
    network_config = relationship(
        "NetworkConfigInfo", back_populates="bus_configurations"
    )


class BusData(Base):
    __tablename__ = "busdata"

    idBus = Column(Integer, primary_key=True, comment="primary identifier for nodes")
    internalBusId = Column(Integer, default=None, comment="redundant with idBus")
    BusName = Column(String(45), default=None, comment="unique name for nodes")
    SwissgridNodeCode = Column(
        String(45), default=None, comment="match Swissgrid codes for all CH nodes"
    )
    ZoneId = Column(Integer, default=None, comment="zonal assignment of all nodes")
    X_Coord = Column(Float, default=None, comment="X coordinate in LV03 format")
    Y_Coord = Column(Float, default=None, comment="Y coordinate in LV03 format")
    BusType = Column(
        String(2),
        default="PQ",
        comment="1=PQ (load bus), 2=PV (generator bus), 3=reference (slack bus)",
    )
    Qd = Column(Float, default=0.0, comment="MVAr")
    Pd = Column(Float, default=0.0, comment="MW")
    Gs = Column(
        Float,
        default=0.0,
        comment="Shunt Conductance, MW demanded at voltage = 1.0 p.u.",
    )
    Bs = Column(
        Float,
        default=0.0,
        comment="Shunt Susceptance, MVAr injected at voltage = 1.0 p.u.",
    )
    baseKV = Column(Float, default=None, comment="kV")
    Country = Column(
        String(45), default=None, comment="Country assignment of all nodes"
    )
    SubRegion = Column(
        String(45),
        default=None,
        comment="Region assignment of all nodes, could be Canton, municipality, etc",
    )
    StartYr = Column(
        Float,
        default=None,
        comment="first year a node should be included in the network configuration",
    )
    EndYr = Column(
        Float,
        default=None,
        comment="last year a node should be included in the network configuration",
    )

    # Relationships
    bus_configurations = relationship("BusConfiguration", back_populates="bus")


class CentFlexPotential(Base):
    __tablename__ = "centflexpotential"

    Country = Column(
        String(45),
        primary_key=True,
        comment="Country assignment for this flexible potential",
    )
    Year = Column(Integer, primary_key=True, comment="year associated with the values")
    flex_type = Column(
        String(45), primary_key=True, comment="flexibility type (DSM, emobility, etc)"
    )
    PowerShift_Hrly = Column(
        Float, default=None, comment="GW, maximum power that can be shifted per hour"
    )
    EnergyShift_Daily = Column(
        Float,
        default=None,
        comment="GWh, maximum energy that can be shifted per day (is the sum of the absolute values of the upward and downward shifted energy)",
    )
    EnergyShift_Cost = Column(
        Float,
        default=None,
        comment="EUR/MWh, cost required to shift down 1 MWh (associated Up shift is not charged)",
    )


class DBInfo(Base):
    __tablename__ = "dbinfo"

    Date = Column(String(25), nullable=False, comment="date this database was created")
    Excel_file_used = Column(
        String(150),
        nullable=False,
        comment="associated Excel file used to originally create this database",
    )
    Matlab_file_used = Column(
        String(150),
        nullable=False,
        comment="associated Matlab file used to originally create this database",
    )
    created_by_user = Column(
        String(100),
        primary_key=True,
        comment="name of user who originally created this database",
    )
    Schema_version = Column(
        Float,
        nullable=False,
        comment="version number of the schema structure for this database, 1 = use for BFE Phase 1 project on flexibility, 2 = updated to more advanced database",
    )
    notes = Column(
        Text,
        default=None,
        comment="please provide any additional notes or comments about this database",
    )


class DistABGenCosts(Base):
    __tablename__ = "distabgencosts"

    Year = Column(Integer, primary_key=True, comment="year associated with the data")
    idDistABGen = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for distributed generators in the ABM",
    )
    GenName = Column(
        String(55), default=None, comment="unique name for distributed generators"
    )
    InvCost_P = Column(
        Float,
        default=None,
        comment="Non-annualized investment cost for building generator based on power capacity, EUR/kW",
    )
    VOM_Cost = Column(Float, default=None, comment="nonFuel variable O&M cost, EUR/kWh")
    Subsidy_Base = Column(
        Float, default=None, comment="one-time capacity subsidy payment, EUR"
    )
    Subsidy_1_kW = Column(
        Float,
        default=None,
        comment="one-time capacity subsidy payment, EUR/kW, applies to the first 30 kW of the PV capacity installed",
    )
    Subsidy_2_kW = Column(
        Float,
        default=None,
        comment="one-time capacity subsidy payment, EUR/kW, applies to everything except the first 30 kW of the PV capacity installed",
    )


class DistFlexPotential(Base):
    __tablename__ = "distflexpotential"

    flex_type = Column(
        String(45), primary_key=True, comment="flexibility type (DSM, emobility, etc)"
    )
    Parameter = Column(
        String(55),
        primary_key=True,
        comment="identifier for parameters with supplied values by year",
    )
    Year = Column(Integer, primary_key=True, comment="year associated with the value")
    value = Column(
        Float,
        default=None,
        comment="values for DSM potential by year, PowerShift_Hrly (in GW) is the maximum power that can be shifted per hour for all of Switzerland, EnergyShift_Daily is the maximum energy that can be shifted per day for all Switzerland",
    )


class DistGenConfigInfo(Base):
    __tablename__ = "distgenconfiginfo"

    idDistGenConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for distributed generator configurations",
    )
    Year = Column(
        Integer,
        default=None,
        comment="year associated with the distributed generator configuration",
    )
    Name = Column(
        String(55),
        default=None,
        comment="name given to the distributed generator configuration",
    )


class DistGenConfiguration(Base):
    __tablename__ = "distgenconfiguration"

    idDistGenConfig = Column(
        Integer,
        ForeignKey("distgenconfiginfo.idDistGenConfig"),
        primary_key=True,
        comment="primary identifier for distributed generator configurations",
    )
    idDistGen = Column(
        Integer,
        ForeignKey("distgendata.idDistGen"),
        primary_key=True,
        comment="primary identifier for distributed generators",
    )
    Year = Column(
        Integer,
        default=None,
        comment="year associated with the distributed generator configuration",
    )
    GenName = Column(
        String(55), default=None, comment="unique name for distributed generators"
    )
    InvCost_P = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building generator based on power capacity, kEUR/kW",
    )
    InvCost_E = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building generator based on energy capacity, kEUR/kWh",
    )
    FOM_Cost = Column(Float, default=None, comment="Fixed O&M cost, EUR/kW/yr")
    VOM_Cost = Column(
        Float, default=None, comment="nonFuel variable O&M cost, EUR-cents/kWh"
    )
    Fuel_Cost = Column(Float, default=None, comment="Fuel cost, EUR-cents/kWh")
    Heat_Credit = Column(
        Float,
        default=None,
        comment="credit/payment for excess heat produced based on electricity generation, EUR-cents/kWh-el",
    )
    KEV = Column(
        Float, default=None, comment="one-time capacity subsidy payment, EUR/kW"
    )
    WACC = Column(
        Float,
        default=None,
        comment="weighted average cost of capital, the average rate a company expects to pay to finance its assets, fraction",
    )
    LCOE = Column(
        Float, default=None, comment="leveled cost of electricity, EUR-cent/kWh"
    )
    Heat_Value = Column(
        Float, default=None, comment="value/price for excess heat, EUR-cents/kWh-th"
    )

    # Relationships
    dist_gen = relationship("DistGenData", back_populates="dist_gen_configurations")


class DistGenData(Base):
    __tablename__ = "distgendata"

    idDistGen = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for distributed generators",
    )
    GenName = Column(
        String(55), default=None, comment="unique name for distributed generators"
    )
    GenType = Column(
        String(45), default=None, comment="Basic gen type (Hydro, Conv, RES, Storage)"
    )
    Technology = Column(
        String(45),
        default=None,
        comment="Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, BatteryPV, BatteryGrid, GasCHP, etc)",
    )
    UnitType = Column(
        String(45),
        default=None,
        comment="Dispatchable or NonDispatchable (used to know which gens are controllable and can be used for reserves)",
    )
    Type = Column(
        String(45),
        default=None,
        comment="more specific details about the distributed generators unit type",
    )
    CandidateUnit = Column(
        Integer,
        default=None,
        comment="indicator if a given distributed generator should be considered for investment",
    )
    InvestmentType = Column(
        String(45),
        default=None,
        comment="indicator for the type of investment, continuous = non-discrete capacity investment blocks",
    )
    min_Size_kW = Column(
        Float, default=None, comment="kW, minimum investment size per unit built"
    )
    Pmax_kW = Column(Float, default=None, comment="kW, rated size of one unit")
    Pmin_kW = Column(
        Float, default=None, comment="kW, minimum generation from one unit"
    )
    Dischrg_max = Column(
        Float,
        default=None,
        comment="kW max discharging rate / kWh of installed energy storage volume",
    )
    Chrg_max = Column(
        Float,
        default=None,
        comment="kW max charging rate / kWh of installed energy storage volume",
    )
    eta_dis = Column(
        Float,
        default=None,
        comment="storage discharging efficiency, kW-from storage / kW-to grid, note this value should be > 1",
    )
    eta_ch = Column(
        Float,
        default=None,
        comment="storage charging efficiency, kW-to storage / kW-from grid, note this value should be < 1",
    )
    Self_dischrg = Column(
        Float, default=None, comment="fraction of Energy in storage lost per hour"
    )
    Emax = Column(
        Float,
        default=None,
        comment="max State Of Charge (Fraction max allowable of E_max_kWh)",
    )
    Emin = Column(
        Float,
        default=None,
        comment="min State Of Charge (Fraction min allowable of E_max_kWh)",
    )
    E_ini = Column(
        Float,
        default=None,
        comment="initial State Of Charge (Fraction of E_max_kWh at initial)",
    )
    E_final = Column(
        Float,
        default=None,
        comment="final State Of Charge (Fraction of E_max_kWh at final)",
    )
    Pini = Column(
        Float,
        default=None,
        comment="Initial power generation level at first time interval of simulation, fraction of Pmax",
    )
    RU = Column(Float, default=None, comment="Ramp Up rate, fraction of Pmax")
    RD = Column(Float, default=None, comment="Ramp Down Rate, fraction of Pmax")
    Lifetime = Column(Float, default=None, comment="Yrs")
    GenEffic = Column(
        Float,
        default=None,
        comment="Fractional electrical generator efficiency or heat rate, MWh-electric / MWh-heat (fuel)",
    )
    ThmlEffic = Column(
        Float,
        default=None,
        comment="Fractional thermal efficiency of generator, MWh-heat (produced) / MWh-heat (fuel)",
    )
    CapFactor = Column(
        Float,
        default=None,
        comment="Fractional ratio of actual annual production compared to maximum annual production",
    )
    CO2Rate = Column(
        Float, default=None, comment="CO2 emission rate, tonne CO2 / MWh-electric"
    )
    Emax_kWh = Column(
        Float, default=None, comment="Maximum storage volume of one unit, kWh"
    )
    FuelType = Column(
        String(45), default=None, comment="unique name of fuel used by given generator"
    )
    ElecOwnUseFactor = Column(
        Float,
        default=None,
        comment="Fraction of electricity generated that is consumed onsite for own use at the power plant",
    )

    # Relationships
    dist_gen_configurations = relationship(
        "DistGenConfiguration", back_populates="dist_gen"
    )


class DistProfiles(Base):
    __tablename__ = "distprofiles"

    idDistProfile = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="primary identifier for distributed profiles",
    )
    name = Column(
        String(45), default=None, comment="descriptive name for given profile"
    )
    type = Column(
        String(45),
        default=None,
        comment="defines the type of profile (DistIvGen, Irradiation, SolarGen, etc.)",
    )
    resolution = Column(
        String(45),
        default=None,
        comment="# hrs each entry in the profile covers (1 = hourly, 24 = daily, 168 = weekly, etc.)",
    )
    unit = Column(
        String(45), default=None, comment="associated units of the given profile"
    )
    timeSeries = Column(JSON, default=None, comment="time series values of the profile")


class DistRegionByGenTypeData(Base):
    __tablename__ = "distregionbygentypedata"

    Parameter = Column(
        String(55),
        primary_key=True,
        comment="identifier for parameters with supplied values by region and generator type",
    )
    idRegion = Column(
        Integer, primary_key=True, comment="primary identifier for distributed regions"
    )
    idDistGen = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for distributed generators",
    )
    GenName = Column(
        String(55), default=None, comment="unique name for distributed generators"
    )
    value = Column(
        Float,
        default=None,
        comment="values for parameters given by region and generator type, RetailTariff & Whole2Retail_Margin (both in EUR/MWh) are the average consumers electricity price and average markup needed to reach the consumers electricity price for a given region and a given consumer size (consumer size is identified by the PV size)",
    )


class DistRegionByIrradLevelData(Base):
    __tablename__ = "distregionbyirradleveldata"

    Parameter = Column(
        String(55),
        primary_key=True,
        comment="identifier for parameters with supplied values by region and irradiation level",
    )
    idRegion = Column(
        Integer, primary_key=True, comment="primary identifier for distributed regions"
    )
    idDistGen = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for distributed generators",
    )
    GenName = Column(
        String(55), default=None, comment="unique name for distributed generators"
    )
    IrradLevel = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for irradiation levels, kWh/m^2",
    )
    value = Column(
        Float,
        default=None,
        comment="values for parameters given by region and generator type and irradiation level, PV_CapacityPotential (in kW)",
    )


class DistRegionData(Base):
    __tablename__ = "distregiondata"

    idRegion = Column(
        Integer, primary_key=True, comment="primary identifier for distributed regions"
    )
    RegionName = Column(
        String(45),
        default=None,
        comment="name given to the distributed region (currently Canton abbreviation, matches busdata SubRegion)",
    )
    idProfile_Irrad = Column(
        Integer,
        default=None,
        comment="primary identifier for profiles, identifies the profile that defines this region’s time series irradiation",
    )
    idProfile_PVCapFactor = Column(
        Integer,
        default=None,
        comment="primary identifier for profiles, identifies the profile that defines this region’s time series PV generation",
    )
    GridTariff = Column(
        Float,
        default=None,
        comment="network usage price for customers in a region, EUR/MWh",
    )
    PVInjTariff = Column(
        Float,
        default=None,
        comment="price paid by DSO to consumers in a region for injection of excess PV generation, EUR/MWh",
    )


class FlexParamsHP(Base):
    __tablename__ = "flex_params_hp"

    idLoadConfig = Column(
        Integer, primary_key=True, comment="primary identifier for load configurations"
    )
    Parameter = Column(
        String(45),
        primary_key=True,
        comment="Indicator for which HP flexibility parameter (PowerCapacity_Max)",
    )
    year = Column(Float, default=None, comment="year associated with given profile")
    BusName = Column(String(45), primary_key=True, comment="unique name for nodes")
    unit = Column(
        String(15),
        default=None,
        comment="Indicator for the units that the values are in",
    )
    value = Column(Float, default=None, comment="values for HP flexibility parameters")


class FlexProfilesEV(Base):
    __tablename__ = "flex_profiles_ev"

    idLoadConfig = Column(
        Integer, primary_key=True, comment="primary identifier for load configurations"
    )
    Parameter = Column(
        String(45),
        primary_key=True,
        comment="Indicator for which EV flexibility profile parameter (Demand_Max, Demand_Min, DailyShift_Max)",
    )
    year = Column(Float, default=None, comment="year associated with given profile")
    BusName = Column(String(45), primary_key=True, comment="unique name for nodes")
    resolution = Column(
        String(15),
        default=None,
        comment="Indicator for the timestep resolution (hourly, daily, etc)",
    )
    unit = Column(
        String(15),
        default=None,
        comment="Indicator for the units that the values are in",
    )
    timeSeries = Column(Text, default=None, comment="time series values of the profile")


class FlexProfilesHP(Base):
    __tablename__ = "flex_profiles_hp"

    idLoadConfig = Column(
        Integer, primary_key=True, comment="primary identifier for load configurations"
    )
    Parameter = Column(
        String(45),
        primary_key=True,
        comment="Indicator for which HP flexibility profile parameter (EnergyCumulPerDay_Max,EnergyCumulPerDay_Min)",
    )
    year = Column(Float, default=None, comment="year associated with given profile")
    BusName = Column(String(45), primary_key=True, comment="unique name for nodes")
    resolution = Column(
        String(15),
        default=None,
        comment="Indicator for the timestep resolution (hourly, daily, etc)",
    )
    unit = Column(
        String(15),
        default=None,
        comment="Indicator for the units that the values are in",
    )
    timeSeries = Column(Text, default=None, comment="time series values of the profile")


class FuelPrices(Base):
    __tablename__ = "fuelprices"

    fuel = Column(
        String(20),
        primary_key=True,
        comment="Name of the fuels, e.g., “Gas_EU”, “Coal_EU”, Coal_CH”.",
    )
    year = Column(
        Integer, primary_key=True, comment="year associated with the given fuel price"
    )
    price = Column(Float, nullable=False, comment="value for the given fuel price")
    price_mult_idProfile = Column(
        Float,
        default=None,
        comment="Profile ID for the hourly price multiplier profile.",
    )
    unit = Column(
        String(45),
        nullable=False,
        comment="definition of the units for the given fuel price",
    )


class GenConfigInfo(Base):
    __tablename__ = "genconfiginfo"

    idGenConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for generator configurations",
    )
    name = Column(
        String(45), default=None, comment="name given to the generator configuration"
    )
    year = Column(
        Integer,
        default=None,
        comment="year associated with the generator configuration",
    )


class GenConfiguration(Base):
    __tablename__ = "genconfiguration"

    idGenConfig = Column(
        Integer,
        ForeignKey("genconfiginfo.idGenConfig"),
        primary_key=True,
        comment="primary identifier for generator configurations",
    )
    idBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        primary_key=True,
        comment="primary identifier for node where given generator is located",
    )
    idGen = Column(
        Integer,
        ForeignKey("gendata.idGen"),
        primary_key=True,
        comment="primary identifier for generators",
    )
    GenName = Column(String(100), default=None, comment="unique name for generators")
    idProfile = Column(
        Integer,
        ForeignKey("profiledata.idProfile"),
        default=None,
        comment="identifier for profile that defines this generator’s time series production (RES units) or time series for water inflows (Hydro units)",
    )
    CandidateUnit = Column(
        Integer,
        default=None,
        comment="indicator if a given generator does not yet exist and should be considered for investment",
    )
    Pmax = Column(Float, default=None, comment="MW")
    Pmin = Column(Float, default=None, comment="MW")
    Qmax = Column(Float, default=None, comment="MVAr")
    Qmin = Column(Float, default=None, comment="MVAr")
    Emax = Column(Float, default=None, comment="Maximum storage volume, MWh")
    Emin = Column(Float, default=None, comment="Minimum allowable storage volume, MWh")
    E_ini = Column(
        Float,
        default=None,
        comment="Initial storage volume at beginning of simulation, fraction of Emax",
    )
    VOM_Cost = Column(Float, default=None, comment="nonFuel variable O&M cost, EUR/MWh")
    FOM_Cost = Column(Float, default=None, comment="Fixed O&M cost, EUR/MW/yr")
    InvCost = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building generator, EUR/MW/yr",
    )
    InvCost_E = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building storage capacity associated with a storage generator, EUR/MWh/yr",
    )
    InvCost_Charge = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building consumption portion of a storage generator (like pumping portion of pumped hydro or electrolyzer portion of hydrogen), EUR/MW/yr",
    )
    StartCost = Column(Float, default=None, comment="EUR/MW/start")
    TotVarCost = Column(
        Float, default=None, comment="Sum of all variable operating costs, EUR/MWh"
    )
    FuelType = Column(
        String(45), default=None, comment="unique name of fuel used by given generator"
    )
    CO2Type = Column(
        String(45),
        default=None,
        comment="unique name of CO2 entry in fuel prices table used by given generator",
    )
    status = Column(
        Float, default=None, comment="online status, 1 = in service, 0 = not in service"
    )
    HedgeRatio = Column(
        Float,
        default=None,
        comment="fraction, portion of monthly average power generated to offer into the Future market clearing",
    )

    # Relationships
    gen = relationship("GenData", back_populates="gen_configurations")
    profile = relationship("ProfileData", back_populates="gen_configurations")


class GenConfigurationExtra(Base):
    __tablename__ = "genconfiguration_extra"

    idGenConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for generator configurations",
    )
    idGen = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for generators",
    )
    GenName = Column(String(100), default=None, comment="unique name for generators")
    idBus = Column(
        Integer,
        default=None,
        comment="identifier for node where given generator is located",
    )
    GenType = Column(
        String(45), default=None, comment="Basic gen type (Hydro, Conv, RES)"
    )
    Technology = Column(
        String(45),
        default=None,
        comment="Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, etc)",
    )
    UnitType = Column(
        String(45),
        default=None,
        comment="Dispatchable or NonDispatchable (used to know which gens are controllable and can be used for reserves)",
    )
    Pmax_methdac = Column(
        Float,
        default=None,
        comment="Maximum output rate of synthetic gas from the DAC + Methanation, in MW-th-Gas",
    )
    Pmin_methdac = Column(
        Float,
        default=None,
        comment="Minimum output rate of synthetic gas from the DAC + Methanation, in MW-th-Gas",
    )
    Emax_h2stor = Column(
        Float,
        default=None,
        comment="Maximum volume of hydrogen that can be stored, in tonnes of H2",
    )
    Emin_h2stor = Column(
        Float,
        default=None,
        comment="Minimum allowable level in storage, in tonnes of H2",
    )
    VOM_methdac = Column(
        Float,
        default=None,
        comment="Variable operating costs of the DAC + Methanation unit, in EUR/MWh-th-Gas",
    )
    InvCost_h2Stor = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building hydrogen storage tank/cavern, EUR/tonne-H2/yr",
    )
    InvCost_methdac = Column(
        Float,
        default=None,
        comment="Annualized investment cost for building DAC + Methanation, EUR/MWh-th-Gas/yr",
    )
    FOM_elzr = Column(
        Float,
        default=None,
        comment="Fixed operations and maintenance cost for electrolyzer, in EUR/MW-el/yr",
    )
    FOM_h2Stor = Column(
        Float,
        default=None,
        comment="Fixed operations and maintenance cost for hydrogen storage tank/cavern, in EUR/tonne-H2yr",
    )
    FOM_methdac = Column(
        Float,
        default=None,
        comment="Fixed operations and maintenance cost for DAC + Methanation, in EUR/MWh-th-Gas/yr",
    )
    Conv_elzr = Column(
        Float,
        default=None,
        comment="Conversion ratio of the electrolyzer, in tonne-H2 (out) / MWh-el (in)",
    )
    Conv_fc = Column(
        Float,
        default=None,
        comment="Conversion ratio of the fuel cell, in MWh-el (out) / tonne-H2 (in)",
    )
    Conv_methdac_h2 = Column(
        Float,
        default=None,
        comment="Conversion ratio of the DAC + Methanation for H2 input, in MWh-th-Gas (out) / tonne-H2 (in)",
    )
    Conv_methdac_el = Column(
        Float,
        default=None,
        comment="Conversion ratio of the DAC + Methanation for electricity input, in MWh-th-Gas (out) / MWh-el (in)",
    )
    Conv_methdac_co2 = Column(
        Float,
        default=None,
        comment="Conversion ratio of the DAC + Methanation for CO2, in MWh-th-Gas (out) / tonne-CO2 (captured)",
    )
    MaxInjRate_h2Stor = Column(
        Float,
        default=None,
        comment="Maximum injection rate of Hydrogen into the storage, in percent of Emax per day",
    )
    MaxWithRate_h2Stor = Column(
        Float,
        default=None,
        comment="Maximum withdrawal rate of Hydrogen from the storage, in percent of Emax per day",
    )
    FuelType_methdac = Column(
        String(45),
        default=None,
        comment="Fuel type for synthetic gas created by DAC + Methanation",
    )
    FuelType_ch4_import = Column(
        String(45),
        default=None,
        comment="Fuel type for imported synthetic methane",
    )
    FuelType_h2_domestic = Column(
        String(45),
        default=None,
        comment="Fuel type for synthesized hydrogen created domestically",
    )
    FuelType_h2_import = Column(
        String(45),
        default=None,
        comment="Fuel type for imported hydrogen",
    )
    Ind_h2_MarketConnect = Column(
        Float,
        default=None,
        comment="Indicator if the P2X unit is connected to the hydrogen market",
    )
    h2Stor_Type = Column(
        String(45), default=None, comment="Type of Hydrogen Storage (Tank or LRC)"
    )
    ElecGen_Type = Column(
        String(45),
        default=None,
        comment="Type of Electricity Generator (Fuel Cell or Hydrogen-fired Turbine)",
    )


class GenData(Base):
    __tablename__ = "gendata"

    idGen = Column(
        Integer, primary_key=True, comment="primary identifier for generators"
    )
    GenName = Column(String(100), default=None, comment="unique name for generators")
    GenType = Column(
        String(45), default=None, comment="Basic gen type (Hydro, Conv, RES)"
    )
    Technology = Column(
        String(45),
        default=None,
        comment="Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, etc)",
    )
    UnitType = Column(
        String(45),
        default=None,
        comment="Dispatchable or NonDispatchable (used to know which gens are controllable and can be used for reserves)",
    )
    StartYr = Column(
        Float,
        default=None,
        comment="Year this generator was first online (default = 2012)",
    )
    EndYr = Column(Float, default=None, comment="Last year this generator is online")
    GenEffic = Column(
        Float,
        default=None,
        comment="Fractional generator efficiency or heat rate, MWh-electric / MWh-heat",
    )
    CO2Rate = Column(
        Float, default=None, comment="CO2 emission rate, tonne CO2 / MWh-electric"
    )
    eta_dis = Column(
        Float,
        default=None,
        comment="storage discharging efficiency, kW-to grid / kW-from storage",
    )
    eta_ch = Column(
        Float,
        default=None,
        comment="storage charging efficiency, kW-to storage / kW-from grid",
    )
    RU = Column(Float, default=None, comment="Ramp Up rate, MW/hr")
    RD = Column(Float, default=None, comment="Ramp Down Rate, MW/hr")
    RU_start = Column(
        Float, default=None, comment="Ramp Up Rate during Start Up, MW/hr"
    )
    RD_shutd = Column(
        Float, default=None, comment="Ramp Down Rate during Shut Down, MW/hr"
    )
    UT = Column(Integer, default=None, comment="Minimum Up Time, hr")
    DT = Column(Integer, default=None, comment="Minimum Down Time, hr")
    Pini = Column(
        Float,
        default=None,
        comment="Initial power generation level at first time interval of simulation, MW",
    )
    Tini = Column(
        Float,
        default=None,
        comment="Number of hours generator has already been online at first time interval of simulation, hr",
    )
    meanErrorForecast24h = Column(
        Float,
        default=None,
        comment="normalized mean error for renewable generation forecasted 24 hrs ahead (dimensionless)",
    )
    sigmaErrorForecast24h = Column(
        Float,
        default=None,
        comment="standard deviation for renewable generation forecasted 24 hrs ahead (dimensionless)",
    )
    Lifetime = Column(Float, default=None, comment="Lifetime of the generator in years")

    # Relationships
    gen_configurations = relationship("GenConfiguration", back_populates="gen")


class GenTypeData(Base):
    __tablename__ = "gentypedata"

    GenType = Column(
        String(45), primary_key=True, comment="Basic gen type (Hydro, Conv, RES)"
    )
    Technology = Column(
        String(45),
        primary_key=True,
        comment="Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, etc)",
    )
    Component = Column(
        String(45),
        primary_key=True,
        comment="Component type (e.g., generator, storage, etc.)",
    )
    Year = Column(Integer, primary_key=True, comment="Year associated with the data")
    Subsidy_Indicator = Column(
        String(45), primary_key=True, comment="Indicator for subsidy type"
    )
    InvCost_UpFront = Column(Float, default=None, comment="In EUR/kW")
    InvCost_Annual_NoSubsidy = Column(Float, default=None, comment="In EUR/kW/yr")
    InvCost_Annual_Subsidy = Column(Float, default=None, comment="In EUR/kW/yr")
    WACC = Column(Float, default=None, comment="As a fraction")
    Lifetime = Column(Float, default=None, comment="In years")
    AnnuityFactor = Column(Float, default=None, comment="In (EUR/kW/yr) / (EUR/kW)")
    Subsidy_Fraction = Column(Float, default=None, comment="As a fraction")
    FixedOM_Cost = Column(Float, default=None, comment="In EUR/MW/yr")


class LineConfiguration(Base):
    __tablename__ = "lineconfiguration"

    idNetworkConfig = Column(
        Integer,
        ForeignKey("networkconfiginfo.idNetworkConfig"),
        primary_key=True,
        comment="primary identifier for network configurations",
    )
    idLine = Column(
        Integer,
        ForeignKey("linedata.idLine"),
        primary_key=True,
        comment="primary identifier for lines",
    )
    LineName = Column(String(45), nullable=False, comment="unique name for lines")
    idFromBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        comment="idBus for FROM side node of a given line",
    )
    idToBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        comment="idBus for TO side node of a given line",
    )
    angmin = Column(
        Float, default=-360.0, comment="minimum voltage angle different in degrees"
    )
    angmax = Column(
        Float, default=360.0, comment="maximum voltage angle different in degrees"
    )
    status = Column(
        Float, default=None, comment="line status, 1 = in service, 0 = out of service"
    )
    FromBusName = Column(
        String(45), default=None, comment="Bus Name for FROM side node of a given line"
    )
    ToBusName = Column(
        String(45), default=None, comment="Bus Name for TO side node of a given line"
    )
    FromCountry = Column(
        String(45),
        default=None,
        comment="Country abbreviation for FROM side node of a given line",
    )
    ToCountry = Column(
        String(45),
        default=None,
        comment="Country abbreviation for TO side node of a given line",
    )
    Ind_CrossBord = Column(
        Integer,
        default=None,
        comment="indicator if a given line crosses between two countries",
    )
    Ind_Agg = Column(
        Integer,
        default=None,
        comment="indicator if a given line is represented as an aggregation/simplification of the actual physical network",
    )
    Ind_HVDC = Column(
        Integer, default=None, comment="indicator if a given line is an HVDC line"
    )
    Candidate = Column(
        Integer,
        default=None,
        comment="indicator if a given line should be considered for investment",
    )
    CandCost = Column(
        Float,
        default=None,
        comment="annualized cost to build a candidate line, EUR/km/yr",
    )

    # Relationships
    network_config = relationship(
        "NetworkConfigInfo", back_populates="line_configurations"
    )
    line = relationship("LineData", back_populates="line_configurations")
    from_bus = relationship("BusData", foreign_keys=[idFromBus])
    to_bus = relationship("BusData", foreign_keys=[idToBus])


class LineData(Base):
    __tablename__ = "linedata"

    idLine = Column(Integer, primary_key=True, comment="primary identifier for lines")
    LineName = Column(String(45), nullable=False, comment="unique name for lines")
    line_type = Column(String(45), default=None)
    loss_factor = Column(Float, default=None)
    r = Column(Float, default=None, comment="line resistance in p.u.")
    x = Column(Float, default=None, comment="line reactance in p.u.")
    b = Column(Float, default=None, comment="line susceptance in p.u.")
    rateA = Column(Float, default=None, comment="line rating in MVA, nominal rating")
    rateA2 = Column(Float, default=None)
    rateB = Column(Float, default=None, comment="line rating in MVA, short term rating")
    rateC = Column(Float, default=None, comment="line rating in MVA, emergency rating")
    StartYr = Column(
        Float,
        default=None,
        comment="first year a line should be included in the network configuration",
    )
    EndYr = Column(
        Float,
        default=None,
        comment="last year a line should be included in the network configuration",
    )
    kV = Column(Float, default=None, comment="line voltage in kV")
    MVA_Winter = Column(
        Float, default=None, comment="line rating in MVA, applicable in winter"
    )
    MVA_Summer = Column(
        Float, default=None, comment="line rating in MVA, applicable in summer"
    )
    MVA_SprFall = Column(
        Float, default=None, comment="line rating in MVA, applicable in spring and fall"
    )
    length = Column(Float, default=None, comment="line length in km")

    # Relationships
    line_configurations = relationship("LineConfiguration", back_populates="line")


class LoadProfiles(Base):
    __tablename__ = "load_profiles"

    idLoadConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for load configurations",
    )
    LoadType = Column(
        String(25),
        primary_key=True,
        comment="Indicator for which type of electricity load (Base, eMobility, HeatPump, Hydrogen)",
    )
    year = Column(Float, default=None, comment="year associated with given profile")
    BusName = Column(String(45), primary_key=True, comment="unique name for nodes")
    unit = Column(
        String(15),
        default=None,
        comment="Indicator for the units that the values are in",
    )
    timeSeries = Column(Text, default=None, comment="time series values of the profile")


class LoadConfigInfo(Base):
    __tablename__ = "loadconfiginfo"

    idLoadConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for load configurations",
    )
    name = Column(
        String(45), default=None, comment="name given to the load configuration"
    )
    year = Column(
        Integer, default=None, comment="year associated with the load configuration"
    )


class LoadConfiguration(Base):
    __tablename__ = "loadconfiguration"

    idLoadConfig = Column(
        Integer,
        ForeignKey("loadconfiginfo.idLoadConfig"),
        primary_key=True,
        comment="primary identifier for load configurations",
    )
    idBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        comment="primary identifier for nodes, defines the node ID that this load is associated with",
    )
    idLoad = Column(
        Integer,
        ForeignKey("loaddata.idLoad"),
        primary_key=True,
        comment="primary identifier for load",
    )
    idProfile = Column(
        Integer,
        ForeignKey("profiledata.idProfile"),
        default=None,
        comment="primary identifier for profiles, identifies the profile that defines this load’s time series demand",
    )
    DemandShare = Column(
        Float,
        default=None,
        comment="fraction, portion of each country's Load demand assigned to a given node",
    )
    idProfile_eMobility = Column(
        Integer,
        default=None,
        comment="primary identifier for profiles, identifies the profile that defines this load’s time series demand for additional electrification of mobility",
    )
    idProfile_eHeatPump = Column(
        Integer,
        default=None,
        comment="primary identifier for profiles, identifies the profile that defines this load’s time series demand for additional electrification of heating",
    )
    idProfile_eHydrogen = Column(
        Integer,
        default=None,
        comment="primary identifier for profiles, identifies the profile that defines this load’s time series demand for additional electrification to produce hydrogen",
    )

    # Relationships
    load = relationship("LoadData", back_populates="load_configurations")
    profile = relationship("ProfileData", back_populates="load_configurations")


class LoadData(Base):
    __tablename__ = "loaddata"

    idLoad = Column(Integer, primary_key=True, comment="primary identifier for loads")
    LoadType = Column(String(45), default=None, comment="name of load profile used")
    Pd = Column(
        Float, default=None, comment="example value for this load’s P demand, MW"
    )
    Qd = Column(
        Float, default=None, comment="example value for this load’s Q demand, MVAr"
    )
    hedgeRatio = Column(
        Float,
        default=None,
        comment="fraction, portion of avg monthly load to supply in the future market clearing",
    )
    meanForecastError24h = Column(
        Float,
        default=None,
        comment="normalized mean error for load forecasted 24 hrs ahead (dimensionless)",
    )
    sigmaForecastError24h = Column(
        Float,
        default=None,
        comment="standard deviation for load forecasted 24 hrs ahead (dimensionless)",
    )

    # Relationships
    load_configurations = relationship("LoadConfiguration", back_populates="load")


class MarketsConfiguration(Base):
    __tablename__ = "marketsconfiguration"

    idMarketsConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for market configurations",
    )
    name = Column(
        String(45), default=None, comment="name given to the market configuration"
    )
    year = Column(
        Integer, default=None, comment="year associated with the market configuration"
    )
    MarketsConfigDataStructure = Column(
        JSON,
        default=None,
        comment="JSON string of data for the market configuration for the given year",
    )


class NetworkConfigInfo(Base):
    __tablename__ = "networkconfiginfo"

    idNetworkConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for network configurations",
    )
    name = Column(
        String(45), default=None, comment="name given to the network configuration"
    )
    year = Column(
        Integer, default=None, comment="year associated with the network configuration"
    )
    baseMVA = Column(
        Float,
        default=None,
        comment="MVA base used for converting power into per unit quantities, usually set to 100 MVA",
    )
    MatpowerVersion = Column(
        String(1),
        default="2",
        comment="defines which MatPower case version is used, currently version = 2 is the default",
    )

    # Relationships
    bus_configurations = relationship(
        "BusConfiguration", back_populates="network_config"
    )
    line_configurations = relationship(
        "LineConfiguration", back_populates="network_config"
    )
    transformer_configurations = relationship(
        "TransformerConfiguration", back_populates="network_config"
    )


class ProfileData(Base):
    __tablename__ = "profiledata"

    idProfile = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="primary identifier for profiles",
    )
    name = Column(
        String(100), default=None, comment="descriptive name for given profile"
    )
    Country = Column(String(45), default=None)
    year = Column(Integer, default=None, comment="year associated with given profile")
    type = Column(
        String(45),
        default=None,
        comment="defines the type of profile (Load, Generation, Water Inflow, etc.)",
    )
    resolution = Column(
        String(45),
        default=None,
        comment="# hrs each entry in the profile covers (1 = hourly, 24 = daily, etc.)",
    )
    unit = Column(
        String(45), default="MW", comment="associated units of the given profile"
    )
    timeSeries = Column(JSON, default=None, comment="time series values of the profile")

    # Relationships
    gen_configurations = relationship("GenConfiguration", back_populates="profile")
    load_configurations = relationship("LoadConfiguration", back_populates="profile")


class Projections(Base):
    __tablename__ = "projections"

    item = Column(
        String(15),
        primary_key=True,
        comment="identifier for parameters with supplied projections",
    )
    scenario = Column(
        String(15),
        primary_key=True,
        comment="identifier for scenario options (e.g., Ref, High)",
    )
    year = Column(Integer, primary_key=True, comment="year associated with the value")
    value = Column(
        Float,
        nullable=False,
        comment="values for projections (e.g., indexed to 2010=1)",
    )


class ScenarioConfiguration(Base):
    __tablename__ = "scenarioconfiguration"

    idScenario = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for scenario configurations",
    )
    idNetworkConfig = Column(
        Integer,
        ForeignKey("networkconfiginfo.idNetworkConfig"),
        primary_key=True,
        comment="primary identifier for network configurations",
    )
    idLoadConfig = Column(
        Integer,
        ForeignKey("loadconfiginfo.idLoadConfig"),
        primary_key=True,
        comment="primary identifier for load configurations",
    )
    idGenConfig = Column(
        Integer,
        ForeignKey("genconfiginfo.idGenConfig"),
        primary_key=True,
        comment="primary identifier for generator configurations",
    )
    idMarketsConfig = Column(
        Integer,
        ForeignKey("marketsconfiguration.idMarketsConfig"),
        primary_key=True,
        comment="primary identifier for market configurations",
    )
    idAnnualTargetsConfig = Column(
        Integer,
        ForeignKey("swiss_annual_targets_configinfo.idAnnualTargetsConfig"),
        primary_key=True,
        comment="primary identifier for swiss annual target/requirement configurations",
    )
    idDistGenConfig = Column(
        Integer,
        ForeignKey("distgenconfiginfo.idDistGenConfig"),
        primary_key=True,
        comment="primary identifier for distributed generator configurations",
    )
    name = Column(
        String(100), default=None, comment="name given to the scenario configuration"
    )
    runParamDataStructure = Column(
        JSON,
        default=None,
        comment="miscellaneous other information, includes startDate, endDate, colorCodes",
    )
    Year = Column(
        Integer,
        nullable=False,
        comment="year associated with the scenario configuration",
    )


class SecurityRef(Base):
    __tablename__ = "securityref"

    DNS_vals = Column(
        JSON,
        primary_key=True,
        default=None,
        comment="Demand not served, in MW, number of entries corresponds to the number of contingencies tested",
    )
    NLF_vals = Column(
        JSON,
        default=None,
        comment="Number of Line/Transformer failures in a given contingency test",
    )


class SwissAnnualTargetsConfigInfo(Base):
    __tablename__ = "swiss_annual_targets_configinfo"

    idAnnualTargetsConfig = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for swiss annual target/requirement configurations",
    )
    name = Column(
        String(45), default=None, comment="name given to the annual target/requirement"
    )
    Year = Column(
        Integer, default=None, comment="year associated with the target/requirement"
    )

    # Relationships
    annual_targets = relationship(
        "SwissAnnualTargetsConfiguration",
        back_populates="annual_targets_config_info",
    )


class SwissAnnualTargetsConfiguration(Base):
    __tablename__ = "swiss_annual_targets_configuration"

    idAnnualTargetsConfig = Column(
        Integer,
        ForeignKey("swiss_annual_targets_configinfo.idAnnualTargetsConfig"),
        primary_key=True,
        comment="primary identifier for swiss annual target/requirement configurations",
    )
    TargetName = Column(
        String(100),
        primary_key=True,
        comment="Name of this target",
    )
    Year = Column(
        Float,
        default=None,
        comment="year associated with the target/requirement",
    )
    Type = Column(
        String(45),
        default="0",
        comment="Type of the target, can be ‘Target’ which sets a threshold to exceed, or ‘Requirement’ that sets a value to try to match without going much over",
    )
    Value = Column(
        Float,
        default=None,
        comment="Value of the target in the year/config indicated",
    )
    Units = Column(
        String(45),
        default=None,
        comment="Units associated with the annual target/requirement, e.g. TWh-el, tonne-CO2, etc",
    )
    idProfile = Column(
        Float,
        default=None,
        comment="identifier for profile that defines targets hourly profile, the profile is normalized by the annual quantity",
    )

    # Relationships
    annual_targets_config_info = relationship(
        "SwissAnnualTargetsConfigInfo", back_populates="annual_targets"
    )


class TransformerConfiguration(Base):
    __tablename__ = "transformerconfiguration"

    idNetworkConfig = Column(
        Integer,
        ForeignKey("networkconfiginfo.idNetworkConfig"),
        primary_key=True,
        comment="primary identifier for network configurations",
    )
    idTransformer = Column(
        Integer,
        ForeignKey("transformerdata.idTransformer"),
        primary_key=True,
        comment="primary identifier for transformers",
    )
    TrafoName = Column(
        String(45), nullable=False, comment="unique name for transformers"
    )
    idFromBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        comment="idBus for FROM side node of a given transformer",
    )
    idToBus = Column(
        Integer,
        ForeignKey("busdata.idBus"),
        comment="idBus for TO side node of a given transformer",
    )
    angmin = Column(
        Float, default=-360.0, comment="minimum voltage angle different in degrees"
    )
    angmax = Column(
        Float, default=360.0, comment="maximum voltage angle different in degrees"
    )
    status = Column(
        Float,
        default=None,
        comment="transformer status, 1 = in service, 0 = out of service",
    )
    FromBusName = Column(
        String(45),
        default=None,
        comment="Bus Name for FROM side node of a given transformer",
    )
    ToBusName = Column(
        String(45),
        default=None,
        comment="Bus Name for TO side node of a given transformer",
    )
    FromCountry = Column(
        String(45),
        default=None,
        comment="Country abbreviation for FROM side node of a given transformer",
    )
    ToCountry = Column(
        String(45),
        default=None,
        comment="Country abbreviation for TO side node of a given transformer",
    )
    Candidate = Column(
        Integer,
        default=None,
        comment="indicator if a given transformer should be considered for investment",
    )
    CandCost = Column(
        Float,
        default=None,
        comment="annualized cost to build a candidate transformer, EUR/km/yr",
    )

    # Relationships
    network_config = relationship(
        "NetworkConfigInfo", back_populates="transformer_configurations"
    )
    transformer = relationship("TransformerData", back_populates="configurations")
    from_bus = relationship("BusData", foreign_keys=[idFromBus])
    to_bus = relationship("BusData", foreign_keys=[idToBus])

class TransformerData(Base):
    __tablename__ = "transformerdata"

    idTransformer = Column(
        Integer,
        primary_key=True,
        comment="primary identifier for transformers",
    )
    TrafoName = Column(
        String(45),
        nullable=False,
        comment="unique name for transformers",
    )
    line_type = Column(
        String(45),
        default=None,
        comment="type of transformer line",
    )
    loss_factor = Column(
        Float,
        default=None,
        comment="loss factor of the transformer",
    )
    r = Column(
        Float,
        default=None,
        comment="transformer resistance in p.u., all transformer parameters are defined in p.u. based on voltage of the TO (secondary) side node",
    )
    x = Column(
        Float,
        default=None,
        comment="transformer reactance in p.u., all transformer parameters are defined in p.u. based on voltage of the TO (secondary) side node",
    )
    b = Column(
        Float,
        default=None,
        comment="transformer susceptance in p.u., all transformer parameters are defined in p.u. based on voltage of the TO (secondary) side node",
    )
    rateA = Column(
        Float,
        default=0.0,
        comment="transformer rating in MVA, nominal rating",
    )
    rateA2 = Column(
        Float,
        default=None,
        comment="additional transformer rating in MVA",
    )
    rateB = Column(
        Float,
        default=0.0,
        comment="transformer rating in MVA, short term rating",
    )
    rateC = Column(
        Float,
        default=0.0,
        comment="transformer rating in MVA, emergency rating",
    )
    tapRatio = Column(
        Float,
        default=1.0,
        comment="transformer tap ratio, unitless",
    )
    angle = Column(
        Float,
        default=0.0,
        comment="transformer phase shift angle in degrees",
    )
    StartYr = Column(
        Float,
        default=None,
        comment="first year a transformer should be included in the network configuration",
    )
    EndYr = Column(
        Float,
        default=None,
        comment="last year a transformer should be included in the network configuration",
    )
    MVA_Winter = Column(
        Float,
        default=None,
        comment="transformer rating in MVA, applicable in winter",
    )
    MVA_Summer = Column(
        Float,
        default=None,
        comment="transformer rating in MVA, applicable in summer",
    )
    MVA_SprFall = Column(
        Float,
        default=None,
        comment="transformer rating in MVA, applicable in spring and fall",
    )
    length = Column(
        Float,
        default=None,
        comment="transformer length in km",
    )

    # Relationships
    configurations = relationship(
        "TransformerConfiguration", back_populates="transformer"
    )

class Workforce(Base):
    __tablename__ = "workforce"

    popscen = Column(
        String(45),
        primary_key=True,
        nullable=False,
        comment="identifier for population projection scenario, scenarios A, B, C-00-2015 represent scenarios developed by BFS for population projections from 2015 to 2050",
    )
    year = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="year associated with the value",
    )
    value = Column(
        Float,
        nullable=False,
        comment="values represent head counts in full time equivalents",
    )
