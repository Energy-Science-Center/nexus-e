-- MySQL dump 10.13  Distrib 8.0.36, for macos14 (arm64)
--
-- Host: itet-psl-s04    Database: z_setup_schema9_v1
-- ------------------------------------------------------
-- Server version	8.0.36

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `busconfiguration`
--

DROP TABLE IF EXISTS `busconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `busconfiguration` (
  `idNetworkConfig` int NOT NULL COMMENT 'primary identifier for network configurations',
  `idBus` int NOT NULL COMMENT 'primary identifier for nodes',
  `BusName` varchar(45) DEFAULT NULL COMMENT 'unique name for nodes',
  `Vmax` double DEFAULT '1.1' COMMENT 'p.u.',
  `Vmin` double DEFAULT '0.9' COMMENT 'p.u.',
  `WindShare` double DEFAULT NULL COMMENT 'fraction, portion of each country''s Wind generation assigned to a given node',
  `SolarShare` double DEFAULT NULL COMMENT 'fraction, portion of each country''s PV generation assigned to a given node',
  `idDistProfile` int DEFAULT NULL COMMENT 'identifier for profile of DistIv’s generation at each node, this table is updated after each scenario-year simulation is completed so the next year will use the previous year’s DistIv generation as an assumption in CentIv',
  PRIMARY KEY (`idNetworkConfig`,`idBus`),
  KEY `fk_BusDataConfiguration_BusData1_idx` (`idBus`),
  KEY `fk_BusDataConfiguration_NetworkConfigInfo1_idx` (`idNetworkConfig`),
  CONSTRAINT `fk_BusDataConfiguration_BusData1` FOREIGN KEY (`idBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_BusDataConfiguration_NetworkConfigInfo1` FOREIGN KEY (`idNetworkConfig`) REFERENCES `networkconfiginfo` (`idNetworkConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `busconfiguration`
--

LOCK TABLES `busconfiguration` WRITE;
/*!40000 ALTER TABLE `busconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `busconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `busdata`
--

DROP TABLE IF EXISTS `busdata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `busdata` (
  `idBus` int NOT NULL COMMENT 'primary identifier for nodes',
  `internalBusId` int DEFAULT NULL COMMENT 'redundant with idBus',
  `BusName` varchar(45) DEFAULT NULL COMMENT 'unique name for nodes',
  `SwissgridNodeCode` varchar(45) DEFAULT NULL COMMENT 'match Swissgrid codes for all CH nodes',
  `ZoneId` int DEFAULT NULL COMMENT 'zonal assignment of all nodes',
  `X_Coord` float DEFAULT NULL COMMENT 'X coordinate in LV03 format',
  `Y_Coord` float DEFAULT NULL COMMENT 'Y coordinate in LV03 format',
  `BusType` varchar(2) DEFAULT 'PQ' COMMENT '1=PQ (load bus), 2=PV (generator bus), 3=reference (slack bus)',
  `Qd` double DEFAULT '0' COMMENT 'MVAr',
  `Pd` double DEFAULT '0' COMMENT 'MW',
  `Gs` double DEFAULT '0' COMMENT 'Shunt Conductance, MW demanded at voltage = 1.0 p.u.',
  `Bs` double DEFAULT '0' COMMENT 'Shunt Susceptance, MVAr injected at voltage = 1.0 p.u.',
  `baseKV` double DEFAULT NULL COMMENT 'kV',
  `Country` varchar(45) DEFAULT NULL COMMENT 'Country assignment of all nodes',
  `SubRegion` varchar(45) DEFAULT NULL COMMENT 'Region assignment of all nodes, could be Canton, municipality, etc',
  `StartYr` double DEFAULT NULL COMMENT 'first year a node should be included in the network configuration',
  `EndYr` double DEFAULT NULL COMMENT 'last year a node should be included in the network configuration',
  PRIMARY KEY (`idBus`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `busdata`
--

LOCK TABLES `busdata` WRITE;
/*!40000 ALTER TABLE `busdata` DISABLE KEYS */;
/*!40000 ALTER TABLE `busdata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `centflexpotential`
--

DROP TABLE IF EXISTS `centflexpotential`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `centflexpotential` (
  `Country` varchar(45) NOT NULL COMMENT 'Country assignment for this flexible potential',
  `Year` int NOT NULL COMMENT 'year associated with the values',
  `flex_type` varchar(45) NOT NULL COMMENT 'flexibility type (DSM, emobility, etc)',
  `PowerShift_Hrly` double DEFAULT NULL COMMENT 'GW, maximum power that can be shifted per hour',
  `EnergyShift_Daily` double DEFAULT NULL COMMENT 'GWh, maximum energy that can be shifted per day (is the sum of the absolute values of the upward and downward shifted energy)',
  `EnergyShift_Cost` double DEFAULT NULL COMMENT 'EUR/MWh, cost required to shift down 1 MWh (associated Up shift is not charged)',
  PRIMARY KEY (`Country`,`Year`,`flex_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `centflexpotential`
--

LOCK TABLES `centflexpotential` WRITE;
/*!40000 ALTER TABLE `centflexpotential` DISABLE KEYS */;
/*!40000 ALTER TABLE `centflexpotential` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dbinfo`
--

DROP TABLE IF EXISTS `dbinfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dbinfo` (
  `Date` varchar(25) NOT NULL COMMENT 'date this database was created',
  `Excel_file_used` varchar(150) NOT NULL COMMENT 'associated Excel file used to originally create this database',
  `Matlab_file_used` varchar(150) NOT NULL COMMENT 'associated Matlab file used to originally create this database',
  `created_by_user` varchar(100) NOT NULL COMMENT 'name of user who originally created this database',
  `Schema_version` double NOT NULL COMMENT 'version number of the schema structure for this database, 1 = use for BFE Phase 1 project on flexibility, 2 = updated to more advanced database',
  `notes` text COMMENT 'please provide any additional notes or comments about this database'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dbinfo`
--

LOCK TABLES `dbinfo` WRITE;
/*!40000 ALTER TABLE `dbinfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `dbinfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distabgencosts`
--

DROP TABLE IF EXISTS `distabgencosts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distabgencosts` (
  `Year` int NOT NULL COMMENT 'year associated with the data',
  `idDistABGen` int NOT NULL COMMENT 'primary identifier for distributed generators in the ABM',
  `GenName` varchar(55) DEFAULT NULL COMMENT 'unique name for distributed generators',
  `InvCost_P` double DEFAULT NULL COMMENT 'Non-annualized investment cost for building generator based on power capacity, EUR/kW',
  `VOM_Cost` double DEFAULT NULL COMMENT 'nonFuel variable O&M cost, EUR/kWh',
  `Subsidy_Base` double DEFAULT NULL COMMENT 'one-time capacity subsidy payment, EUR\\n',
  `Subsidy_1_kW` double DEFAULT NULL COMMENT 'one-time capacity subsidy payment, EUR/kW, applies to the first 30 kW of the PV capacity installed',
  `Subsidy_2_kW` double DEFAULT NULL COMMENT 'one-time capacity subsidy payment, EUR/kW, applies to everything except the first 30 kW of the PV capacity installed',
  PRIMARY KEY (`Year`,`idDistABGen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distabgencosts`
--

LOCK TABLES `distabgencosts` WRITE;
/*!40000 ALTER TABLE `distabgencosts` DISABLE KEYS */;
/*!40000 ALTER TABLE `distabgencosts` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distflexpotential`
--

DROP TABLE IF EXISTS `distflexpotential`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distflexpotential` (
  `flex_type` varchar(45) NOT NULL COMMENT 'flexibility type (DSM, emobility, etc)',
  `Parameter` varchar(55) NOT NULL COMMENT 'identifier for parameters with supplied values by year',
  `Year` int NOT NULL COMMENT 'year associated with the value',
  `value` double DEFAULT NULL COMMENT 'values for DSM potential by year, PowerShift_Hrly (in GW) is the maximum power that can be shifted per hour for all of Switzerland, EnergyShift_Daily is the maximum energy that can be shifted per day for all Switzerland',
  PRIMARY KEY (`flex_type`,`Parameter`,`Year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distflexpotential`
--

LOCK TABLES `distflexpotential` WRITE;
/*!40000 ALTER TABLE `distflexpotential` DISABLE KEYS */;
/*!40000 ALTER TABLE `distflexpotential` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distgenconfiginfo`
--

DROP TABLE IF EXISTS `distgenconfiginfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distgenconfiginfo` (
  `idDistGenConfig` int NOT NULL COMMENT 'primary identifier for distributed generator configurations',
  `Year` int DEFAULT NULL COMMENT 'year associated with the distributed generator configuration',
  `Name` varchar(55) DEFAULT NULL COMMENT 'name given to the distributed generator configuration',
  PRIMARY KEY (`idDistGenConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distgenconfiginfo`
--

LOCK TABLES `distgenconfiginfo` WRITE;
/*!40000 ALTER TABLE `distgenconfiginfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `distgenconfiginfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distgenconfiguration`
--

DROP TABLE IF EXISTS `distgenconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distgenconfiguration` (
  `idDistGenConfig` int NOT NULL COMMENT 'primary identifier for distributed generator configurations',
  `idDistGen` int NOT NULL COMMENT 'primary identifier for distributed generators',
  `Year` int DEFAULT NULL COMMENT 'year associated with the distributed generator configuration',
  `GenName` varchar(55) DEFAULT NULL COMMENT 'unique name for distributed generators',
  `InvCost_P` double DEFAULT NULL COMMENT 'Annualized investment cost for building generator based on power capacity, kEUR/kW',
  `InvCost_E` double DEFAULT NULL COMMENT 'Annualized investment cost for building generator based on energy capacity, kEUR/kWh',
  `FOM_Cost` double DEFAULT NULL COMMENT 'Fixed O&M cost, EUR/kW/yr',
  `VOM_Cost` double DEFAULT NULL COMMENT 'nonFuel variable O&M cost, EUR-cents/kWh',
  `Fuel_Cost` double DEFAULT NULL COMMENT 'Fuel cost, EUR-cents/kWh',
  `Heat_Credit` double DEFAULT NULL COMMENT 'credit/payment for excess heat produced based on electricity generation, EUR-cents/kWh-el',
  `KEV` double DEFAULT NULL COMMENT 'one-time capacity subsidy payment, EUR/kW',
  `WACC` double DEFAULT NULL COMMENT 'weighted average cost of capital, the average rate a company expects to pay to finance its assets, fraction',
  `LCOE` double DEFAULT NULL COMMENT 'leveled cost of electricity, EUR-cent/kWh',
  `Heat_Value` double DEFAULT NULL COMMENT 'value/price for excess heat, EUR-cents/kWh-th',
  PRIMARY KEY (`idDistGenConfig`,`idDistGen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distgenconfiguration`
--

LOCK TABLES `distgenconfiguration` WRITE;
/*!40000 ALTER TABLE `distgenconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `distgenconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distgendata`
--

DROP TABLE IF EXISTS `distgendata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distgendata` (
  `idDistGen` int NOT NULL COMMENT 'primary identifier for distributed generators',
  `GenName` varchar(55) DEFAULT NULL COMMENT 'unique name for distributed generators',
  `GenType` varchar(45) DEFAULT NULL COMMENT 'Basic gen type (Hydro, Conv, RES, Storage)',
  `Technology` varchar(45) DEFAULT NULL COMMENT 'Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, BatteryPV, BatteryGrid, GasCHP, etc)',
  `UnitType` varchar(45) DEFAULT NULL COMMENT 'Dispatchable or NonDispatchable (used to know which gens are controllable and can be used for reserves)',
  `Type` varchar(45) DEFAULT NULL COMMENT 'more specific details about the distributed generators unit type',
  `CandidateUnit` tinyint DEFAULT NULL COMMENT 'indicator if a given distributed generator should be considered for investment',
  `InvestmentType` varchar(45) DEFAULT NULL COMMENT 'indicator for the type of investment, continuous = non-discrete capacity investment blocks',
  `min_Size_kW` double DEFAULT NULL COMMENT 'kW, minimum investment size per unit built',
  `Pmax_kW` double DEFAULT NULL COMMENT 'kW, rated size of one unit',
  `Pmin_kW` double DEFAULT NULL COMMENT 'kW, minimum generation from one unit',
  `Dischrg_max` double DEFAULT NULL COMMENT 'kW max discharging rate / kWh of installed energy storage volume',
  `Chrg_max` double DEFAULT NULL COMMENT 'kW max charging rate / kWh of installed energy storage volume',
  `eta_dis` double DEFAULT NULL COMMENT 'storage discharging efficiency, kW-from storage / kW-to grid, note this value should be > 1',
  `eta_ch` double DEFAULT NULL COMMENT 'storage charging efficiency, kW-to storage / kW-from grid, note this value should be < 1',
  `Self_dischrg` double DEFAULT NULL COMMENT 'fraction of Energy in storage lost per hour',
  `Emax` double DEFAULT NULL COMMENT 'max State Of Charge (Fraction max allowable of E_max_kWh)',
  `Emin` double DEFAULT NULL COMMENT 'min State Of Charge (Fraction min allowable of E_max_kWh)',
  `E_ini` double DEFAULT NULL COMMENT 'initial State Of Charge (Fraction of E_max_kWh at initial)',
  `E_final` double DEFAULT NULL COMMENT 'final State Of Charge (Fraction of E_max_kWh at final)',
  `Pini` double DEFAULT NULL COMMENT 'Initial power generation level at first time interval of simulation, fraction of Pmax',
  `RU` double DEFAULT NULL COMMENT 'Ramp Up rate, fraction of Pmax',
  `RD` double DEFAULT NULL COMMENT 'Ramp Down Rate, fraction of Pmax',
  `Lifetime` double DEFAULT NULL COMMENT 'Yrs',
  `GenEffic` double DEFAULT NULL COMMENT 'Fractional electrical generator efficiency or heat rate, MWh-electric / MWh-heat (fuel)',
  `ThmlEffic` double DEFAULT NULL COMMENT 'Fractional thermal efficiency of generator, MWh-heat (produced) / MWh-heat (fuel)',
  `CapFactor` double DEFAULT NULL COMMENT 'Fractional ratio of actual annual production compared to maximum annual production',
  `CO2Rate` double DEFAULT NULL COMMENT 'CO2 emission rate, tonne CO2 / MWh-electric',
  `Emax_kWh` double DEFAULT NULL COMMENT 'Maximum storage volume of one unit, kWh',
  `FuelType` varchar(45) DEFAULT NULL COMMENT 'unique name of fuel used by given generator',
  `ElecOwnUseFactor` double DEFAULT NULL COMMENT 'Fraction of electricity generated that is consumed onsite for own use at the power plant',
  PRIMARY KEY (`idDistGen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distgendata`
--

LOCK TABLES `distgendata` WRITE;
/*!40000 ALTER TABLE `distgendata` DISABLE KEYS */;
/*!40000 ALTER TABLE `distgendata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distprofiles`
--

DROP TABLE IF EXISTS `distprofiles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distprofiles` (
  `idDistProfile` int NOT NULL AUTO_INCREMENT COMMENT 'primary identifier for distributed profiles',
  `name` varchar(45) DEFAULT NULL COMMENT 'descriptive name for given profile',
  `type` varchar(45) DEFAULT NULL COMMENT 'defines the type of profile (DistIvGen, Irradiation, SolarGen, etc.)',
  `resolution` varchar(45) DEFAULT NULL COMMENT '# hrs each entry in the profile covers (1 = hourly, 24 = daily, 168 = weekly, etc.)',
  `unit` varchar(45) DEFAULT NULL COMMENT 'associated units of the given profile',
  `timeSeries` json DEFAULT NULL COMMENT 'time series values of the profile',
  PRIMARY KEY (`idDistProfile`)
) ENGINE=InnoDB AUTO_INCREMENT=219 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distprofiles`
--

LOCK TABLES `distprofiles` WRITE;
/*!40000 ALTER TABLE `distprofiles` DISABLE KEYS */;
/*!40000 ALTER TABLE `distprofiles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distregionbygentypedata`
--

DROP TABLE IF EXISTS `distregionbygentypedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distregionbygentypedata` (
  `Parameter` varchar(55) NOT NULL COMMENT 'identifier for parameters with supplied values by region and generator type',
  `idRegion` int NOT NULL COMMENT 'primary identifier for distributed regions',
  `idDistGen` int NOT NULL COMMENT 'primary identifier for distributed generators',
  `GenName` varchar(55) DEFAULT NULL COMMENT 'unique name for distributed generators',
  `value` double DEFAULT NULL COMMENT 'values for parameters given by region and generator type, RetailTariff & Whole2Retail_Margin (both in EUR/MWh) are the average consumers electricity price and average markup needed to reach the consumers electricity price for a given region and a given consumer size (consumer size is identified by the PV size)',
  PRIMARY KEY (`idRegion`,`idDistGen`,`Parameter`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distregionbygentypedata`
--

LOCK TABLES `distregionbygentypedata` WRITE;
/*!40000 ALTER TABLE `distregionbygentypedata` DISABLE KEYS */;
/*!40000 ALTER TABLE `distregionbygentypedata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distregionbyirradleveldata`
--

DROP TABLE IF EXISTS `distregionbyirradleveldata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distregionbyirradleveldata` (
  `Parameter` varchar(55) NOT NULL COMMENT 'identifier for parameters with supplied values by region and irradiation level',
  `idRegion` int NOT NULL COMMENT 'primary identifier for distributed regions',
  `idDistGen` int NOT NULL COMMENT 'primary identifier for distributed generators',
  `GenName` varchar(55) DEFAULT NULL COMMENT 'unique name for distributed generators',
  `IrradLevel` int NOT NULL COMMENT 'primary identifier for irradiation levels, kWh/m^2',
  `value` double DEFAULT NULL COMMENT 'values for parameters given by region and generator type and irradiation level, PV_CapacityPotential (in kW)',
  PRIMARY KEY (`Parameter`,`idRegion`,`idDistGen`,`IrradLevel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distregionbyirradleveldata`
--

LOCK TABLES `distregionbyirradleveldata` WRITE;
/*!40000 ALTER TABLE `distregionbyirradleveldata` DISABLE KEYS */;
/*!40000 ALTER TABLE `distregionbyirradleveldata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `distregiondata`
--

DROP TABLE IF EXISTS `distregiondata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `distregiondata` (
  `idRegion` int NOT NULL COMMENT 'primary identifier for distributed regions',
  `RegionName` varchar(45) DEFAULT NULL COMMENT 'name given to the distributed region (currently Canton abbreviation, matches busdata SubRegion)',
  `idProfile_Irrad` int DEFAULT NULL COMMENT 'primary identifier for profiles, identifies the profile that defines this region’s time series irradiation',
  `idProfile_PVCapFactor` int DEFAULT NULL COMMENT 'primary identifier for profiles, identifies the profile that defines this region’s time series PV generation',
  `GridTariff` double DEFAULT NULL COMMENT 'network usage price for customers in a region, EUR/MWh',
  `PVInjTariff` double DEFAULT NULL COMMENT 'price paid by DSO to consumers in a region for injection of excess PV generation, EUR/MWh',
  PRIMARY KEY (`idRegion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `distregiondata`
--

LOCK TABLES `distregiondata` WRITE;
/*!40000 ALTER TABLE `distregiondata` DISABLE KEYS */;
/*!40000 ALTER TABLE `distregiondata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `flex_params_hp`
--

DROP TABLE IF EXISTS `flex_params_hp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `flex_params_hp` (
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `Parameter` varchar(45) NOT NULL COMMENT 'Indicator for which HP flexibility parameter (PowerCapacity_Max)',
  `year` double DEFAULT NULL COMMENT 'year associated with given profile',
  `BusName` varchar(45) NOT NULL COMMENT 'unique name for nodes',
  `unit` varchar(15) DEFAULT NULL COMMENT 'Indicator for the units that the values are in',
  `value` double DEFAULT NULL COMMENT 'values for HP flexibility parameters',
  PRIMARY KEY (`idLoadConfig`,`Parameter`,`BusName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `flex_params_hp`
--

LOCK TABLES `flex_params_hp` WRITE;
/*!40000 ALTER TABLE `flex_params_hp` DISABLE KEYS */;
/*!40000 ALTER TABLE `flex_params_hp` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `flex_profiles_ev`
--

DROP TABLE IF EXISTS `flex_profiles_ev`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `flex_profiles_ev` (
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `Parameter` varchar(45) NOT NULL COMMENT 'Indicator for which EV flexibility profile parameter (Demand_Max, Demand_Min, DailyShift_Max)',
  `year` double DEFAULT NULL COMMENT 'year associated with given profile',
  `BusName` varchar(45) NOT NULL COMMENT 'unique name for nodes',
  `resolution` varchar(15) DEFAULT NULL COMMENT 'Indicator for the timestep resolution (hourly, daily, etc)',
  `unit` varchar(15) DEFAULT NULL COMMENT 'Indicator for the units that the values are in',
  `timeSeries` mediumtext COMMENT 'time series values of the profile',
  PRIMARY KEY (`idLoadConfig`,`Parameter`,`BusName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `flex_profiles_ev`
--

LOCK TABLES `flex_profiles_ev` WRITE;
/*!40000 ALTER TABLE `flex_profiles_ev` DISABLE KEYS */;
/*!40000 ALTER TABLE `flex_profiles_ev` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `flex_profiles_hp`
--

DROP TABLE IF EXISTS `flex_profiles_hp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `flex_profiles_hp` (
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `Parameter` varchar(45) NOT NULL COMMENT 'Indicator for which HP flexibility profile parameter (EnergyCumulPerDay_Max,EnergyCumulPerDay_Min)',
  `year` double DEFAULT NULL COMMENT 'year associated with given profile',
  `BusName` varchar(45) NOT NULL COMMENT 'unique name for nodes',
  `resolution` varchar(15) DEFAULT NULL COMMENT 'Indicator for the timestep resolution (hourly, daily, etc)',
  `unit` varchar(15) DEFAULT NULL COMMENT 'Indicator for the units that the values are in',
  `timeSeries` mediumtext COMMENT 'time series values of the profile',
  PRIMARY KEY (`idLoadConfig`,`Parameter`,`BusName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `flex_profiles_hp`
--

LOCK TABLES `flex_profiles_hp` WRITE;
/*!40000 ALTER TABLE `flex_profiles_hp` DISABLE KEYS */;
/*!40000 ALTER TABLE `flex_profiles_hp` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fuelprices`
--

DROP TABLE IF EXISTS `fuelprices`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fuelprices` (
  `fuel` varchar(20) NOT NULL COMMENT 'Name of the fuels, e.g., “Gas_EU”, “Coal_EU”, Coal_CH”.',
  `year` int NOT NULL COMMENT 'year associated with the given fuel price',
  `price` double NOT NULL COMMENT 'value for the given fuel price',
  `price_mult_idProfile` double DEFAULT NULL COMMENT 'Profile ID for the hourly price multiplier profile.',
  `unit` varchar(45) NOT NULL COMMENT 'definition of the units for the given fuel price',
  PRIMARY KEY (`fuel`,`year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fuelprices`
--

LOCK TABLES `fuelprices` WRITE;
/*!40000 ALTER TABLE `fuelprices` DISABLE KEYS */;
/*!40000 ALTER TABLE `fuelprices` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `genconfiginfo`
--

DROP TABLE IF EXISTS `genconfiginfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `genconfiginfo` (
  `idGenConfig` int NOT NULL COMMENT 'primary identifier for generator configurations',
  `name` varchar(45) DEFAULT NULL COMMENT 'name given to the generator configuration',
  `year` int DEFAULT NULL COMMENT 'year associated with the generator configuration',
  PRIMARY KEY (`idGenConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `genconfiginfo`
--

LOCK TABLES `genconfiginfo` WRITE;
/*!40000 ALTER TABLE `genconfiginfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `genconfiginfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `genconfiguration`
--

DROP TABLE IF EXISTS `genconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `genconfiguration` (
  `idGenConfig` int NOT NULL COMMENT 'primary identifier for generator configurations',
  `idBus` int NOT NULL COMMENT 'primary identifier for node where given generator is located',
  `idGen` int NOT NULL COMMENT 'primary identifier for generators',
  `GenName` varchar(100) DEFAULT NULL COMMENT 'unique name for generators',
  `idProfile` int DEFAULT NULL COMMENT 'identifier for profile that defines this generator’s time series production (RES units) or time series for water inflows (Hydro units)',
  `CandidateUnit` tinyint DEFAULT NULL COMMENT 'indicator if a given generator does not yet exist and should be considered for investment',
  `Pmax` double DEFAULT NULL COMMENT 'MW',
  `Pmin` double DEFAULT NULL COMMENT 'MW',
  `Qmax` double DEFAULT NULL COMMENT 'MVAr',
  `Qmin` double DEFAULT NULL COMMENT 'MVAr',
  `Emax` double DEFAULT NULL COMMENT 'Maximum storage volume, MWh',
  `Emin` double DEFAULT NULL COMMENT 'Minimum allowable storage volume, MWh',
  `E_ini` double DEFAULT NULL COMMENT 'Initial storage volume at beginning of simulation, fraction of Emax',
  `VOM_Cost` double DEFAULT NULL COMMENT 'nonFuel variable O&M cost, EUR/MWh',
  `FOM_Cost` double DEFAULT NULL COMMENT 'Fixed O&M cost, EUR/MW/yr',
  `InvCost` double DEFAULT NULL COMMENT 'Annualized investment cost for building generator, EUR/MW/yr',
  `InvCost_E` double DEFAULT NULL COMMENT 'Annualized investment cost for building storage capacity associated with a storage generator, EUR/MWh/yr',
  `InvCost_Charge` double DEFAULT NULL COMMENT 'Annualized investment cost for building consumption portion of a storage generator (like pumping portion of pumped hydro or electrolyzer portion of hydrogen), EUR/MW/yr',
  `StartCost` double DEFAULT NULL COMMENT 'EUR/MW/start',
  `TotVarCost` double DEFAULT NULL COMMENT 'Sum of all variable operating costs, EUR/MWh',
  `FuelType` varchar(45) DEFAULT NULL COMMENT 'unique name of fuel used by given generator',
  `CO2Type` varchar(45) DEFAULT NULL COMMENT 'unique name of CO2 entry in fuel prices table used by given generator',
  `status` double DEFAULT NULL COMMENT 'online status, 1 = in service, 0 = not in service',
  `HedgeRatio` double DEFAULT NULL COMMENT 'fraction, portion of monthly average power generated to offer into the Future market clearing',
  PRIMARY KEY (`idGenConfig`,`idBus`,`idGen`),
  KEY `fk_GenComposition_BusData1_idx` (`idBus`) /*!80000 INVISIBLE */,
  KEY `fk_GenComposition_GenData1_idx` (`idGen`),
  KEY `fk_GenConfiguration_ProfileData1_idx` (`idProfile`),
  KEY `fk_GenConfiguration_GenConfigInfo1_idx` (`idGenConfig`),
  CONSTRAINT `fk_GenComposition_BusData1` FOREIGN KEY (`idBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_GenComposition_GenData1` FOREIGN KEY (`idGen`) REFERENCES `gendata` (`idGen`),
  CONSTRAINT `fk_GenConfiguration_GenConfigInfo1` FOREIGN KEY (`idGenConfig`) REFERENCES `genconfiginfo` (`idGenConfig`),
  CONSTRAINT `fk_GenConfiguration_ProfileData1` FOREIGN KEY (`idProfile`) REFERENCES `profiledata` (`idProfile`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `genconfiguration`
--

LOCK TABLES `genconfiguration` WRITE;
/*!40000 ALTER TABLE `genconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `genconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `genconfiguration_extra`
--

DROP TABLE IF EXISTS `genconfiguration_extra`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `genconfiguration_extra` (
  `idGenConfig` int NOT NULL,
  `idGen` int NOT NULL COMMENT 'primary identifier for generators',
  `GenName` varchar(100) DEFAULT NULL COMMENT 'unique name for generators',
  `idBus` int DEFAULT NULL COMMENT 'identifier for node where given generator is located',
  `GenType` varchar(45) DEFAULT NULL COMMENT 'Basic gen type (Hydro, Conv, RES)',
  `Technology` varchar(45) DEFAULT NULL COMMENT 'Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, etc)',
  `UnitType` varchar(45) DEFAULT NULL COMMENT 'Dispatchable or NonDispatchable (used to know which gens are controllable and can be used for reserves)',
  `Pmax_methdac` double DEFAULT NULL COMMENT 'Maximum output rate of synthetic gas from the DAC + Methanation, meant to be the size of this component, in MW-th-Gas',
  `Pmin_methdac` double DEFAULT NULL COMMENT 'Minimum output rate of synthetic gas from the DAC + Methanation, in MW-th-Gas',
  `Emax_h2stor` double DEFAULT NULL COMMENT 'The maximum volume of hydrogen that can be stored in this storage tank/cavern, in tonnes of H2',
  `Emin_h2stor` double DEFAULT NULL COMMENT 'Minimum allowable level in storage, in tonnes of H2',
  `VOM_methdac` double DEFAULT NULL COMMENT 'Variable operating costs of the DAC + Methanation unit, in EUR/MWh-th-Gas',
  `InvCost_h2Stor` double DEFAULT NULL COMMENT 'Annualized investment cost for building hydrogen storage tank/cavern, EUR/tonne-H2/yr',
  `InvCost_methdac` double DEFAULT NULL COMMENT 'Annualized investment cost for building DAC + Methanation, EUR/MWh-th-Gas/yr',
  `FOM_elzr` double DEFAULT NULL COMMENT 'Fixed operations and maintenance cost for electrolyzer, in EUR/MW-el/yr',
  `FOM_h2Stor` double DEFAULT NULL COMMENT 'Fixed operations and maintenance cost for hydrogen storage tank/cavern, in EUR/tonne-H2yr',
  `FOM_methdac` double DEFAULT NULL COMMENT 'Fixed operations and maintenance cost for DAC + Methanation, in EUR/MWh-th-Gas/yr',
  `Conv_elzr` double DEFAULT NULL COMMENT 'Conversion ratio of the electrolyzer, in tonne-H2 (out) / MWh-el (in)',
  `Conv_fc` double DEFAULT NULL COMMENT 'Conversion ratio of the fuel cell, in MWh-el (out) / tonne-H2 (in)',
  `Conv_methdac_h2` double DEFAULT NULL COMMENT 'Conversion ratio of the DAC + Methanation for H2 input, in MWh-th-Gas (out) / tonne-H2 (in)',
  `Conv_methdac_el` double DEFAULT NULL COMMENT 'Conversion ratio of the DAC + Methanation for electricity input, in MWh-th-Gas (out) / MWh-el (in)',
  `Conv_methdac_co2` double DEFAULT NULL COMMENT 'Conversion ratio of the DAC + Methanation for CO2 (internally captured then converted), in MWh-th-Gas (out) / tonne-CO2 (captured)',
  `MaxInjRate_h2Stor` double DEFAULT NULL COMMENT 'Maximum injection rate of Hydrogen into the storage, in percent of Emax per day',
  `MaxWithRate_h2Stor` double DEFAULT NULL COMMENT 'Maximum withdrawal rate of Hydrogen into the storage, in percent of Emax per day',
  `FuelType_methdac` varchar(45) DEFAULT NULL COMMENT 'Unique name of fuel that the synthetic gas created by the DAC + Methanation is sold at, in EUR/MWh-LHV',
  `FuelType_ch4_import` varchar(45) DEFAULT NULL COMMENT 'Unique name of fuel that the imported synthetic-methane is sold at, in EUR/MWh-LHV',
  `FuelType_h2_domestic` varchar(45) DEFAULT NULL COMMENT 'Unique name of fuel that the synthesized hydrogen created by the domestic electrolyzer is sold at, in EUR/tonne-H2',
  `FuelType_h2_import` varchar(45) DEFAULT NULL COMMENT 'Unique name of fuel that the imported hydrogen is sold at, in EUR/tonne-H2',
  `Ind_h2_MarketConnect` double DEFAULT NULL COMMENT 'Indicator if the P2X unit is connected to the hydrogen market (must be along the hydrogen pipeline path)',
  `h2Stor_Type` varchar(45) DEFAULT NULL COMMENT 'Defines the type of Hydrogen Storage (Tank or LRC)',
  `ElecGen_Type` varchar(45) DEFAULT NULL COMMENT 'Defines the type of Electricity Generator (Fuel Cell or Hydrogen-fired Turbine)',
  PRIMARY KEY (`idGenConfig`,`idGen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `genconfiguration_extra`
--

LOCK TABLES `genconfiguration_extra` WRITE;
/*!40000 ALTER TABLE `genconfiguration_extra` DISABLE KEYS */;
/*!40000 ALTER TABLE `genconfiguration_extra` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `gendata`
--

DROP TABLE IF EXISTS `gendata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `gendata` (
  `idGen` int NOT NULL COMMENT 'primary identifier for generators',
  `GenName` varchar(100) DEFAULT NULL COMMENT 'unique name for generators',
  `GenType` varchar(45) DEFAULT NULL COMMENT 'Basic gen type (Hydro, Conv, RES)',
  `Technology` varchar(45) DEFAULT NULL COMMENT 'Technology subtype (Dam, Pump, RoR, Nucl, Lignite, Coal, GasCC, GasSC, Biomass, Oil, Wind, PV, GeoTh, etc)',
  `UnitType` varchar(45) DEFAULT NULL COMMENT 'Dispatchable or NonDispatchable (used to know which gens are controllable and can be used for reserves)',
  `StartYr` double DEFAULT NULL COMMENT 'Year this generator was first online (default = 2012)',
  `EndYr` double DEFAULT NULL COMMENT 'Last year this generator is online',
  `GenEffic` double DEFAULT NULL COMMENT 'Fractional generator efficiency or heat rate, MWh-electric / MWh-heat',
  `CO2Rate` double DEFAULT NULL COMMENT 'CO2 emission rate, tonne CO2 / MWh-electric',
  `eta_dis` double DEFAULT NULL COMMENT 'storage discharging efficiency, kW-to grid / kW-from storage',
  `eta_ch` double DEFAULT NULL COMMENT 'storage charging efficiency, kW-to storage / kW-from grid',
  `RU` double DEFAULT NULL COMMENT 'Ramp Up rate, MW/hr',
  `RD` double DEFAULT NULL COMMENT 'Ramp Down Rate, MW/hr',
  `RU_start` double DEFAULT NULL COMMENT 'Ramp Up Rate during Start Up, MW/hr',
  `RD_shutd` double DEFAULT NULL COMMENT 'Ramp Down Rate during Shut Down, MW/hr',
  `UT` int DEFAULT NULL COMMENT 'Minimum Up Time, hr',
  `DT` int DEFAULT NULL COMMENT 'Minimum Down Time, hr',
  `Pini` double DEFAULT NULL COMMENT 'Initial power generation level at first time interval of simulation, MW',
  `Tini` double DEFAULT NULL COMMENT 'Number of hours generator has already been online at first time interval of simulation, hr',
  `meanErrorForecast24h` double DEFAULT NULL COMMENT 'normalised mean error for renewable generation forecasted 24 hrs ahead (dimensionless)',
  `sigmaErrorForecast24h` double DEFAULT NULL COMMENT 'standard deviation for renewable generation forecasted 24 hrs ahead (dimensionless)',
  `Lifetime` double DEFAULT NULL,
  PRIMARY KEY (`idGen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `gendata`
--

LOCK TABLES `gendata` WRITE;
/*!40000 ALTER TABLE `gendata` DISABLE KEYS */;
/*!40000 ALTER TABLE `gendata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `gentypedata`
--

DROP TABLE IF EXISTS `gentypedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `gentypedata` (
  `GenType` varchar(45) NOT NULL,
  `Technology` varchar(45) NOT NULL,
  `Component` varchar(45) NOT NULL,
  `Year` int NOT NULL,
  `Subsidy_Indicator` varchar(45) NOT NULL,
  `InvCost_UpFront` double DEFAULT NULL COMMENT 'In EUR/kW',
  `InvCost_Annual_NoSubsidy` double DEFAULT NULL COMMENT 'In EUR/kW/yr',
  `InvCost_Annual_Subsidy` double DEFAULT NULL COMMENT 'In EUR/kW/yr',
  `WACC` double DEFAULT NULL COMMENT 'As a fraction',
  `Lifetime` double DEFAULT NULL COMMENT 'In years',
  `AnnuityFactor` double DEFAULT NULL COMMENT 'In (EUR/kW/yr) / (EUR/kW)',
  `Subsidy_Fraction` double DEFAULT NULL COMMENT 'As a fraction',
  `FixedOM_Cost` double DEFAULT NULL COMMENT 'In EUR/MW/yr',
  PRIMARY KEY (`GenType`,`Technology`,`Year`,`Subsidy_Indicator`,`Component`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `gentypedata`
--

LOCK TABLES `gentypedata` WRITE;
/*!40000 ALTER TABLE `gentypedata` DISABLE KEYS */;
/*!40000 ALTER TABLE `gentypedata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lineconfiguration`
--

DROP TABLE IF EXISTS `lineconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `lineconfiguration` (
  `idNetworkConfig` int NOT NULL COMMENT 'primary identifier for network configurations',
  `idLine` int NOT NULL COMMENT 'primary identifier for lines',
  `LineName` varchar(45) NOT NULL COMMENT 'unique name for lines',
  `idFromBus` int NOT NULL COMMENT 'idBus for FROM side node of a given line',
  `idToBus` int NOT NULL COMMENT 'idBus for TO side node of a given line',
  `angmin` double DEFAULT '-360' COMMENT 'minimum voltage angle different in degrees',
  `angmax` double DEFAULT '360' COMMENT 'maximum voltage angle different in degrees',
  `status` double DEFAULT NULL COMMENT 'line status, 1 = in service, 0 = out of service',
  `FromBusName` varchar(45) DEFAULT NULL COMMENT 'Bus Name for FROM side node of a given line',
  `ToBusName` varchar(45) DEFAULT NULL COMMENT 'Bus Name for TO side node of a given line',
  `FromCountry` varchar(45) DEFAULT NULL COMMENT 'Country abbreviation for FROM side node of a given line',
  `ToCountry` varchar(45) DEFAULT NULL COMMENT 'Country abbreviation for TO side node of a given line',
  `Ind_CrossBord` int DEFAULT NULL COMMENT 'indicator if a given line crosses between two countries',
  `Ind_Agg` int DEFAULT NULL COMMENT 'indicator if a given line is represented as an aggregation/simplification of the actual physical network',
  `Ind_HVDC` int DEFAULT NULL COMMENT 'indicator if a given line is an HVDC line',
  `Candidate` tinyint DEFAULT NULL COMMENT 'indicator if a given line should be considered for investment',
  `CandCost` double DEFAULT NULL COMMENT 'annualized cost to build a candidate line, EUR/km/yr',
  PRIMARY KEY (`idNetworkConfig`,`idLine`),
  KEY `fk_LineConfiguration_BusData1_idx` (`idFromBus`),
  KEY `fk_LineConfiguration_BusData2_idx` (`idToBus`),
  KEY `fk_LineConfiguration_LineData1_idx` (`idLine`),
  KEY `fk_LineConfiguration_NetworkconfigInfo1_idx` (`idNetworkConfig`),
  CONSTRAINT `fk_LineConfiguration_BusData1` FOREIGN KEY (`idFromBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_LineConfiguration_BusData2` FOREIGN KEY (`idToBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_LineConfiguration_LineData1` FOREIGN KEY (`idLine`) REFERENCES `linedata` (`idLine`),
  CONSTRAINT `fk_LineConfiguration_NetworkconfigInfo1` FOREIGN KEY (`idNetworkConfig`) REFERENCES `networkconfiginfo` (`idNetworkConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lineconfiguration`
--

LOCK TABLES `lineconfiguration` WRITE;
/*!40000 ALTER TABLE `lineconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `lineconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `linedata`
--

DROP TABLE IF EXISTS `linedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `linedata` (
  `idLine` int NOT NULL COMMENT 'primary identifier for lines',
  `LineName` varchar(45) NOT NULL COMMENT 'unique name for lines',
  `line_type` varchar(45) DEFAULT NULL,
  `loss_factor` double DEFAULT NULL,
  `r` double DEFAULT NULL COMMENT 'line resistance in p.u.',
  `x` double DEFAULT NULL COMMENT 'line reactance in p.u.',
  `b` double DEFAULT NULL COMMENT 'line susceptance in p.u.',
  `rateA` double DEFAULT NULL COMMENT 'line rating in MVA, nominal rating',
  `rateA2` double DEFAULT NULL,
  `rateB` double DEFAULT NULL COMMENT 'line rating in MVA, short term rating',
  `rateC` double DEFAULT NULL COMMENT 'line rating in MVA, emergency rating',
  `StartYr` double DEFAULT NULL COMMENT 'first year a line should be included in the network configuration',
  `EndYr` double DEFAULT NULL COMMENT 'last year a line should be included in the network configuration',
  `kV` double DEFAULT NULL COMMENT 'line voltage in kV',
  `MVA_Winter` double DEFAULT NULL COMMENT 'line rating in MVA, applicable in winter',
  `MVA_Summer` double DEFAULT NULL COMMENT 'line rating in MVA, applicable in summer',
  `MVA_SprFall` double DEFAULT NULL COMMENT 'line rating in MVA, applicable in spring and fall',
  `length` double DEFAULT NULL COMMENT 'line length in km',
  PRIMARY KEY (`idLine`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `linedata`
--

LOCK TABLES `linedata` WRITE;
/*!40000 ALTER TABLE `linedata` DISABLE KEYS */;
/*!40000 ALTER TABLE `linedata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `load_profiles`
--

DROP TABLE IF EXISTS `load_profiles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `load_profiles` (
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `LoadType` varchar(25) NOT NULL COMMENT 'Indicator for which type of electricity load (Base, eMobility, HeatPump, Hydrogen)',
  `year` double DEFAULT NULL COMMENT 'year associated with given profile',
  `BusName` varchar(45) NOT NULL COMMENT 'unique name for nodes',
  `unit` varchar(15) DEFAULT NULL COMMENT 'Indicator for the units that the values are in',
  `timeSeries` mediumtext COMMENT 'time series values of the profile',
  PRIMARY KEY (`idLoadConfig`,`LoadType`,`BusName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `load_profiles`
--

LOCK TABLES `load_profiles` WRITE;
/*!40000 ALTER TABLE `load_profiles` DISABLE KEYS */;
/*!40000 ALTER TABLE `load_profiles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `loadconfiginfo`
--

DROP TABLE IF EXISTS `loadconfiginfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `loadconfiginfo` (
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `name` varchar(45) DEFAULT NULL COMMENT 'name given to the load configuration',
  `year` int DEFAULT NULL COMMENT 'year associated with the load configuration',
  PRIMARY KEY (`idLoadConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `loadconfiginfo`
--

LOCK TABLES `loadconfiginfo` WRITE;
/*!40000 ALTER TABLE `loadconfiginfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `loadconfiginfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `loadconfiguration`
--

DROP TABLE IF EXISTS `loadconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `loadconfiguration` (
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `idBus` int NOT NULL COMMENT 'primary identifier for nodes, defines the node ID that this load is associated with',
  `idLoad` int NOT NULL COMMENT 'primary identifier for load',
  `idProfile` int DEFAULT NULL COMMENT 'primary identifier for profiles, identifies the profile that defines this load’s time series demand, is considered the base demand without additional electrification compared to recent historical level',
  `DemandShare` double DEFAULT NULL COMMENT 'fraction, portion of each country''s Load demand assigned to a given node, can be used to split a single load value for a country into the nodal loads or to split a single profile of values for a country into the nodal profiles for loads',
  `idProfile_eMobility` int DEFAULT NULL COMMENT 'primary identifier for profiles, identifies the profile that defines this load’s time series demand, is for additional electrification of mobility/transport compared to recent historical level',
  `idProfile_eHeatPump` int DEFAULT NULL COMMENT 'primary identifier for profiles, identifies the profile that defines this load’s time series demand, is for additional electrification of heating from heat pumps compared to recent historical level',
  `idProfile_eHydrogen` int DEFAULT NULL COMMENT 'primary identifier for profiles, identifies the profile that defines this load’s time series demand, is for additional electrification to produce hydrogen for use in other energy sectors compared to recent historical level',
  PRIMARY KEY (`idLoadConfig`,`idLoad`),
  KEY `fk_LoadConfiguration_LoadData1_idx` (`idLoad`),
  KEY `fk_LoadConfiguration_BusData1_idx` (`idBus`),
  KEY `fk_LoadConfiguration_ProfileData1_idx` (`idProfile`),
  KEY `fk_LoadConfiguration_LoadConfigInfo1_idx` (`idLoadConfig`),
  CONSTRAINT `fk_LoadConfiguration_BusData1` FOREIGN KEY (`idBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_LoadConfiguration_LoadConfigInfo1` FOREIGN KEY (`idLoadConfig`) REFERENCES `loadconfiginfo` (`idLoadConfig`),
  CONSTRAINT `fk_LoadConfiguration_LoadData1` FOREIGN KEY (`idLoad`) REFERENCES `loaddata` (`idLoad`),
  CONSTRAINT `fk_LoadConfiguration_ProfileData1` FOREIGN KEY (`idProfile`) REFERENCES `profiledata` (`idProfile`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `loadconfiguration`
--

LOCK TABLES `loadconfiguration` WRITE;
/*!40000 ALTER TABLE `loadconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `loadconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `loaddata`
--

DROP TABLE IF EXISTS `loaddata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `loaddata` (
  `idLoad` int NOT NULL COMMENT 'primary identifier for loads',
  `LoadType` varchar(45) DEFAULT NULL COMMENT 'name of load profile used',
  `Pd` double DEFAULT NULL COMMENT 'example value for this load’s P demand, MW',
  `Qd` double DEFAULT NULL COMMENT 'example value for this load’s Q demand, MVAr',
  `hedgeRatio` double DEFAULT NULL COMMENT 'fraction, portion of avg monthly load to supply in the future market clearing',
  `meanForecastError24h` double DEFAULT NULL COMMENT 'normalized mean error for load forecasted 24 hrs ahead (dimensionless)',
  `sigmaForecastError24h` double DEFAULT NULL COMMENT 'standard deviation for load forecasted 24 hrs ahead (dimensionless)',
  PRIMARY KEY (`idLoad`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `loaddata`
--

LOCK TABLES `loaddata` WRITE;
/*!40000 ALTER TABLE `loaddata` DISABLE KEYS */;
/*!40000 ALTER TABLE `loaddata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `marketsconfiguration`
--

DROP TABLE IF EXISTS `marketsconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `marketsconfiguration` (
  `idMarketsConfig` int NOT NULL COMMENT 'primary identifier for market configurations',
  `name` varchar(45) DEFAULT NULL COMMENT 'name given to the market configuration',
  `year` int DEFAULT NULL COMMENT 'year associated with the market configuration',
  `MarketsConfigDataStructure` json DEFAULT NULL COMMENT 'JSON string of data for the market configuration for the given year, includes ntcValues, marketDaZones, marketDACoupling, marketDaBusMapping, marketForwardCoupling, marketForwardBusMapping, marketBaReserveInformation',
  PRIMARY KEY (`idMarketsConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `marketsconfiguration`
--

LOCK TABLES `marketsconfiguration` WRITE;
/*!40000 ALTER TABLE `marketsconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `marketsconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `networkconfiginfo`
--

DROP TABLE IF EXISTS `networkconfiginfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `networkconfiginfo` (
  `idNetworkConfig` int NOT NULL COMMENT 'primary identifier for network configurations',
  `name` varchar(45) DEFAULT NULL COMMENT 'name given to the network configuration',
  `year` int DEFAULT NULL COMMENT 'year associated with the network configuration',
  `baseMVA` double DEFAULT NULL COMMENT 'MVA base used for converting power into per unit quantities, usually set to 100 MVA',
  `MatpowerVersion` varchar(1) DEFAULT '2' COMMENT 'defines which MatPower case version is used, currently version = 2 is the default, version 1 is outdated',
  PRIMARY KEY (`idNetworkConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `networkconfiginfo`
--

LOCK TABLES `networkconfiginfo` WRITE;
/*!40000 ALTER TABLE `networkconfiginfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `networkconfiginfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `profiledata`
--

DROP TABLE IF EXISTS `profiledata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `profiledata` (
  `idProfile` int NOT NULL AUTO_INCREMENT COMMENT 'primary identifier for profiles',
  `name` varchar(100) DEFAULT NULL COMMENT 'descriptive name for given profile',
  `Country` varchar(45) DEFAULT NULL,
  `year` int DEFAULT NULL COMMENT 'year associated with given profile',
  `type` varchar(45) DEFAULT NULL COMMENT 'defines the type of profile (Load, Generation, Water Inflow, Refueling/Maintenance Status, Reserve Requirement, etc.)',
  `resolution` varchar(45) DEFAULT NULL COMMENT '# hrs each entry in the profile covers (1 = hourly, 24 = daily, 168 = weekly, etc.)',
  `unit` varchar(45) DEFAULT 'MW' COMMENT 'associated units of the given profile',
  `timeSeries` json DEFAULT NULL COMMENT 'time series values of the profile',
  PRIMARY KEY (`idProfile`)
) ENGINE=InnoDB AUTO_INCREMENT=1124 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `profiledata`
--

LOCK TABLES `profiledata` WRITE;
/*!40000 ALTER TABLE `profiledata` DISABLE KEYS */;
/*!40000 ALTER TABLE `profiledata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `projections`
--

DROP TABLE IF EXISTS `projections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `projections` (
  `item` varchar(15) NOT NULL COMMENT 'identifier for parameters with supplied projections',
  `scenario` varchar(15) NOT NULL COMMENT 'identifier for scenario options, Ref and High loosely related to scenarios with reference and high potential for GDP and energy demand according to http://simlab.ethz.ch/1stSemp_sceAssump.htm',
  `year` int NOT NULL COMMENT 'year associated with the value',
  `value` double NOT NULL COMMENT 'values for projections, (gdp, dem_ene, dem_ele, gdpcap, gfac are all indexed to 2010=1), ',
  PRIMARY KEY (`item`,`scenario`,`year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `projections`
--

LOCK TABLES `projections` WRITE;
/*!40000 ALTER TABLE `projections` DISABLE KEYS */;
/*!40000 ALTER TABLE `projections` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarioconfiguration`
--

DROP TABLE IF EXISTS `scenarioconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `scenarioconfiguration` (
  `idScenario` int NOT NULL COMMENT 'primary identifier for scenario configurations',
  `idNetworkConfig` int NOT NULL COMMENT 'primary identifier for network configurations',
  `idLoadConfig` int NOT NULL COMMENT 'primary identifier for load configurations',
  `idGenConfig` int NOT NULL COMMENT 'primary identifier for generator configurations',
  `idMarketsConfig` int NOT NULL COMMENT 'primary identifier for market configurations',
  `idAnnualTargetsConfig` int NOT NULL COMMENT 'primary identifier for swiss annual target/requirement configurations',
  `idDistGenConfig` int NOT NULL COMMENT 'primary identifier for distributed generator configurations',
  `name` varchar(100) DEFAULT NULL COMMENT 'name given to the scenario configuration',
  `runParamDataStructure` json DEFAULT NULL COMMENT 'miscellaneous other information, includes startDate, endDate, colorCodes',
  `Year` int NOT NULL COMMENT 'year associated with the scenario configuration',
  PRIMARY KEY (`idScenario`,`idNetworkConfig`,`idLoadConfig`,`idGenConfig`,`idMarketsConfig`,`idAnnualTargetsConfig`,`idDistGenConfig`,`Year`),
  KEY `fk_scenarioConfiguration_NetworkConfigInfo1_idx` (`idNetworkConfig`),
  KEY `fk_scenarioConfiguration_LoadConfigInfo1_idx` (`idLoadConfig`),
  KEY `fk_scenarioConfiguration_GenConfigInfo1_idx` (`idGenConfig`),
  KEY `fk_scenarioConfiguration_marketsConfiguration1_idx` (`idMarketsConfig`),
  KEY `fk_scenarioConfiguration_RenewTargetConfig1_idx` (`idAnnualTargetsConfig`),
  KEY `fk_scenarioConfiguration_DistGenConfigInfo1_idx` (`idDistGenConfig`),
  CONSTRAINT `fk_scenarioConfiguration_DistGenConfigInfo1` FOREIGN KEY (`idDistGenConfig`) REFERENCES `distgenconfiginfo` (`idDistGenConfig`),
  CONSTRAINT `fk_scenarioConfiguration_GenConfigInfo1` FOREIGN KEY (`idGenConfig`) REFERENCES `genconfiginfo` (`idGenConfig`),
  CONSTRAINT `fk_scenarioConfiguration_LoadConfigInfo1` FOREIGN KEY (`idLoadConfig`) REFERENCES `loadconfiginfo` (`idLoadConfig`),
  CONSTRAINT `fk_scenarioConfiguration_marketsConfiguration1` FOREIGN KEY (`idMarketsConfig`) REFERENCES `marketsconfiguration` (`idMarketsConfig`),
  CONSTRAINT `fk_scenarioConfiguration_NetworkConfigInfo1` FOREIGN KEY (`idNetworkConfig`) REFERENCES `networkconfiginfo` (`idNetworkConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarioconfiguration`
--

LOCK TABLES `scenarioconfiguration` WRITE;
/*!40000 ALTER TABLE `scenarioconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarioconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `securityref`
--

DROP TABLE IF EXISTS `securityref`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `securityref` (
  `DNS_vals` json DEFAULT NULL COMMENT 'Demand not served, in MW, number of entries corresponds to the number of contingencies tested',
  `NLF_vals` json DEFAULT NULL COMMENT 'Number of Line/Transformer failures in a given contingency test, unitless, includes original contingencies and additional failures during cascading outage simulation'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `securityref`
--

LOCK TABLES `securityref` WRITE;
/*!40000 ALTER TABLE `securityref` DISABLE KEYS */;
/*!40000 ALTER TABLE `securityref` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `swiss_annual_targets_configinfo`
--

DROP TABLE IF EXISTS `swiss_annual_targets_configinfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `swiss_annual_targets_configinfo` (
  `idAnnualTargetsConfig` int NOT NULL COMMENT 'primary identifier for swiss annual target/requirement configurations',
  `name` varchar(45) DEFAULT NULL COMMENT 'name given to the annual target/requirement',
  `Year` int DEFAULT NULL COMMENT 'year associated with the target/requirement',
  PRIMARY KEY (`idAnnualTargetsConfig`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `swiss_annual_targets_configinfo`
--

LOCK TABLES `swiss_annual_targets_configinfo` WRITE;
/*!40000 ALTER TABLE `swiss_annual_targets_configinfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `swiss_annual_targets_configinfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `swiss_annual_targets_configuration`
--

DROP TABLE IF EXISTS `swiss_annual_targets_configuration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `swiss_annual_targets_configuration` (
  `idAnnualTargetsConfig` int NOT NULL COMMENT 'primary identifier for swiss annual target/requirement configurations',
  `TargetName` varchar(45) NOT NULL COMMENT 'Name of this target',
  `Year` double DEFAULT NULL COMMENT 'year associated with the target/requirement',
  `Type` varchar(45) DEFAULT '0' COMMENT 'Type of the target, can be ‘Target’ which sets a threshold to exceed, or ‘Requirement’ that sets a value to try to match without going much over',
  `Value` double DEFAULT NULL COMMENT 'Value of the target in the year/config indicated',
  `Units` varchar(45) DEFAULT NULL COMMENT 'Units associated with the annual target/requirement, e.g. TWh-el, tonne-CO2, etc',
  `idProfile` double DEFAULT NULL COMMENT 'identifier for profile that defines targets hourly profile, the profile is normalized by the annual quantity',
  PRIMARY KEY (`idAnnualTargetsConfig`,`TargetName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `swiss_annual_targets_configuration`
--

LOCK TABLES `swiss_annual_targets_configuration` WRITE;
/*!40000 ALTER TABLE `swiss_annual_targets_configuration` DISABLE KEYS */;
/*!40000 ALTER TABLE `swiss_annual_targets_configuration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `transformerconfiguration`
--

DROP TABLE IF EXISTS `transformerconfiguration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `transformerconfiguration` (
  `idNetworkConfig` int NOT NULL COMMENT 'primary identifier for network configurations',
  `idTransformer` int NOT NULL COMMENT 'primary identifier for transformers',
  `TrafoName` varchar(45) NOT NULL COMMENT 'unique name for transformers',
  `idFromBus` int NOT NULL COMMENT 'idBus for FROM side node of a given transformer',
  `idToBus` int NOT NULL COMMENT 'idBus for TO side node of a given transformer',
  `angmin` double DEFAULT '-360' COMMENT 'minimum voltage angle different in degrees',
  `angmax` double DEFAULT '360' COMMENT 'maximum voltage angle different in degrees',
  `status` double DEFAULT NULL COMMENT 'transformer status, 1 = in service, 0 = out of service',
  `FromBusName` varchar(45) DEFAULT NULL COMMENT 'Bus Name for FROM side node of a given transformer',
  `ToBusName` varchar(45) DEFAULT NULL COMMENT 'Bus Name for TO side node of a given transformer',
  `FromCountry` varchar(45) DEFAULT NULL COMMENT 'Country abbreviation for FROM side node of a given transformer',
  `ToCountry` varchar(45) DEFAULT NULL COMMENT 'Country abbreviation for TO side node of a given transformer',
  `Candidate` tinyint DEFAULT NULL COMMENT 'indicator if a given transformer should be considered for investment',
  `CandCost` double DEFAULT NULL COMMENT 'annualized cost to build a candidate transformer, EUR/km/yr',
  PRIMARY KEY (`idNetworkConfig`,`idTransformer`),
  KEY `fk_TransformerConfiguration_BusData2_idx` (`idFromBus`),
  KEY `fk_TransformerConfiguration_TransformerData1_idx` (`idTransformer`),
  KEY `fk_TransformerConfiguration_NetworkconfigInfo1_idx` (`idNetworkConfig`),
  KEY `fk_TransformerConfiguration_BusData1_idx` (`idToBus`),
  CONSTRAINT `fk_TransformerConfiguration_BusData1` FOREIGN KEY (`idToBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_TransformerConfiguration_BusData2` FOREIGN KEY (`idFromBus`) REFERENCES `busdata` (`idBus`),
  CONSTRAINT `fk_TransformerConfiguration_NetworkconfigInfo1` FOREIGN KEY (`idNetworkConfig`) REFERENCES `networkconfiginfo` (`idNetworkConfig`),
  CONSTRAINT `fk_TransformerConfiguration_TransformerData1` FOREIGN KEY (`idTransformer`) REFERENCES `transformerdata` (`idTransformer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `transformerconfiguration`
--

LOCK TABLES `transformerconfiguration` WRITE;
/*!40000 ALTER TABLE `transformerconfiguration` DISABLE KEYS */;
/*!40000 ALTER TABLE `transformerconfiguration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `transformerdata`
--

DROP TABLE IF EXISTS `transformerdata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `transformerdata` (
  `idTransformer` int NOT NULL COMMENT 'primary identifier for transformers',
  `TrafoName` varchar(45) NOT NULL COMMENT 'unique name for transformers',
  `line_type` varchar(45) DEFAULT NULL,
  `loss_factor` double DEFAULT NULL,
  `r` double DEFAULT NULL COMMENT 'transformer resistance in p.u., all transformer parameters are defined in p.u. based on voltage of the TO (secondary) side node',
  `x` double DEFAULT NULL COMMENT 'transformer reactance in p.u., all transformer parameters are defined in p.u. based on voltage of the TO (secondary) side node',
  `b` double DEFAULT NULL COMMENT 'transformer susceptance in p.u., all transformer parameters are defined in p.u. based on voltage of the TO (secondary) side node',
  `rateA` double DEFAULT '0' COMMENT 'transformer rating in MVA, nominal rating',
  `rateA2` double DEFAULT NULL,
  `rateB` double DEFAULT '0' COMMENT 'transformer rating in MVA, short term rating',
  `rateC` double DEFAULT '0' COMMENT 'transformer rating in MVA, emergency rating',
  `tapRatio` double DEFAULT '1' COMMENT 'transformer tap ratio, unitless',
  `angle` double DEFAULT '0' COMMENT 'transformer phase shift angle in degrees',
  `StartYr` double DEFAULT NULL COMMENT 'first year a transformer should be included in the network configuration',
  `EndYr` double DEFAULT NULL COMMENT 'last year a transformer should be included in the network configuration',
  `MVA_Winter` double DEFAULT NULL COMMENT 'transformer rating in MVA, applicable in winter',
  `MVA_Summer` double DEFAULT NULL COMMENT 'transformer rating in MVA, applicable in summer',
  `MVA_SprFall` double DEFAULT NULL COMMENT 'transformer rating in MVA, applicable in spring and fall',
  `length` double DEFAULT NULL COMMENT 'transformer length in km',
  PRIMARY KEY (`idTransformer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `transformerdata`
--

LOCK TABLES `transformerdata` WRITE;
/*!40000 ALTER TABLE `transformerdata` DISABLE KEYS */;
/*!40000 ALTER TABLE `transformerdata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `workforce`
--

DROP TABLE IF EXISTS `workforce`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `workforce` (
  `popscen` varchar(45) NOT NULL COMMENT 'identifier for population projection scenario, scenarios A, B, C-00-2015 represent scenarios developed by BFS for population projections from 2015 to 2050',
  `year` int NOT NULL COMMENT 'year associated with the value',
  `value` double NOT NULL COMMENT 'values represent head counts in full time equivalents',
  PRIMARY KEY (`popscen`,`year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `workforce`
--

LOCK TABLES `workforce` WRITE;
/*!40000 ALTER TABLE `workforce` DISABLE KEYS */;
/*!40000 ALTER TABLE `workforce` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'z_setup_schema9_v1'
--
/*!50003 DROP PROCEDURE IF EXISTS `Ex_deleteGeneratorConfig` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `Ex_deleteGeneratorConfig`(IN configId Int)
BEGIN
DELETE gendata,genconfiguration
FROM gendata
INNER JOIN genconfiguration ON gendata.idGen = genconfiguration.idGen where genconfiguration.idGenConfig = configId;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `Ex_deleteNetworkConfig` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `Ex_deleteNetworkConfig`(IN configId Int)
BEGIN
DELETE linedata,lineconfiguration
FROM linedata
INNER JOIN lineconfiguration ON linedata.idLine =lineconfiguration.idLine where lineconfiguration.idNetworkConfig = configId;

DELETE transformerdata,transformerconfiguration
FROM transformerdata
INNER JOIN transformerconfiguration ON transformerdata.idTransformer = transformerconfiguration.idTransformer where transformerconfiguration.idNetworkConfig = configId;

DELETE busdata,busconfiguration
FROM busdata
INNER JOIN busconfiguration ON busdata.idBus = busconfiguration.idBus where busconfiguration.idNetworkConfig = configId;

DELETE FROM networkconfigInfo where networkconfiginfo.idNetworkConfig = configId;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `Ex_setGenParamsUsingSQL` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `Ex_setGenParamsUsingSQL`(in genconfig INT)
BEGIN

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '80' where gendata.Technology= 'Biomass' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '8' where gendata.Technology= 'Nuclear' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '2' where gendata.Technology= 'RoR' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '2' where gendata.Technology= 'PV' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '70' where gendata.Technology= 'Oil' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '1' where gendata.Technology= 'Dam' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '1' where gendata.Technology= 'Pump' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '30' where gendata.Technology= 'Lignite' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '100' where gendata.Technology= 'GasCC' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '100' where gendata.Technology= 'GasSC' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.VarCost = '60' where gendata.Technology= 'Coal' and genconfiguration.idGenConfig = genconfig;


Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.HedgeRatio = '0.6'
where gendata.UnitType = 'Dispatchable' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.HedgeRatio = '-1'
where gendata.UnitType = 'NonDispatchable' and genconfiguration.idGenConfig = genconfig;

Update gendata
inner join genconfiguration on genconfiguration.idGen = gendata.idGen
set gendata.meanErrorForecast24h = '0',gendata.sigmaErrorForecast24h = '0'
where genconfiguration.idGenConfig = genconfig;



Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
set genconfiguration.Emax =  genconfiguration.Pmax *1000, genconfiguration.E_ini = '0.2'
where gendata.Technology = 'Dam' and genconfiguration.idGenConfig = genconfig;


Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
set genconfiguration.Emax = genconfiguration.Pmax *100, genconfiguration.E_ini = '0.2'
where gendata.Technology = 'Pump' and genconfiguration.idGenConfig = genconfig;


Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '18'
where gendata.Technology = 'Pump' and busdata.ZoneId = '1' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '17'
where gendata.Technology = 'Dam' and busdata.ZoneId = '1' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '16'
where gendata.Technology = 'RoR' and busdata.ZoneId = '1' and genconfiguration.idGenConfig = genconfig;



Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '18'
where gendata.Technology = 'Pump' and busdata.ZoneId = '2' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '17'
where gendata.Technology = 'Dam' and busdata.ZoneId = '2' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '16'
where gendata.Technology = 'RoR' and busdata.ZoneId = '2' and genconfiguration.idGenConfig = genconfig;



Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '18'
where gendata.Technology = 'Pump' and busdata.ZoneId = '3' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '17'
where gendata.Technology = 'Dam' and busdata.ZoneId = '3' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '16'
where gendata.Technology = 'RoR' and busdata.ZoneId = '3' and genconfiguration.idGenConfig = genconfig;


Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '18'
where gendata.Technology = 'Pump' and busdata.ZoneId = '5' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '17'
where gendata.Technology = 'Dam' and busdata.ZoneId = '5' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '16'
where gendata.Technology = 'RoR' and busdata.ZoneId = '5' and genconfiguration.idGenConfig = genconfig;


Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '18'
where gendata.Technology = 'Pump' and busdata.ZoneId = '4' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '17'
where gendata.Technology = 'Dam' and busdata.ZoneId = '4' and genconfiguration.idGenConfig = genconfig;

Update genconfiguration
inner join gendata on gendata.idGen = genconfiguration.idGen 
inner join busdata on busdata.idBus = genconfiguration.idBus
set genconfiguration.idProfile = '16'
where gendata.Technology = 'RoR' and busdata.ZoneId = '4' and genconfiguration.idGenConfig = genconfig;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getBranchData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getBranchData`(in config INT)
BEGIN

SELECT  (SELECT busdata.internalBusId
		FROM busdata
		where lineconfiguration.idFromBus = busdata.idBus ) as idFromBus,
		(SELECT busdata.internalBusId
		FROM busdata
		where lineconfiguration.idToBus = busdata.idBus ) as idToBus,
        linedata.LineName,
        lineconfiguration.FromBusName,
        lineconfiguration.ToBusName,
        lineconfiguration.FromCountry,
        lineconfiguration.ToCountry,
        linedata.kV,
        linedata.r, 
        linedata.x,
        linedata.b, 
        linedata.rateA,
        linedata.rateB,
        linedata.rateC,
        lineconfiguration.status,
        lineconfiguration.angmin,
        lineconfiguration.angmax,
        linedata.StartYr,
        linedata.EndYr,
        linedata.MVA_Winter,
        linedata.MVA_Summer,
        linedata.MVA_SprFall,
        lineconfiguration.Ind_CrossBord,
        lineconfiguration.Ind_Agg,
        lineconfiguration.Ind_HVDC,
        lineconfiguration.Candidate,
        lineconfiguration.CandCost,
        linedata.length,
        linedata.line_type,
        linedata.loss_factor,
        linedata.rateA2
FROM linedata
INNER JOIN lineconfiguration ON linedata.idLine =lineconfiguration.idLine where lineconfiguration.idNetworkConfig = config;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getBusData_v2` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getBusData_v2`(in config int)
BEGIN
SELECT  busdata.idBus as idIntBus,
		busdata.BusName,
        busdata.SwissgridNodeCode,
        busdata.Country,
        busdata.SubRegion,
        busdata.ZoneId,
		busdata.BusType,
        busdata.Pd,
        busdata.Qd,
        busdata.Bs,
        busdata.Gs,
        busdata.baseKV,
        busconfiguration.Vmin,
        busconfiguration.Vmax,
        busdata.StartYr,
        busdata.EndYr,
        (SELECT loadconfiguration.DemandShare
		FROM loadconfiguration
        
		where busdata.idBus = loadconfiguration.idBus AND loadconfiguration.idLoadConfig = scenarioconfiguration.idLoadConfig) as DemandShare,
        (SELECT busconfiguration.WindShare
		FROM busconfiguration
		
        where busdata.idBus = busconfiguration.idBus AND busconfiguration.idNetworkConfig = scenarioconfiguration.idNetworkConfig) as WindShare,
        
        (SELECT busconfiguration.SolarShare
		FROM busconfiguration
		
        where busdata.idBus = busconfiguration.idBus AND busconfiguration.idNetworkConfig = scenarioconfiguration.idNetworkConfig) as SolarShare,
        
        (SELECT busconfiguration.idDistProfile
		FROM busconfiguration
        where busdata.idBus = busconfiguration.idBus AND busconfiguration.idNetworkConfig = scenarioconfiguration.idNetworkConfig) as idDistProfile,
        busdata.X_Coord,
        busdata.Y_Coord
FROM busdata
INNER JOIN busconfiguration ON busdata.idBus = busconfiguration.idBus 
INNER JOIN scenarioconfiguration ON busconfiguration.idNetworkConfig = scenarioconfiguration.idNetworkConfig  WHERE scenarioconfiguration.idScenario = config;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getDistGeneratorData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getDistGeneratorData`(in config INT)
BEGIN
SELECT 	distgendata.idDistGen,
		distgendata.GenName,
        distgendata.GenType,
        distgendata.Technology,
        distgendata.UnitType,
        distgendata.Type,
        distgendata.CandidateUnit,
        distgendata.InvestmentType,
        distgendata.min_Size_kW,
        distgendata.Pmax_kW,
        distgendata.Pmin_kW,
        distgendata.Dischrg_max,
        distgendata.Chrg_max,
        distgendata.eta_dis,
        distgendata.eta_ch,
        distgendata.Self_dischrg,
        distgendata.Emax,
        distgendata.Emin,
        distgendata.Emax_kWh,
        distgendata.E_ini,
        distgendata.E_final,
        distgendata.Pini,
        distgendata.RU,
        distgendata.RD,
        distgendata.Lifetime,
        distgendata.GenEffic,
        distgendata.ThmlEffic,
        distgendata.CapFactor,
        distgendata.CO2Rate,
        distgendata.FuelType,
        distgendata.ElecOwnUseFactor,
		distgenconfiguration.InvCost_P,
        distgenconfiguration.InvCost_E,
        distgenconfiguration.FOM_Cost,
        distgenconfiguration.VOM_Cost,
        distgenconfiguration.Fuel_Cost,
        distgenconfiguration.Heat_Credit,
        distgenconfiguration.KEV,
        distgenconfiguration.WACC,
        distgenconfiguration.LCOE,
        distgenconfiguration.Heat_Value
FROM distgendata
INNER JOIN distgenconfiguration ON distgendata.idDistGen =distgenconfiguration.idDistGen where distgenconfiguration.idDistGenConfig = config;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getGeneratorData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getGeneratorData`(in config INT)
BEGIN

SELECT  (SELECT busdata.internalBusId
		FROM busdata
		where genconfiguration.idBus = busdata.idBus ) as idIntBus,
        gendata.idGen,
        genconfiguration.GenName,
        (SELECT busdata.BusName
        FROM busdata
		where genconfiguration.idBus = busdata.idBus ) as BusName,
        (SELECT busdata.Country
        FROM busdata
		where genconfiguration.idBus = busdata.idBus ) as Country,
        gendata.GenType,
        gendata.Technology,
        gendata.UnitType,
        gendata.GenEffic,
        gendata.CO2Rate,
        genconfiguration.FuelType,
        genconfiguration.VOM_Cost,
        genconfiguration.FOM_Cost,
        genconfiguration.InvCost,
        genconfiguration.InvCost_E,
        genconfiguration.InvCost_Charge,
        genconfiguration.StartCost,
        genconfiguration.TotVarCost,
        genconfiguration.Pmax,
        genconfiguration.Pmin,
        genconfiguration.Qmax,
        genconfiguration.Qmin,
        genconfiguration.status,
        gendata.RU,
        gendata.RD,
        gendata.RU_start,
        gendata.RD_shutd,
        gendata.UT,
        gendata.DT,
        gendata.Pini,
        gendata.Tini,
        gendata.StartYr,
        gendata.EndYr,
        genconfiguration.CandidateUnit,
        genconfiguration.HedgeRatio,
        gendata.meanErrorForecast24h,
        gendata.sigmaErrorForecast24h,
        genconfiguration.idProfile,
        genconfiguration.Emax,
        genconfiguration.Emin,
        genconfiguration.E_ini,
        gendata.eta_dis,
        gendata.eta_ch,
        gendata.Lifetime,
        (SELECT busdata.SubRegion
        FROM busdata
		where genconfiguration.idBus = busdata.idBus ) as SubRegion,
        (SELECT busdata.X_Coord
        FROM busdata
		where genconfiguration.idBus = busdata.idBus ) as Bus_X_Coord,
        (SELECT busdata.Y_Coord
        FROM busdata
		where genconfiguration.idBus = busdata.idBus ) as Bus_Y_Coord,
        (SELECT fuelprices.price
        FROM fuelprices
		where genconfiguration.FuelType = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as FuelPrice,
        (SELECT fuelprices.price_mult_idProfile
        FROM fuelprices
		where genconfiguration.FuelType = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as FuelPrice_mult_idProfile,
        (SELECT fuelprices.price
        FROM fuelprices
		where genconfiguration.CO2Type = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as CO2Price,
        (SELECT fuelprices.price_mult_idProfile
        FROM fuelprices
		where genconfiguration.CO2Type = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as CO2Price_mult_idProfile
FROM gendata
INNER JOIN genconfiguration ON gendata.idGen =genconfiguration.idGen 
INNER JOIN genconfiginfo on genconfiguration.idGenConfig = genconfiginfo.idGenConfig
where genconfiguration.idGenConfig = config;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getGeneratorData_Extra` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getGeneratorData_Extra`(in config INT)
BEGIN

SELECT  (SELECT busdata.internalBusId
		FROM busdata
		where genconfiguration_extra.idBus = busdata.idBus ) as idIntBus,
        genconfiguration_extra.idGen,
        genconfiguration_extra.GenName,
        (SELECT busdata.BusName
        FROM busdata
		where genconfiguration_extra.idBus = busdata.idBus ) as BusName,
        (SELECT busdata.Country
        FROM busdata
		where genconfiguration_extra.idBus = busdata.idBus ) as Country,
        genconfiguration_extra.GenType,
        genconfiguration_extra.Technology,
        genconfiguration_extra.UnitType,
        genconfiguration_extra.Pmax_methdac,
        genconfiguration_extra.Pmin_methdac,
        genconfiguration_extra.Emax_h2stor,
        genconfiguration_extra.Emin_h2stor,
        genconfiguration_extra.VOM_methdac,
        genconfiguration_extra.InvCost_h2stor,
        genconfiguration_extra.InvCost_methdac,
        genconfiguration_extra.FOM_elzr,
        genconfiguration_extra.FOM_h2stor,
        genconfiguration_extra.FOM_methdac,
        genconfiguration_extra.Conv_elzr,
        genconfiguration_extra.Conv_fc,
        genconfiguration_extra.Conv_methdac_h2,
        genconfiguration_extra.Conv_methdac_el,
        genconfiguration_extra.Conv_methdac_co2,
        genconfiguration_extra.MaxInjRate_h2Stor,
        genconfiguration_extra.MaxWithRate_h2Stor,
        genconfiguration_extra.FuelType_methdac,
        genconfiguration_extra.Ind_h2_MarketConnect,
        (SELECT busdata.SubRegion
        FROM busdata
		where genconfiguration_extra.idBus = busdata.idBus ) as SubRegion,
        (SELECT busdata.X_Coord
        FROM busdata
		where genconfiguration_extra.idBus = busdata.idBus ) as Bus_X_Coord,
        (SELECT busdata.Y_Coord
        FROM busdata
		where genconfiguration_extra.idBus = busdata.idBus ) as Bus_Y_Coord,
        (SELECT fuelprices.price
        FROM fuelprices
		where genconfiguration_extra.FuelType_methdac = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as FuelPrice_sell,
        (SELECT fuelprices.price_mult_idProfile
        FROM fuelprices
		where genconfiguration_extra.FuelType_methdac = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as FuelPrice_sell_mult_idProfile,
        (SELECT fuelprices.price
        FROM fuelprices
		where genconfiguration_extra.FuelType_ch4_import = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as CH4Price_import,
        (SELECT fuelprices.price_mult_idProfile
        FROM fuelprices
		where genconfiguration_extra.FuelType_ch4_import = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as CH4Price_import_mult_idProfile,
        (SELECT fuelprices.price
        FROM fuelprices
		where genconfiguration_extra.FuelType_h2_domestic = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as H2Price_sell,
        (SELECT fuelprices.price_mult_idProfile
        FROM fuelprices
		where genconfiguration_extra.FuelType_h2_domestic = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as H2Price_sell_mult_idProfile,
        (SELECT fuelprices.price
        FROM fuelprices
		where genconfiguration_extra.FuelType_h2_import = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as H2Price_import,
        (SELECT fuelprices.price_mult_idProfile
        FROM fuelprices
		where genconfiguration_extra.FuelType_h2_import = fuelprices.fuel AND fuelprices.Year = genconfiginfo.year) as H2Price_import_mult_idProfile,
        genconfiguration_extra.h2Stor_Type,
        genconfiguration_extra.ElecGen_Type
FROM gendata
INNER JOIN genconfiguration_extra ON gendata.idGen =genconfiguration_extra.idGen 
INNER JOIN genconfiginfo on genconfiguration_extra.idGenConfig = genconfiginfo.idGenConfig
where genconfiguration_extra.idGenConfig = config;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getLoadData_v2` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getLoadData_v2`(in config INT)
BEGIN

SELECT  (SELECT busdata.idBus
		FROM busdata
		where loadconfiguration.idBus = busdata.idBus ) as idIntBus,
        loaddata.idLoad,
        loadconfiguration.idProfile,
        (SELECT busdata.Country
				FROM busdata
				WHERE loadconfiguration.idBus = busdata.idBus ) AS Country,
        loaddata.hedgeRatio,
        loaddata.meanForecastError24h,
        loaddata.sigmaForecastError24h,
		loadconfiguration.DemandShare,
        loaddata.Pd,
        loaddata.Qd,
        loadconfiguration.idProfile_eMobility,
        loadconfiguration.idProfile_eHeatPump,
        loadconfiguration.idProfile_eHydrogen,
        (SELECT centflexpotential.PowerShift_Hrly
                FROM busdata
				INNER JOIN centflexpotential on centflexpotential.Country = busdata.Country
				WHERE loadconfiguration.idBus = busdata.idBus AND centflexpotential.flex_type = 'DSM_general' AND centflexpotential.Year = loadconfiginfo.year ) AS DSM_PowerShift_Hrly,
		(SELECT centflexpotential.EnergyShift_Daily
                FROM busdata
				INNER JOIN centflexpotential on centflexpotential.Country = busdata.Country
				WHERE loadconfiguration.idBus = busdata.idBus AND centflexpotential.flex_type = 'DSM_general' AND centflexpotential.Year = loadconfiginfo.year ) AS DSM_EnergyShift_Daily,
		(SELECT centflexpotential.PowerShift_Hrly
                FROM busdata
				INNER JOIN centflexpotential on centflexpotential.Country = busdata.Country
				WHERE loadconfiguration.idBus = busdata.idBus AND centflexpotential.flex_type = 'emobility' AND centflexpotential.Year = loadconfiginfo.year ) AS emobility_PowerShift_Hrly,
		(SELECT centflexpotential.EnergyShift_Daily
                FROM busdata
				INNER JOIN centflexpotential on centflexpotential.Country = busdata.Country
				WHERE loadconfiguration.idBus = busdata.idBus AND centflexpotential.flex_type = 'emobility' AND centflexpotential.Year = loadconfiginfo.year ) AS emobility_EnergyShift_Daily
FROM loaddata
INNER JOIN loadconfiguration ON loaddata.idLoad =loadconfiguration.idLoad 
INNER JOIN loadconfiginfo on loadconfiguration.idLoadConfig = loadconfiginfo.idLoadConfig
WHERE loadconfiguration.idLoadConfig = config;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getSwissAnnualTargets` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getSwissAnnualTargets`(in config INT)
BEGIN

SELECT  (SELECT swiss_annual_targets_configinfo.name
		FROM swiss_annual_targets_configinfo
		where swiss_annual_targets_configuration.idAnnualTargetsConfig = swiss_annual_targets_configinfo.idAnnualTargetsConfig ) as TargetsConfigName,
		swiss_annual_targets_configuration.TargetName,
		swiss_annual_targets_configuration.Year,
        swiss_annual_targets_configuration.Type,
        swiss_annual_targets_configuration.Value,
        swiss_annual_targets_configuration.Units,
        swiss_annual_targets_configuration.idProfile
FROM swiss_annual_targets_configuration
INNER JOIN swiss_annual_targets_configinfo on swiss_annual_targets_configuration.idAnnualTargetsConfig = swiss_annual_targets_configinfo.idAnnualTargetsConfig
WHERE swiss_annual_targets_configuration.idAnnualTargetsConfig = config;
        
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `getTransformerData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `getTransformerData`(in config INT)
BEGIN
SELECT  (SELECT busdata.internalBusId
		FROM busdata
		where  transformerconfiguration.idFromBus = busdata.idBus ) as idFromBus,
		(SELECT busdata.internalBusId
		FROM busdata
		where transformerconfiguration.idToBus = busdata.idBus ) as idToBus,
		transformerdata.TrafoName,
        transformerconfiguration.FromBusName,
        transformerconfiguration.ToBusName,
        transformerconfiguration.FromCountry,
        transformerconfiguration.ToCountry,
        transformerdata.r, 
        transformerdata.x,
        transformerdata.b,
        transformerdata.rateA,
        transformerdata.rateB,
        transformerdata.rateC,
        transformerdata.tapRatio,
        transformerdata.angle,
        transformerconfiguration.status,
        transformerconfiguration.angmin,
        transformerconfiguration.angmax,
        transformerdata.StartYr,
        transformerdata.EndYr,
        transformerdata.MVA_Winter,
        transformerdata.MVA_Summer,
        transformerdata.MVA_SprFall,
        transformerconfiguration.Candidate,
        transformerconfiguration.CandCost,
        transformerdata.length,
        transformerdata.line_type,
        transformerdata.loss_factor,
        transformerdata.rateA2
FROM transformerdata
INNER JOIN transformerconfiguration ON transformerdata.idTransformer = transformerconfiguration.idTransformer where transformerconfiguration.idNetworkConfig = config;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `Old_getBusData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `Old_getBusData`(in config int)
BEGIN
SELECT  busdata.internalBusId,
		busdata.BusName,
        busdata.SwissgridNodeCode,
        busdata.Country,
        busdata.SubRegion,
        busdata.ZoneId,
		busdata.BusType,
        busdata.Pd,
        busdata.Qd,
        busdata.Bs,
        busdata.Gs,
        busdata.baseKV,
        busconfiguration.Vmin,
        busconfiguration.Vmax,
        busdata.StartYr,
        busdata.EndYr,
        (SELECT loadconfiguration.loadFactor
		FROM loadconfiguration
		where busdata.idBus = loadconfiguration.idBus ) as DemandShare,
        (SELECT busconfiguration.WindShare
		FROM busconfiguration
		where busdata.idBus = busconfiguration.idBus ) as WindShare,
        
        (SELECT busconfiguration.SolarShare
		FROM busconfiguration
		where busdata.idBus = busconfiguration.idBus ) as SolarShare
        
FROM busdata
INNER JOIN busconfiguration ON busdata.idBus = busconfiguration.idBus where busconfiguration.idNetworkConfig = config;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `Old_getLoadData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`nexuseroutines`@`localhost` PROCEDURE `Old_getLoadData`(in config INT)
BEGIN

SELECT  (SELECT busdata.internalBusId
		FROM busdata
		where loadconfiguration.idBus = busdata.idBus ) as idIntBus,
        loaddata.idLoad,
        loadconfiguration.idProfile,
        loaddata.hedgeRatio,
        loaddata.meanForecastError24h,
        loaddata.sigmaForecastError24h,
		loadconfiguration.loadFactor,
        loaddata.Pd,
        loaddata.Qd,
        loadconfiguration.idProfile_eMobility,
        loadconfiguration.idProfile_eHeatPump,
        loadconfiguration.idProfile_eHydrogen
FROM loaddata
INNER JOIN loadconfiguration ON loaddata.idLoad =loadconfiguration.idLoad where loadconfiguration.idLoadConfig = config;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-02-07 12:07:21
