from dataclasses import dataclass, replace
import logging
from typing import Literal
from sqlalchemy import create_engine, select, ScalarResult, Engine
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, create_database

from .tables import (
    BusConfiguration,
    BusData,
    CentFlexPotential,
    DBInfo,
    DistABGenCosts,
    DistFlexPotential,
    DistGenConfigInfo,
    DistGenConfiguration,
    DistGenData,
    DistProfiles,
    DistRegionByGenTypeData,
    DistRegionByIrradLevelData,
    DistRegionData,
    FlexParamsHP,
    FlexProfilesEV,
    FlexProfilesHP,
    FuelPrices,
    GenConfigInfo,
    GenConfiguration,
    GenConfigurationExtra,
    GenData,
    GenTypeData,
    LineConfiguration,
    LineData,
    LoadProfiles,
    LoadConfigInfo,
    LoadConfiguration,
    LoadData,
    MarketsConfiguration,
    NetworkConfigInfo,
    ProfileData,
    Projections,
    ScenarioConfiguration,
    SecurityRef,
    SwissAnnualTargetsConfigInfo,
    SwissAnnualTargetsConfiguration,
    TransformerConfiguration,
    TransformerData,
    Workforce,
    Base,
)

@dataclass
class DataContext():
    type: Literal["mysql", "sqlite"] = "mysql"
    name: str = ""
    host: str = ""
    port: str = ""
    user: str = ""
    password: str = ""

class Scenario:
    def __init__(self, data_context: DataContext):
        """Initialize the Scenario repository with an injected data context."""
        self.__data_context = data_context

    def __get_table(self, table_class) -> ScalarResult:
        """Helper method to query a specific table."""
        with self.__new_session as session:
            return [row.__dict__ for row in session.scalars(select(table_class)).all()]
        
    @property
    def __new_session(self) -> Session:
        return Session(self.__create_engine())
    
    def __create_engine(self) -> Engine:
        """Return an active session to interact with sql databases"""
        if self.__data_context.type == "mysql":
            output = create_engine(
                "mysql+pymysql://"
                f"{self.__data_context.user}"
                f":{self.__data_context.password}"
                f"@{self.__data_context.host}"
                f":{self.__data_context.port}"
                f"/{self.__data_context.name}"
            )
        elif self.__data_context.type == "sqlite":
            output = create_engine(f"sqlite:///{self.__data_context.name}")
        return output
    
    def execute(self, statement) -> ScalarResult:
        """
        Execute a SQLAlchemy statement on the SQL database given by DataContext
        at class creation.
        """
        with self.__new_session as session, session.begin():
            # Probably vulnerable to SQL injection
            return session.execute(statement).scalars()
    
    def get_data_context(self) -> DataContext:
        logging.warning((
            "The direct use of data context is discouraged and will be "
            "deprecated. Please consider using Scenario.execute() with "
            "SQLAlchemy statements instead."
        ))
        return replace(self.__data_context)
    
    def create_new_database(self) -> None:
        if database_exists(self.__create_engine().url):
            logging.warning(
                f"Database {self.__data_context.name} already exists."
            )
            return
        create_database(self.__create_engine().url)
        Base.metadata.create_all(self.__create_engine())


    # Properties for each table
    @property
    def busconfiguration(self) -> ScalarResult:
        return self.__get_table(BusConfiguration)

    @property
    def busdata(self) -> ScalarResult:
        return self.__get_table(BusData)

    @property
    def centflexpotential(self) -> ScalarResult:
        return self.__get_table(CentFlexPotential)

    @property
    def dbinfo(self) -> ScalarResult:
        return self.__get_table(DBInfo)

    @property
    def distabgencosts(self) -> ScalarResult:
        return self.__get_table(DistABGenCosts)

    @property
    def distflexpotential(self) -> ScalarResult:
        return self.__get_table(DistFlexPotential)

    @property
    def distgenconfiginfo(self) -> ScalarResult:
        return self.__get_table(DistGenConfigInfo)

    @property
    def distgenconfiguration(self) -> ScalarResult:
        return self.__get_table(DistGenConfiguration)

    @property
    def distgendata(self) -> ScalarResult:
        return self.__get_table(DistGenData)

    @property
    def distprofiles(self) -> ScalarResult:
        return self.__get_table(DistProfiles)

    @property
    def distregionbygentypedata(self) -> ScalarResult:
        return self.__get_table(DistRegionByGenTypeData)

    @property
    def distregionbyirradleveldata(self) -> ScalarResult:
        return self.__get_table(DistRegionByIrradLevelData)

    @property
    def distregiondata(self) -> ScalarResult:
        return self.__get_table(DistRegionData)

    @property
    def flex_params_hp(self) -> ScalarResult:
        return self.__get_table(FlexParamsHP)

    @property
    def flex_profiles_ev(self) -> ScalarResult:
        return self.__get_table(FlexProfilesEV)

    @property
    def flex_profiles_hp(self) -> ScalarResult:
        return self.__get_table(FlexProfilesHP)

    @property
    def fuelprices(self) -> ScalarResult:
        return self.__get_table(FuelPrices)

    @property
    def genconfiginfo(self) -> ScalarResult:
        return self.__get_table(GenConfigInfo)

    @property
    def genconfiguration(self) -> ScalarResult:
        return self.__get_table(GenConfiguration)

    @property
    def genconfiguration_extra(self) -> ScalarResult:
        return self.__get_table(GenConfigurationExtra)

    @property
    def gendata(self) -> ScalarResult:
        return self.__get_table(GenData)

    @property
    def gentypedata(self) -> ScalarResult:
        return self.__get_table(GenTypeData)

    @property
    def lineconfiguration(self) -> ScalarResult:
        return self.__get_table(LineConfiguration)

    @property
    def linedata(self) -> ScalarResult:
        return self.__get_table(LineData)

    @property
    def load_profiles(self) -> ScalarResult:
        return self.__get_table(LoadProfiles)

    @property
    def loadconfiginfo(self) -> ScalarResult:
        return self.__get_table(LoadConfigInfo)

    @property
    def loadconfiguration(self) -> ScalarResult:
        return self.__get_table(LoadConfiguration)

    @property
    def loaddata(self) -> ScalarResult:
        return self.__get_table(LoadData)

    @property
    def marketsconfiguration(self) -> ScalarResult:
        return self.__get_table(MarketsConfiguration)

    @property
    def networkconfiginfo(self) -> ScalarResult:
        return self.__get_table(NetworkConfigInfo)

    @property
    def profiledata(self) -> ScalarResult:
        return self.__get_table(ProfileData)

    @property
    def projections(self) -> ScalarResult:
        return self.__get_table(Projections)

    @property
    def scenarioconfiguration(self) -> ScalarResult:
        return self.__get_table(ScenarioConfiguration)

    @property
    def securityref(self) -> ScalarResult:
        if not self.__new_session:
            raise RuntimeError("Session is not active. Provide a valid session.")
        output = {
            "DNS_vals": self.__new_session.scalars(select(SecurityRef.DNS_vals)).first(),
            "NLF_vals": self.__new_session.scalars(select(SecurityRef.NLF_vals)).first(),
        }
        return [output]

    @property
    def swiss_annual_targets_configinfo(self) -> ScalarResult:
        return self.__get_table(SwissAnnualTargetsConfigInfo)

    @property
    def swiss_annual_targets_configuration(self) -> ScalarResult:
        return self.__get_table(SwissAnnualTargetsConfiguration)

    @property
    def transformerconfiguration(self) -> ScalarResult:
        return self.__get_table(TransformerConfiguration)

    @property
    def transformerdata(self) -> ScalarResult:
        return self.__get_table(TransformerData)

    @property
    def workforce(self) -> ScalarResult:
        return self.__get_table(Workforce)
