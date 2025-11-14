from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.engine import ScalarResult
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
)


class Scenario:
    def __init__(self, session: Session):
        """Initialize the Scenario repository with an injected session."""
        self.__session: Session = session

    def __get_table(self, table_class) -> ScalarResult:
        """Helper method to query a specific table."""
        if not self.__session:
            raise RuntimeError("Session is not active. Provide a valid session.")
        return [row.__dict__ for row in self.__session.scalars(select(table_class)).all()]

    # Properties for each table
    @property
    def bus_configurations(self) -> ScalarResult:
        return self.__get_table(BusConfiguration)

    @property
    def bus_data(self) -> ScalarResult:
        return self.__get_table(BusData)

    @property
    def cent_flex_potential(self) -> ScalarResult:
        return self.__get_table(CentFlexPotential)

    @property
    def db_info(self) -> ScalarResult:
        return self.__get_table(DBInfo)

    @property
    def dist_ab_gen_costs(self) -> ScalarResult:
        return self.__get_table(DistABGenCosts)

    @property
    def dist_flex_potential(self) -> ScalarResult:
        return self.__get_table(DistFlexPotential)

    @property
    def dist_gen_config_info(self) -> ScalarResult:
        return self.__get_table(DistGenConfigInfo)

    @property
    def dist_gen_configuration(self) -> ScalarResult:
        return self.__get_table(DistGenConfiguration)

    @property
    def dist_gen_data(self) -> ScalarResult:
        return self.__get_table(DistGenData)

    @property
    def dist_profiles(self) -> ScalarResult:
        return self.__get_table(DistProfiles)

    @property
    def dist_region_by_gen_type_data(self) -> ScalarResult:
        return self.__get_table(DistRegionByGenTypeData)

    @property
    def dist_region_by_irrad_level_data(self) -> ScalarResult:
        return self.__get_table(DistRegionByIrradLevelData)

    @property
    def dist_region_data(self) -> ScalarResult:
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
    def fuel_prices(self) -> ScalarResult:
        return self.__get_table(FuelPrices)

    @property
    def gen_config_info(self) -> ScalarResult:
        return self.__get_table(GenConfigInfo)

    @property
    def gen_configuration(self) -> ScalarResult:
        return self.__get_table(GenConfiguration)

    @property
    def gen_configuration_extra(self) -> ScalarResult:
        return self.__get_table(GenConfigurationExtra)

    @property
    def gen_data(self) -> ScalarResult:
        return self.__get_table(GenData)

    @property
    def gen_type_data(self) -> ScalarResult:
        return self.__get_table(GenTypeData)

    @property
    def line_configuration(self) -> ScalarResult:
        return self.__get_table(LineConfiguration)

    @property
    def line_data(self) -> ScalarResult:
        return self.__get_table(LineData)

    @property
    def load_profiles(self) -> ScalarResult:
        return self.__get_table(LoadProfiles)

    @property
    def load_config_info(self) -> ScalarResult:
        return self.__get_table(LoadConfigInfo)

    @property
    def load_configuration(self) -> ScalarResult:
        return self.__get_table(LoadConfiguration)

    @property
    def load_data(self) -> ScalarResult:
        return self.__get_table(LoadData)

    @property
    def markets_configuration(self) -> ScalarResult:
        return self.__get_table(MarketsConfiguration)

    @property
    def network_config_info(self) -> ScalarResult:
        return self.__get_table(NetworkConfigInfo)

    @property
    def profile_data(self) -> ScalarResult:
        return self.__get_table(ProfileData)

    @property
    def projections(self) -> ScalarResult:
        return self.__get_table(Projections)

    @property
    def scenario_configuration(self) -> ScalarResult:
        return self.__get_table(ScenarioConfiguration)

    @property
    def security_ref(self) -> ScalarResult:
        if not self.__session:
            raise RuntimeError("Session is not active. Provide a valid session.")
        output = {
            "DNS_vals": self.__session.scalars(select(SecurityRef.DNS_vals)).first(),
            "NLF_vals": self.__session.scalars(select(SecurityRef.NLF_vals)).first(),
        }
        print({key: len(value) for key, value in output.items()})
        return [output]

    @property
    def swiss_annual_targets_config_info(self) -> ScalarResult:
        return self.__get_table(SwissAnnualTargetsConfigInfo)

    @property
    def swiss_annual_targets_configuration(self) -> ScalarResult:
        return self.__get_table(SwissAnnualTargetsConfiguration)

    @property
    def transformer_configuration(self) -> ScalarResult:
        return self.__get_table(TransformerConfiguration)

    @property
    def transformer_data(self) -> ScalarResult:
        return self.__get_table(TransformerData)

    @property
    def workforce(self) -> ScalarResult:
        return self.__get_table(Workforce)
