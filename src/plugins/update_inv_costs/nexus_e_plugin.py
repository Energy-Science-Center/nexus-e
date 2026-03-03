from dataclasses import asdict, dataclass
import logging
from pathlib import Path
from nexus_e_interface.scenario import Scenario
import pandas as pd
from sqlmodel import Field, SQLModel, Session, create_engine, select, text
from typing import Literal

from nexus_e_interface.plugin import Plugin

@dataclass
class Parameters:
    cost_scenario: Literal["Low", "Reference", "High"] = "Reference"
    

class NexusePlugin(Plugin):
    """
    Updates investment cost data in the database using cost data from CSV files.
    
    Implements: simulation.Module Protocol
    
    Features:
    - Supports Low, Reference, High cost scenarios
    - Automatically annualizes investment costs using WACC and lifetime parameters
    - Converts currencies using configurable conversion factors from CSV
    - Maps generator types to cost data technology categories
    """
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Parameters())

    def __init__(self, parameters: dict, scenario: Scenario):
        self.__settings = Parameters(**parameters)
        self.__data_context = scenario.get_data_context()
        if self.__data_context.type != "mysql":
            raise ValueError("update_inv_costs only works with a MySQL database")
        
        # Suppress verbose MySQL connector logs
        logging.getLogger('mysql.connector').setLevel(logging.WARNING)
        
        # Add connection timeout to fail faster - increased for slow networks
        # Disable SSL/TLS to avoid handshake hangs
        connection_string = (
            f"mysql+mysqlconnector://{self.__data_context.user}:"
            f"{self.__data_context.password}@{self.__data_context.host}/"
            f"{self.__data_context.name}?connect_timeout=10&"
            f"ssl_disabled=true&use_pure=true"
        )
        self.__engine = create_engine(
            connection_string,
            pool_timeout=10,
            pool_recycle=300,
            pool_pre_ping=False
        )
      

    def run(self) -> None:
        logging.debug("Starting Investment cost data update process...")
        
        # Test database connection first
        self.database_available = False
        try:
            with self.__engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                self.database_available = True
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            self.database_available = False
        
        self.__update_cost()

    def __update_cost(self):
        """Main orchestrator for cost update process."""
        cost_lookup, fom_lookup = self.__get_new_costs()
        if cost_lookup:
            self.__update_database_costs(cost_lookup, fom_lookup)
        logging.info("Investment cost data update process completed")

    def __get_new_costs(self):
        """Prepare all cost data from CSV files.
        
        Returns:
            tuple: (cost_lookup dict, fom_lookup dict)
        """
        df_investment_costs = self.__read_investment_costs()
        if df_investment_costs.empty:
            return {}, {}
        
        df_annualization = self.__read_annualization_parameters()
        df_fom_costs = self.__read_fom_costs()
        currency_conversion = self.__read_currency_conversion()
        
        df_costs = self.__calculate_annualized_costs(
            df_investment_costs, df_annualization, currency_conversion
        )
        cost_lookup = self.__create_cost_lookup(df_costs)
        cost_lookup = self.__add_pv_roof_aggregation(cost_lookup)
        fom_lookup = self.__create_fom_lookup(df_fom_costs)
        
        return cost_lookup, fom_lookup

    def __read_investment_costs(self):
        """Load and filter investment costs CSV by cost scenario.
        
        Returns:
            pd.DataFrame: Filtered investment costs data
        """
        inv_cost_path = (
            Path(__file__).parent / "cost_data" / "investment_costs.csv"
        )
        df_investment_costs = pd.read_csv(inv_cost_path)
        
        # Filter for the specified cost scenario
        df_investment_costs = df_investment_costs[
            df_investment_costs["Variant"] == self.__settings.cost_scenario
        ].copy()
        
        if df_investment_costs.empty:
            logging.debug(
                f"No cost data found for scenario "
                f"'{self.__settings.cost_scenario}'; skipping."
            )
        
        return df_investment_costs

    def __read_annualization_parameters(self):
        """Load annualization parameters CSV.
        
        Returns:
            pd.DataFrame: Annualization parameters (WACC, Lifetime)
        """
        annualization_path = (
            Path(__file__).parent
            / "cost_data"
            / "annualization_parameters.csv"
        )
        return pd.read_csv(annualization_path)

    def __read_fom_costs(self):
        """Load FOM costs CSV.
        
        Returns:
            pd.DataFrame: Fixed O&M costs data
        """
        fom_cost_path = (
            Path(__file__).parent / "cost_data" / "FOM_costs.csv"
        )
        return pd.read_csv(fom_cost_path)

    def __read_currency_conversion(self):
        """Load currency conversion factors CSV.
        
        Returns:
            dict: Dictionary mapping (From, To) -> conversion factor
        """
        conversion_path = (
            Path(__file__).parent / "cost_data" / "currency_conversion.csv"
        )
        df = pd.read_csv(conversion_path)
        return {
            (row["From"], row["To"]): row["ConversionFactor"]
            for _, row in df.iterrows()
        }

    def __calculate_annualized_costs(self, df_investment_costs, df_annualization, currency_conversion):
        """Calculate annualized costs from investment costs and parameters.
        
        Args:
            df_investment_costs: Investment cost data
            df_annualization: Annualization parameters (WACC, Lifetime)
            currency_conversion: Dictionary of currency conversion factors
            
        Returns:
            pd.DataFrame: Cost data with annualized costs calculated
        """
        # Merge investment costs with annualization parameters
        df_costs = df_investment_costs.merge(
            df_annualization,
            on="Technology",
            how="left"
        )
        
        # Fill missing annualization parameters with defaults
        missing_params = df_costs["WACC"].isna()
        if missing_params.any():
            missing_techs = df_costs.loc[missing_params, "Technology"].unique()
            logging.warning(
                f"No annualization parameters found for technologies: "
                f"{', '.join(missing_techs)}, using default 5% WACC and 30 years"
            )
            df_costs.loc[missing_params, "WACC"] = 0.05
            df_costs.loc[missing_params, "Lifetime"] = 30
        
        # Convert CHF to EUR where needed using conversion factor from CSV
        chf_to_eur = currency_conversion.get(("CHF", "EUR"), 1.1)
        chf_mask = df_costs["Unit"].str.contains("CHF", na=False)
        df_costs["Investment_cost_EUR"] = df_costs["Investment_cost"].copy()
        df_costs.loc[chf_mask, "Investment_cost_EUR"] *= chf_to_eur
        
        # Calculate annualization factor vectorially
        wacc = df_costs["WACC"]
        lifetime = df_costs["Lifetime"]
        df_costs["annualization_factor"] = (
            wacc * (1 + wacc)**lifetime / ((1 + wacc)**lifetime - 1)
        )
        
        # Calculate annualized cost
        df_costs["annualized_cost"] = (
            df_costs["Investment_cost_EUR"] * df_costs["annualization_factor"]
        )
        
        # Fill empty Size values with empty string
        df_costs["Size"] = df_costs["Size"].fillna("")
        
        # Drop rows with missing Year or Investment_cost
        df_costs = df_costs.dropna(subset=["Year", "Investment_cost"])
        
        return df_costs

    def __create_cost_lookup(self, df_costs):
        """Create lookup dictionary from cost dataframe.
        
        Args:
            df_costs: Processed cost data with annualized costs
            
        Returns:
            dict: Lookup dict with (Technology, Size, Year) -> (annualized, raw) costs
        """
        return {
            (row["Technology"], row["Size"], row["Year"]): (
                row["annualized_cost"],
                row["Investment_cost_EUR"]
            )
            for _, row in df_costs.iterrows()
        }

    def __add_pv_roof_aggregation(self, cost_lookup):
        """Add weighted PV-roof costs for 100-1000 kWp size class.
        
        Combines 100-300 kWp and 300-1000 kWp data using capacity weights.
        
        Args:
            cost_lookup: Existing cost lookup dictionary
            
        Returns:
            dict: Updated cost lookup with aggregated PV-roof costs
        """
        # Load PV-roof capacity weights
        pv_weights_path = (
            Path(__file__).parent / "cost_data" / "pv_roof_weights.csv"
        )
        df_pv_weights = pd.read_csv(pv_weights_path)
        
        # Create weighted 100 - 1000 kWp cost data by combining
        # 100-300 and 300-1000 data
        weight_100_300 = df_pv_weights["total_capacity_100-300"].iloc[0]
        weight_300_1000 = df_pv_weights["total_capacity_300-1000"].iloc[0]
        total_weight = weight_100_300 + weight_300_1000
        
        tech = "PV-roof"
        for year in [2020, 2030, 2040, 2050]:
            key_100_300 = (tech, "100 - 300 kWp", year)
            key_300_1000 = (tech, "300 - 1000 kWp", year)
            
            if key_100_300 in cost_lookup and key_300_1000 in cost_lookup:
                ann_100_300, raw_100_300 = cost_lookup[key_100_300]
                ann_300_1000, raw_300_1000 = cost_lookup[key_300_1000]
                
                # Calculate weighted average
                weighted_ann = (
                    ann_100_300 * weight_100_300
                    + ann_300_1000 * weight_300_1000
                ) / total_weight
                weighted_raw = (
                    raw_100_300 * weight_100_300
                    + raw_300_1000 * weight_300_1000
                ) / total_weight
                
                # Store with 100 - 1000 kWp size key
                cost_lookup[(tech, "100 - 1000 kWp", year)] = (
                    weighted_ann,
                    weighted_raw
                )
        
        return cost_lookup

    def __create_fom_lookup(self, df_fom_costs):
        """Create FOM cost lookup dictionary.
        
        Args:
            df_fom_costs: Fixed O&M costs data
            
        Returns:
            dict: Lookup dict with (Technology, Year) -> (percentage, annualized_costs)
        """
        # Fill NaN values with 0
        df_fom_costs["percentage_per_annualizedCosts"] = (
            df_fom_costs["percentage_per_annualizedCosts"].fillna(0)
        )
        df_fom_costs["annualized_costs [EUR/kW/Year]"] = (
            df_fom_costs["annualized_costs [EUR/kW/Year]"].fillna(0)
        )
        
        # Create lookup dictionary
        return {
            (row["Technology"], row["Year"]): (
                row["percentage_per_annualizedCosts"],
                row["annualized_costs [EUR/kW/Year]"]
            )
            for _, row in df_fom_costs.iterrows()
        }

    def __update_database_costs(self, cost_lookup, fom_lookup):
        """Update generator configuration costs in database.
        
        Args:
            cost_lookup: Dictionary mapping (Technology, Size, Year) to costs
            fom_lookup: Dictionary mapping (Technology, Year) to FOM costs
        """
        if not self.database_available:
            logging.warning("Database not available - skipping cost update")
            return

        with Session(self.__engine) as session, session.begin():
            # Get all generator types and their years from Gendata
            stmt_gendata = select(
                Gendata.idGen, Gendata.Technology, Gendata.StartYr
            )
            gendata_results = session.exec(stmt_gendata).all()

            # Convert to DataFrame and group by technology and year
            df_gendata = pd.DataFrame([
                {
                    "idGen": r.idGen,
                    "Technology": r.Technology,
                    "StartYr": int(r.StartYr) if r.StartYr else None
                }
                for r in gendata_results
            ])
            
            # Filter out rows with missing Technology or StartYr
            df_gendata = df_gendata.dropna(subset=["Technology", "StartYr"])
            
            # Group by technology and year to get lists of generator IDs
            tech_year_generators = (
                df_gendata.groupby(["Technology", "StartYr"])["idGen"]
                .apply(list)
                .to_dict()
            )
            
            all_gen_ids = df_gendata["idGen"].tolist()
            
            # Fetch ALL configurations in one query
            stmt_all_configs = select(GenConfiguration).where(
                GenConfiguration.idGen.in_(all_gen_ids)
            )
            all_configs = session.exec(stmt_all_configs).all()
            
            # Convert to DataFrame and group configurations by idGen for fast lookup
            df_configs = pd.DataFrame([
                {"idGen": c.idGen, "config": c}
                for c in all_configs
            ])
            
            configs_by_gen_id = (
                df_configs.groupby("idGen")["config"]
                .apply(list)
                .to_dict()
            )
            
            # Log available technologies in database
            db_techs = set(
                tech for tech, year in tech_year_generators.keys()
            )
            logging.debug(
                f"Technologies in database: {', '.join(sorted(db_techs))}"
            )
            
            # Log available technologies in cost data
            csv_techs = set(
                tech for tech, size, year in cost_lookup.keys()
            )
            logging.debug(
                f"Technologies in CSV: {', '.join(sorted(csv_techs))}"
            )

            # Update investment costs
            updated_count = 0
            not_found_count = 0
            # Track which technologies are missing
            missing_techs = {}
            # Track which technologies had InvCost updated
            updated_techs_inv = {}
            # Track which technologies had FOM_Cost updated
            updated_techs_fom = {}
            
            # Mapping from idGenConfig to year
            config_year_mapping = {
                3: 2030,
                4: 2040,
                5: 2050
            }
            
            for (tech, year), gen_ids in tech_year_generators.items():
                tech_updated = 0
                tech_not_found = 0
                
                for gen_id in gen_ids:
                    # Get configurations from pre-fetched data
                    configs = configs_by_gen_id.get(gen_id, [])
                    
                    for config in configs:
                        # Determine the correct year from idGenConfig
                        config_year = config_year_mapping.get(
                            config.idGenConfig, year
                        )
                        
                        size_key = ""
                        
                        # Special handling for PV-roof only - extract size
                        # from GenName
                        # PV-alpine does not have size classes in the CSV
                        if tech == "PV-roof" and config.GenName:
                            # Extract size from the last part of the name
                            # (e.g., "10-30kW")
                            name_parts = config.GenName.split('_')
                            if name_parts:
                                size_key = name_parts[-1]
                                # Map size formats: "10-30kW" -> "10 - 30 kWp",
                                # "100-1000kW" -> "100 - 1000 kWp"
                                if "kW" in size_key:
                                    size_key = size_key.replace(
                                        "kW", " kWp"
                                    ).replace("-", " - ")
                        
                        # Look up cost in our precomputed lookup table
                        # using config_year
                        lookup_key = (tech, size_key, config_year)
                        
                        # Debug logging for PV-alpine
                        if (
                            tech == "PV-alpine"
                            and tech_updated == 0
                            and tech_not_found == 0
                        ):
                            logging.debug(
                                f"PV-alpine lookup: GenName={config.GenName}, "
                                f"size_key='{size_key}', "
                                f"config_year={config_year}, "
                                f"lookup_key={lookup_key}, "
                                f"key_exists={lookup_key in cost_lookup}"
                            )
                        
                        if lookup_key in cost_lookup:
                            (
                                annualized_inv_cost,
                                raw_investment_cost
                            ) = cost_lookup[lookup_key]
                            # Convert from EUR/kW/yr to EUR/MW/yr
                            config.InvCost = float(annualized_inv_cost * 1000)
                            
                            # Track InvCost update
                            if tech not in updated_techs_inv:
                                updated_techs_inv[tech] = 0
                            updated_techs_inv[tech] += 1
                            
                            # Calculate FOM_Cost based on FOM_costs.csv
                            # using config_year
                            # Now using percentage of RAW investment cost,
                            # not annualized cost
                            fom_key = (tech, config_year)
                            fom_cost_value = 0
                            if fom_key in fom_lookup:
                                percentage, annualized_costs = fom_lookup[
                                    fom_key
                                ]
                                if annualized_costs > 0:
                                    # Use direct annualized_costs value
                                    # (convert from EUR/kW to EUR/MW)
                                    fom_cost_value = annualized_costs * 1000
                                elif percentage > 0:
                                    # Use percentage of RAW investment cost
                                    # (not annualized)
                                    # raw_investment_cost is in EUR/kW,
                                    # convert to EUR/MW
                                    fom_cost_value = (
                                        (raw_investment_cost * 1000)
                                        * (percentage / 100)
                                    )
                            
                            config.FOM_Cost = float(fom_cost_value)
                            
                            # Track FOM_Cost update
                            if (
                                fom_cost_value > 0
                                or fom_key in fom_lookup
                            ):
                                if tech not in updated_techs_fom:
                                    updated_techs_fom[tech] = 0
                                updated_techs_fom[tech] += 1
                            
                            # Set VOM cost to 0
                            config.VOM_Cost = 0.0
                            
                            updated_count += 1
                            tech_updated += 1
                        else:
                            not_found_count += 1
                            tech_not_found += 1
                            if tech not in missing_techs:
                                missing_techs[tech] = 0
                            missing_techs[tech] += 1

            # Log which technologies had Investment costs updated
            if updated_techs_inv:
                tech_list = ', '.join([
                    f'{tech} ({count})'
                    for tech, count in updated_techs_inv.items()
                ])
                logging.info(
                    f"Updated Investment costs for technologies: {tech_list}"
                )
            
            # Log which technologies had FOM costs updated
            if updated_techs_fom:
                tech_list = ', '.join([
                    f'{tech} ({count})'
                    for tech, count in updated_techs_fom.items()
                ])
                logging.info(
                    f"Updated FOM costs for technologies: {tech_list}"
                )
            
            logging.info(f"Updated {updated_count} generator configurations")
                    
class Gendata(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    idGen: int = Field(primary_key=True)
    GenName: str | None = Field(default=None, max_length=100)
    GenType: str | None = Field(default=None, max_length=45)
    Technology: str | None = Field(default=None, max_length=45)
    UnitType: str | None = Field(default=None, max_length=45)
    StartYr: float | None = Field(default=None)
    EndYr: float | None = Field(default=None)
    GenEffic: float | None = Field(default=None)
    CO2Rate: float | None = Field(default=None)
    eta_dis: float | None = Field(default=None)
    eta_ch: float | None = Field(default=None)
    RU: float | None = Field(default=None)
    RD: float | None = Field(default=None)
    RU_start: float | None = Field(default=None)
    RD_shutd: float | None = Field(default=None)
    UT: int | None = Field(default=None)
    DT: int | None = Field(default=None)
    Pini: float | None = Field(default=None)
    Tini: float | None = Field(default=None)
    meanErrorForecast24h: float | None = Field(default=None)
    sigmaErrorForecast24h: float | None = Field(default=None)
    Lifetime: float | None = Field(default=None)

class GenConfiguration(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    idGenConfig: int = Field(primary_key=True, description="primary identifier for generator configurations")
    idBus: int = Field(primary_key=True, description="primary identifier for node where given generator is located")
    idGen: int = Field(primary_key=True, description="primary identifier for generators")
    GenName: str | None = Field(default=None, max_length=100, description="unique name for generators")
    idProfile: int | None = Field(default=None, description="identifier for profile that defines this generator’s time series production (RES units) or time series for water inflows (Hydro units)")
    CandidateUnit: int | None = Field(default=None, description="indicator if a given generator does not yet exist and should be considered for investment")
    Pmax: float | None = Field(default=None, description="MW")
    Pmin: float | None = Field(default=None, description="MW")
    Qmax: float | None = Field(default=None, description="MVAr")
    Qmin: float | None = Field(default=None, description="MVAr")
    Emax: float | None = Field(default=None, description="Maximum storage volume, MWh")
    Emin: float | None = Field(default=None, description="Minimum allowable storage volume, MWh")
    E_ini: float | None = Field(default=None, description="Initial storage volume at beginning of simulation, fraction of Emax")
    VOM_Cost: float | None = Field(default=None, description="nonFuel variable O&M cost, EUR/MWh")
    FOM_Cost: float | None = Field(default=None, description="Fixed O&M cost, EUR/MW/yr")
    InvCost: float | None = Field(default=None, description="Annualized investment cost for building generator, EUR/MW/yr")
    InvCost_E: float | None = Field(default=None, description="Annualized investment cost for building storage capacity associated with a storage generator, EUR/MWh/yr")
    InvCost_Charge: float | None = Field(default=None, description="Annualized investment cost for building consumption portion of a storage generator (like pumping portion of pumped hydro or electrolyzer portion of hydrogen), EUR/MW/yr")
    StartCost: float | None = Field(default=None, description="EUR/MW/start")
    TotVarCost: float | None = Field(default=None, description="Sum of all variable operating costs, EUR/MWh")
    FuelType: str | None = Field(default=None, max_length=45, description="unique name of fuel used by given generator")
    CO2Type: str | None = Field(default=None, max_length=45, description="unique name of CO2 entry in fuel prices table used by given generator")
    status: float | None = Field(default=None, description="online status, 1 = in service, 0 = not in service")
    HedgeRatio: float | None = Field(default=None, description="fraction, portion of monthly average power generated to offer into the Future market clearing")

