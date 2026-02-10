from dataclasses import asdict, dataclass
import logging
import nexus_e_interface
import numpy as np
import pandas as pd
from pathlib import Path
import pymysql
from sqlalchemy import JSON, Column, text, func
from sqlmodel import Field, SQLModel, Session, create_engine, select

from nexus_e_interface.plugin import Plugin

@dataclass
class Parameters:
    input_data_host: str = "localhost"
    input_data_user: str = "username"
    input_data_password: str = "password"
    input_data_name: str = "database_name"
    upload_pv: bool = False
    pv_data_year: str = "weather year" # Options: 2016
    pv_data_scenario: str = "aggregation level"  # Options: 'full resolution', 'aggregated',
    upload_ror: bool = False
    ror_data_scenario: str = "historical" #Options: historical, rcp26
    upload_wind: bool = False
    upload_alpine_PV: bool = False
    enable_pathway_runs: bool = False
    biodiversity_scenario: str = "current" # Options: 'current', 'target'
    public_acceptance_scenario: str = "00" # Options: '00'=not considered '06'=threshold06 (only few areas allowed)
    wind_data_scenario: str = "cat:(1-1)"  # Options: 'cf >= 0.15' (cat 1 - 5), 'cf >= 0.20' (cat 1-4), 'cf >= 0.25' (cat 1-3), 'cf >= 0.30' (cat 1-2)
    wind_rcp_scenario: str = "rcp85" # Options: rcp26, rcp85, historical
    copy_database: bool = False  # If True, copy the database before uploading data
    new_dbName: str | None = None  # Name for the new copied database (required if copy_database=True)



class NexusePlugin(Plugin):
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Parameters())
    
    def __init__(self, parameters: dict, scenario: nexus_e_interface.Scenario | None = None):
        self.__settings = Parameters(**parameters)
        
        # Copy database if requested
        if self.__settings.copy_database:
            if not self.__settings.new_dbName:
                raise ValueError("new_dbName must be specified when copy_database is True")
            self.__copy_database()
            target_db = self.__settings.new_dbName
        else:
            target_db = self.__settings.input_data_name
        
        logging.info(f"Connecting to database: {target_db} on host: {self.__settings.input_data_host}")
        self.__engine = create_engine((
            f"mysql+pymysql://{self.__settings.input_data_user}:{self.__settings.input_data_password}"
            f"@{self.__settings.input_data_host}/{target_db}"))
        
        # Dictionary to store GenName -> idProfile mapping for current upload session
        self.__genname_to_idprofile = {}

    def run(self) -> None:
        SQLModel.metadata.create_all(self.__engine)
        self.__ensure_gendata_columns()
        if self.__settings.upload_wind:
            self.__upload_wind()
        if self.__settings.upload_alpine_PV:
            self.__upload_alpine_PV()
        if self.__settings.upload_pv:
            self.__upload_pv()
        if self.__settings.upload_ror:
            self.__upload_ror()

    def __add_columns_to_database_table_gendata(self, columns):
        """Helper method to add columns to the gendata table if they don't exist"""
        with self.__engine.connect() as conn:
            # Check which columns already exist
            result = conn.execute(text(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'gendata'"
            ))
            existing_columns = {row[0] for row in result}
            
            # Add missing columns
            for column in columns:
                if column.name not in existing_columns:
                    logging.info(f"Adding column '{column.name}' ({column.type}) to gendata table")
                    conn.execute(text(f"ALTER TABLE gendata ADD COLUMN {column.name} {column.type}"))
                    conn.commit()

    def __ensure_gendata_columns(self):
        """Ensure RoR-specific columns exist in the gendata table"""
        logging.debug("Checking and adding missing RoR columns to gendata table...")
        
        @dataclass
        class Column:
            name: str
            type: str
            excel_name: str = ""
        
        # Define the columns that need to exist with their SQL data types
        required_columns = (
            Column("WASTA", "bigint", "WASTANum"),
            Column("QT", "double", "Qturbine (m3/s)"),
            Column("QP", "double", "Qpump (m3/s)"),
            Column("idResUp", "varchar(100)", "idRes_up"),
            Column("idResLow", "varchar(100)", "idRes_low"),
            Column("idCasc", "int", "cascade_id"),
            Column("HydroCHAgg", "int", "HydroAggCH"),
            Column("HydroCHCasc", "int", "HydroDetailCH"),
        )
        
        self.__add_columns_to_database_table_gendata(required_columns)
        logging.debug("RoR columns check complete")

    def __copy_database(self):
        """Copy the original database to a new database name using SQL queries directly"""
        original_db = self.__settings.input_data_name
        new_db = self.__settings.new_dbName
        
        logging.info(f"Copying database '{original_db}' to '{new_db}' directly...")
        
        try:
            # Create connection to MySQL server with timeout
            logging.debug(f"Connecting to MySQL server {self.__settings.input_data_host}...")
            conn = pymysql.connect(
                host=self.__settings.input_data_host,
                user=self.__settings.input_data_user,
                password=self.__settings.input_data_password,
                connect_timeout=30
            )
            cursor = conn.cursor()
            
            # Drop the new database if it exists
            logging.debug(f"Dropping database '{new_db}' if it exists...")
            cursor.execute(f"DROP DATABASE IF EXISTS `{new_db}`")
            
            # Create new database and copy directly using CREATE DATABASE ... LIKE
            logging.info(f"Creating and copying database (this may take several minutes)...")
            cursor.execute(f"CREATE DATABASE `{new_db}`")
            
            # Get all tables from the original database
            cursor.execute(f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{original_db}'")
            tables = cursor.fetchall()
            
            logging.debug(f"Copying {len(tables)} tables...")
            
            # Copy each table
            for (table_name,) in tables:
                # Create table structure
                cursor.execute(f"CREATE TABLE `{new_db}`.`{table_name}` LIKE `{original_db}`.`{table_name}`")
                # Copy data
                cursor.execute(f"INSERT INTO `{new_db}`.`{table_name}` SELECT * FROM `{original_db}`.`{table_name}`")
            
            # Copy views
            cursor.execute(f"SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '{original_db}'")
            views = cursor.fetchall()
            
            if views:
                logging.debug(f"Copying {len(views)} views...")
                for (view_name,) in views:
                    cursor.execute(f"SHOW CREATE VIEW `{original_db}`.`{view_name}`")
                    view_def = cursor.fetchone()[1]
                    # Replace database name in view definition
                    view_def = view_def.replace(f"`{original_db}`.", f"`{new_db}`.")
                    cursor.execute(f"USE `{new_db}`")
                    cursor.execute(view_def)
            
            # Copy stored procedures and functions
            cursor.execute(f"SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '{original_db}'")
            routines = cursor.fetchall()
            
            if routines:
                logging.debug(f"Copying {len(routines)} stored procedures/functions...")
                for (routine_name, routine_type) in routines:
                    if routine_type == 'PROCEDURE':
                        cursor.execute(f"SHOW CREATE PROCEDURE `{original_db}`.`{routine_name}`")
                    else:
                        cursor.execute(f"SHOW CREATE FUNCTION `{original_db}`.`{routine_name}`")
                    routine_def = cursor.fetchone()[2]
                    # Replace database name in routine definition
                    routine_def = routine_def.replace(f"`{original_db}`.", f"`{new_db}`.")
                    cursor.execute(f"USE `{new_db}`")
                    cursor.execute(routine_def)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"[OK] Database copied successfully: {original_db} -> {new_db}")
            
        except pymysql.Error as e:
            logging.error(f"MySQL error during database copy: {e}")
            raise RuntimeError(f"Failed to copy database: {e}")
        except Exception as e:
            logging.error(f"Database copy failed: {e}")
            raise

    def __upload_wind(self):
        profiles_path, gens_path = self.__define_scenario_path_bio_public_acceptance(tech="wind")
        logging.info(f"Uploading wind data from profiles: {profiles_path}, gens: {gens_path}")
        self.__delete_existing_tech(tech='WindOn')
        
        # Read and preprocess data, then filter once
        profiles_df_raw = pd.read_csv(profiles_path)
        gens_df_raw = pd.read_csv(gens_path)
        
        # Preprocess to get proper column names
        _, profiles_df = self.__prepareData_profiles(profiles_df_raw)
        
        # Now filter the preprocessed data
        wind_categories = self.__settings.wind_data_scenario
        logging.info(f"Filtering wind data for wind_categories = {wind_categories}")
        profiles_df = self.__filter_categories_wind_data(profiles_df)
        gens_df = self.__filter_categories_wind_data(gens_df_raw)
        logging.info("Filtering of wind categories successfully done!")
        
        # Apply pathway duplication if enabled (for both gens and profiles)
        if self.__settings.enable_pathway_runs:
            gens_df, profiles_df = self.__apply_pathway_duplication_with_profiles(gens_df, profiles_df, tech_type="WindOn")
        
        self.__add_new_tech_data_profiles_preprocessed(profiles_df, tech_type="WindOn")
        self.__upload_gendata_from_df(gens_df, tech_type="WindOn")
        self.__upload_genconfiguration_from_df(gens_df, tech_type="WindOn")
        self.__reorder_generators()
        self.__reorder_profiles()

    def __upload_alpine_PV(self):
        profiles_path, gens_path = self.__define_scenario_path_bio_public_acceptance(tech="alpine_PV")
        self.__delete_existing_tech(tech='PV-alpine')
        
        # Read both profiles and gens
        profiles_df_raw = pd.read_csv(profiles_path)
        gens_df = pd.read_csv(gens_path)
        
        # Check for and merge duplicate generator entries before processing
        gens_df = self.__merge_duplicate_generators(gens_df, tech_type="PV-alpine")
        
        # Preprocess profiles
        _, profiles_df = self.__prepareData_profiles(profiles_df_raw)
        
        # Apply pathway duplication if enabled (for both gens and profiles)
        if self.__settings.enable_pathway_runs:
            gens_df, profiles_df = self.__apply_pathway_duplication_with_profiles(gens_df, profiles_df, tech_type="PV-alpine")
        
        self.__add_new_tech_data_profiles_preprocessed(profiles_df, tech_type="PV-alpine")
        self.__upload_gendata_from_df(gens_df, tech_type="PV-alpine")
        self.__upload_genconfiguration_from_df(gens_df, tech_type="PV-alpine")
        self.__reorder_generators()
        self.__reorder_profiles()
        

    def __upload_pv(self):
        pv_sub_folder = f"{self.__settings.pv_data_year}/{self.__settings.pv_data_scenario}"
        new_gens_file_path = Path(__file__).parent / "pv_data" / pv_sub_folder / "NexusInput_genstab_newPV.csv"
        new_gendata_file_path = Path(__file__).parent / "pv_data" / pv_sub_folder / "NexusInput_genstab_newPV.csv"
        new_profiles_file_path = Path(__file__).parent / "pv_data" / pv_sub_folder / "NexusInput_profiles_newPV_mv.csv"
        self.__delete_existing_tech(tech='PV-roof', start_year=2030)
        
        # Read both profiles and gens
        profiles_df_raw = pd.read_csv(new_profiles_file_path)
        gens_df = pd.read_csv(new_gendata_file_path)
        
        # Check for and merge duplicate generator entries before processing
        gens_df = self.__merge_duplicate_generators(gens_df, tech_type="PV-roof")
        
        # Preprocess profiles
        _, profiles_df = self.__prepareData_profiles(profiles_df_raw)
        
        # Apply pathway duplication if enabled (for both gens and profiles)
        if self.__settings.enable_pathway_runs:
            gens_df, profiles_df = self.__apply_pathway_duplication_with_profiles(gens_df, profiles_df, tech_type="PV-roof")
        
        # Debug: Print aggregated capacity per year and production
        if not gens_df.empty and 'start_year' in gens_df.columns and 'P_gen_max in 2015 (MW)' in gens_df.columns:
            capacity_by_year = gens_df.groupby('start_year')['P_gen_max in 2015 (MW)'].sum()
            logging.debug(f"[PV-roof] Aggregated capacity by year (MW):")
            for year, capacity in capacity_by_year.items():
                logging.debug(f"  Year {int(year)}: {capacity:.2f} MW")
        
        # Clear the mapping before uploading new profiles
        self.__genname_to_idprofile.clear()
        
        self.__add_new_tech_data_profiles_preprocessed(profiles_df, tech_type="PV-roof")
        self.__upload_gendata_from_df(gens_df, tech_type="PV-roof")
        self.__upload_genconfiguration_from_df(gens_df, tech_type="PV-roof")
        self.__reorder_generators()
        self.__reorder_profiles()
        
        # Debug check: Verify all idProfile numbers are unique
        self.__verify_idprofile_uniqueness(tech_type="PV-roof")
        
        # Verify database content - capacity and generation per year
        self.__verify_database_content(tech_type='PV-roof')

    def __upload_ror(self):
        ror_scenario = self.__settings.ror_data_scenario
        gens_path = Path(__file__).parent / "ror_data" / ror_scenario / "gens.csv"
        profiles_path = Path(__file__).parent / "ror_data" / ror_scenario / "profiles.csv"
        logging.info(f"Uploading ROR data from profiles: {profiles_path}, gens: {gens_path}")
        self.__delete_existing_tech(tech='RoR', check_hydro_columns=True)
        
        # Read both profiles and gens
        profiles_df_raw = pd.read_csv(profiles_path)
        gens_df = pd.read_csv(gens_path)
        
        # Calculate capacity per year from CSV
        capacity_by_year = {}
        for _, row in gens_df.iterrows():
            if pd.notna(row.get('start_year')) and pd.notna(row.get('P_gen_max in 2015 (MW)')):
                year = int(row['start_year'])
                capacity = float(row['P_gen_max in 2015 (MW)'])
                capacity_by_year[year] = capacity_by_year.get(year, 0) + capacity
        
        # Check for generators without profiles
        gens_without_profile = gens_df[gens_df['idProfile'].isna() | (gens_df['idProfile'] == '')]
        profile_diff = len(gens_df) - len(profiles_df_raw)
        
        if profile_diff == len(gens_without_profile) and profile_diff > 0:
            logging.info(f"[RoR] Loaded from CSV: {len(gens_df)} generators, {len(profiles_df_raw)} profiles ({profile_diff} generators without profiles)")
        else:
            logging.info(f"[RoR] Loaded from CSV: {len(gens_df)} generators, {len(profiles_df_raw)} profiles")
            if not gens_without_profile.empty:
                logging.warning(f"[RoR] Found {len(gens_without_profile)} generators without idProfile (profile_diff={profile_diff})")
        
        # Report capacity per year from CSV
        total_csv_capacity = sum(capacity_by_year.values())
        logging.info(f"[RoR] Total capacity in CSV: {total_csv_capacity:.2f} MW")
        for year in sorted(capacity_by_year.keys()):
            logging.info(f"[RoR]   - Year {year}: {capacity_by_year[year]:.2f} MW")
        
        # Check for and merge duplicate generator entries before processing
        gens_df = self.__merge_duplicate_generators(gens_df, tech_type="RoR")
        
        # Preprocess profiles
        _, profiles_df = self.__prepareData_profiles(profiles_df_raw)
        
        # Apply pathway duplication if enabled (for both gens and profiles)
        if self.__settings.enable_pathway_runs:
            gens_df, profiles_df = self.__apply_pathway_duplication_with_profiles(gens_df, profiles_df, tech_type="RoR")
        
        gens_df = gens_df.dropna(subset=["idProfile"])
        self.__add_new_tech_data_profiles_preprocessed(profiles_df, tech_type="RoR")
        self.__upload_gendata_from_df(gens_df, tech_type="RoR")
        self.__upload_genconfiguration_from_df(gens_df, tech_type="RoR")
        self.__reorder_generators()
        self.__reorder_profiles()
        
        # Verification checks
        self.__verify_ror_upload(gens_df, profiles_df)


    def __apply_pathway_duplication_with_profiles(self, gens_df: pd.DataFrame, profiles_df: pd.DataFrame, tech_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Duplicate generators and profiles for pathway years 2030 and 2040 if they don't exist in the input data"""
        
        # Check if data already has 2030 and 2040 generators
        has_2030 = (gens_df['start_year'] == 2030).any()
        has_2040 = (gens_df['start_year'] == 2040).any()
        
        if has_2030 and has_2040:
            logging.info(f"[{tech_type}] Input csv data is already pathway ready")
            return gens_df, profiles_df
        
        logging.info(f"[{tech_type}] No 2030/2040 generators found, creating pathway duplicates...")
        
        # Check if generator names have _Cd pattern
        import re
        cd_pattern = re.compile(r'_Cd\d+$')
        has_cd_pattern = gens_df['name'].str.contains(cd_pattern, na=False).any()
        
        if not has_cd_pattern:
            logging.info(f"[{tech_type}] No _Cd pattern found in names, adding _Cd suffixes...")
            # Add _Cd1, _Cd2, etc. to both generator and profile names
            profiles_renamed = 0
            gens_renamed = 0
            profiles_not_found = 0
            
            for idx in range(len(gens_df)):
                original_gen_name = gens_df.at[idx, 'name']
                new_gen_name = f"{original_gen_name}_Cd{idx + 1}"
                gens_df.at[idx, 'name'] = new_gen_name
                gens_renamed += 1
                
                # Also rename matching profile
                profile_mask = profiles_df['name'] == original_gen_name
                if profile_mask.any():
                    profiles_df.loc[profile_mask, 'name'] = new_gen_name
                    profiles_renamed += 1
                else:
                    profiles_not_found += 1
            
            logging.info(f"[{tech_type}] Renamed {gens_renamed} generators and {profiles_renamed} profiles with _Cd suffixes")
            if profiles_not_found > 0:
                logging.warning(f"[{tech_type}] {profiles_not_found} generators had no matching profiles")
        else:
            logging.info(f"[{tech_type}] Generator names already contain _Cd pattern")
        
        original_count = len(gens_df)
        
        # Create copies for 2030 and 2040 (gens only, profiles stay the same)
        gens_df_2030 = gens_df.copy()
        gens_df_2030['start_year'] = 2030
        gens_df_2030['end_year (50yrNucl)'] = 2039
        
        gens_df_2040 = gens_df.copy()
        gens_df_2040['start_year'] = 2040
        gens_df_2040['end_year (50yrNucl)'] = 2049
        
        # Update original to have EndYr = 2100
        gens_df['end_year (50yrNucl)'] = 2100
        
        # Concatenate all dataframes (gens only)
        gens_df_combined = pd.concat([gens_df_2030, gens_df_2040, gens_df], ignore_index=True)
        
        final_count = len(gens_df_combined)
        logging.info(f"[{tech_type}] Pathway duplication complete: {original_count} -> {final_count} generators (3x)")
        logging.info(f"[{tech_type}] Profiles kept at {len(profiles_df)} (shared across all years)")
        
        # Verify triplication
        if final_count != original_count * 3:
            logging.warning(f"[{tech_type}] Expected {original_count * 3} generators but got {final_count}")
        
        return gens_df_combined, profiles_df
    
    def __merge_duplicate_generators(self, df: pd.DataFrame, tech_type: str) -> pd.DataFrame:
        """Check for duplicate generator entries (same name + start_year) and merge them by summing Pmax"""
        
        # Find duplicates based on name and start_year
        duplicate_mask = df.duplicated(subset=['name', 'start_year'], keep=False)
        
        if not duplicate_mask.any():
            logging.debug(f"[{tech_type}] No duplicate generators found")
            return df
        
        duplicates_df = df[duplicate_mask].copy()
        unique_df = df[~duplicate_mask].copy()
        
        # Group duplicates by name and start_year
        grouped = duplicates_df.groupby(['name', 'start_year'])
        
        merged_rows = []
        merge_count = 0
        
        for (name, start_year), group in grouped:
            if len(group) > 1:
                merge_count += 1
                # Sum the Pmax values
                total_pmax = group['P_gen_max in 2015 (MW)'].sum()
                
                # Take the first row and update Pmax
                merged_row = group.iloc[0].copy()
                merged_row['P_gen_max in 2015 (MW)'] = total_pmax
                
                logging.debug(f"[{tech_type}] Merged {len(group)} duplicates for '{name}' (StartYr={start_year}): Pmax={total_pmax:.3f} MW (from {group['P_gen_max in 2015 (MW)'].tolist()})")
                
                merged_rows.append(merged_row)
            else:
                # Only one entry for this name+year combination (shouldn't happen, but handle it)
                merged_rows.append(group.iloc[0])
        
        # Combine unique and merged rows
        if merged_rows:
            merged_df = pd.DataFrame(merged_rows)
            result_df = pd.concat([unique_df, merged_df], ignore_index=True)
            logging.debug(f"[{tech_type}] Merged {merge_count} duplicate generator groups, reduced from {len(df)} to {len(result_df)} entries")
            return result_df
        else:
            return df
    
    def __delete_existing_tech(self, tech: str, start_year: int = 2012, check_hydro_columns: bool = False) -> None:
        """Delete profiles and generators of specific technology (StartYr >= start_year, GenName starting with 'CH')
        
        Args:
            tech: Technology type to delete
            start_year: Minimum start year for deletion
            check_hydro_columns: If True (for RoR), only delete generators where all hydro cascade columns are empty/zero
        """
        logging.info(f"Deleting existing {tech} generators (StartYr >= {start_year}, GenName like 'CH%')")
        
        with Session(self.__engine) as session:
            # First get the generator names, idGen, and idProfile values from GenConfiguration
            genconfig_statement = select(
                GenConfiguration.GenName, 
                GenConfiguration.idGen,
                GenConfiguration.idProfile
            ).join(
                Gendata, GenConfiguration.idGen == Gendata.idGen
            ).where(
                (Gendata.Technology == tech) & 
                (Gendata.StartYr >= start_year) &
                (Gendata.GenName.like('CH%'))
            )
            genconfig_results = session.exec(genconfig_statement)
            generators_to_check = [(row.GenName, row.idGen, row.idProfile) for row in genconfig_results]
            
            # Filter generators based on hydro cascade columns if requested
            if check_hydro_columns and tech == 'RoR':
                logging.info(f"[{tech}] Checking {len(generators_to_check)} generators for hydro cascade data...")
                generators_to_delete = []
                generators_to_keep = []
                
                # First check if columns exist in the database
                sample_gen = session.exec(select(Gendata).limit(1)).first()
                hydro_columns_exist = {
                    'WASTA': hasattr(sample_gen, 'WASTA') if sample_gen else False,
                    'QT': hasattr(sample_gen, 'QT') if sample_gen else False,
                    'QP': hasattr(sample_gen, 'QP') if sample_gen else False,
                    'idResUp': hasattr(sample_gen, 'idResUp') if sample_gen else False,
                    'idResLow': hasattr(sample_gen, 'idResLow') if sample_gen else False,
                    'idCasc': hasattr(sample_gen, 'idCasc') if sample_gen else False,
                    'HydroCHAgg': hasattr(sample_gen, 'HydroCHAgg') if sample_gen else False,
                    'HydroCHCasc': hasattr(sample_gen, 'HydroCHCasc') if sample_gen else False,
                }
                existing_cols = [col for col, exists in hydro_columns_exist.items() if exists]
                missing_cols = [col for col, exists in hydro_columns_exist.items() if not exists]
                
                if existing_cols:
                    logging.debug(f"[{tech}] Found hydro cascade columns in database: {', '.join(existing_cols)}")
                if missing_cols:
                    logging.debug(f"[{tech}] Missing hydro cascade columns in database: {', '.join(missing_cols)}")
                
                for gen_name, idgen, idprofile in generators_to_check:
                    # Check if this generator has any non-zero/non-empty hydro cascade columns
                    gendata_check = session.exec(
                        select(Gendata).where(Gendata.idGen == idgen)
                    ).first()
                    
                    if gendata_check:
                        # Check hydro cascade columns and collect which ones have data
                        hydro_data_found = []
                        
                        if hasattr(gendata_check, 'WASTA') and gendata_check.WASTA is not None and gendata_check.WASTA != 0:
                            hydro_data_found.append(f"WASTA={gendata_check.WASTA}")
                        if hasattr(gendata_check, 'QT') and gendata_check.QT is not None and gendata_check.QT != 0:
                            hydro_data_found.append(f"QT={gendata_check.QT}")
                        if hasattr(gendata_check, 'QP') and gendata_check.QP is not None and gendata_check.QP != 0:
                            hydro_data_found.append(f"QP={gendata_check.QP}")
                        if hasattr(gendata_check, 'idResUp') and gendata_check.idResUp is not None and gendata_check.idResUp != '' and gendata_check.idResUp != '0':
                            hydro_data_found.append(f"idResUp={gendata_check.idResUp}")
                        if hasattr(gendata_check, 'idResLow') and gendata_check.idResLow is not None and gendata_check.idResLow != '' and gendata_check.idResLow != '0':
                            hydro_data_found.append(f"idResLow={gendata_check.idResLow}")
                        if hasattr(gendata_check, 'idCasc') and gendata_check.idCasc is not None and gendata_check.idCasc != 0:
                            hydro_data_found.append(f"idCasc={gendata_check.idCasc}")

                        
                        if hydro_data_found:
                            generators_to_keep.append((gen_name, idgen, idprofile))
                            if len(generators_to_keep) <= 3:  # Show details for first 3
                                logging.debug(f"  ✓ Keeping '{gen_name}': {', '.join(hydro_data_found)}")
                        else:
                            generators_to_delete.append((gen_name, idgen, idprofile))
                    else:
                        generators_to_delete.append((gen_name, idgen, idprofile))
                
                
                # Summary of check results
                logging.debug(f"[{tech}] Check complete: {len(generators_to_keep)} generators to KEEP, {len(generators_to_delete)} generators to DELETE")
                
                # drop all dublicates in generators to keep:
                generators_to_keep = list(set(generators_to_keep))
                logging.info(f"generators_to_keep: {generators_to_keep}")
                if generators_to_keep:
                    logging.info(f"[{tech}] Preserving {len(generators_to_keep)} generators with hydro cascade data:")
                    for gen_name, _, _ in generators_to_keep[:5]:  # Show first 5
                        logging.info(f"  - Keeping: {gen_name}")
                    if len(generators_to_keep) > 5:
                        logging.info(f"  ... and {len(generators_to_keep) - 5} more")
                else:
                    logging.debug(f"[{tech}] No generators have hydro cascade data - all will be deleted")
            else:
                generators_to_delete = generators_to_check
            
            generators_to_delete = list(set(generators_to_delete))
            logging.debug(f"generators_to_delete: {generators_to_delete}")
            idgens_to_delete = [idgen for _, idgen, _ in generators_to_delete]

            gen_names = [name for name, _, _ in generators_to_delete]
            idprofiles = [idprofile for _, _, idprofile in generators_to_delete if idprofile is not None]
            logging.debug(f"idgens_to_delete: {idgens_to_delete}")
            logging.debug(f"gen_names: {gen_names}")
            logging.debug(f"idprofiles: {idprofiles}")
            # Delete from GenConfiguration table first (child table)
            if idgens_to_delete:
                genconfig_statement = select(GenConfiguration).where(GenConfiguration.idGen.in_(idgens_to_delete))
                genconfig_results = session.exec(genconfig_statement)
                pv_genconfigs = genconfig_results.all()
                logging.info(f"length of idgens_to_delete: {len(idgens_to_delete)}")
                logging.info(f"Deleting pv_genconfigs {len(pv_genconfigs)} {tech} generators from GenConfiguration table")
                for genconfig in pv_genconfigs:
                    session.delete(genconfig)
                    logging.debug(f"genconfig: {genconfig}")
            
            # Delete from Gendata table after GenConfiguration (parent table)
            if idgens_to_delete:
                gendata_statement = select(Gendata).where(Gendata.idGen.in_(idgens_to_delete))
                results_gens = session.exec(gendata_statement)
                pv_gendata = results_gens.all()
                logging.debug(f"Deleting {len(pv_gendata)} {tech} generators from Gendata table")
                for gendata in pv_gendata:
                    logging.debug(f"gendata: {gendata}")
                    session.delete(gendata)
            
            # Delete profiles by matching generator names AND idProfile values
            profiles_deleted = 0
            if gen_names or idprofiles:
                from sqlalchemy import or_
                
                # Build conditions for both name matching and idProfile matching
                conditions = []
                if gen_names:
                    conditions.extend([Profiledata.name.like(f"%{name}%") for name in gen_names])
                if idprofiles:
                    conditions.append(Profiledata.idProfile.in_(idprofiles))
                
                profile_statement = select(Profiledata).where(or_(*conditions))
                profiles_to_delete = session.exec(profile_statement).all()
                
                logging.debug(f"Deleting {len(profiles_to_delete)} {tech} profiles from Profiledata table")
                for profile in profiles_to_delete:
                    session.delete(profile)
                profiles_deleted = len(profiles_to_delete)
            
            # Commit all changes at once
            session.commit()
            logging.info(f"Successfully deleted {tech} data: {len(pv_gendata) if idgens_to_delete else 0} generators, {profiles_deleted} profiles")

    def __define_scenario_path_bio_public_acceptance(self, tech) -> tuple[Path, Path]:
        bio_scenario = self.__settings.biodiversity_scenario
        public_acceptance_scenario = self.__settings.public_acceptance_scenario
        rcp_scenario = self.__settings.wind_rcp_scenario

        # # Scenario name used for file naming
        # scenario_name = f"{rcp_scenario}_bio_{bio_scenario}_public{public_acceptance_scenario}"

        # # Scenario path for folder structure
        # scenario_path = Path(f"{rcp_scenario}") / f"bio_{bio_scenario}" / f"public{public_acceptance_scenario}"

        # Full file paths
        base_dir = (
            Path(__file__).parent
            / f"{tech}_data"
            / rcp_scenario
            / f"bio_{bio_scenario}"
            / f"public{public_acceptance_scenario}"
        )

        profiles_path = base_dir / f"profiles.csv"
        gens_path = base_dir / f"gens.csv"

        return profiles_path, gens_path

    def __filter_categories_wind_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter wind data categories based on the scenario settings.
        Explanation of categories:
        capacity factor = cf
        cat1: cf >= 0.3
        cat2: 0.25 <= cf < 0.3
        cat3: 0.2 <= cf < 0.25
        cat4: 0.15 <= cf < 0.2
        cat5: cf < 0.15"""
        
        wind_categories = self.__settings.wind_data_scenario
        mapping = {
            'cat:(1-5)': ['cat1', 'cat2', 'cat3', 'cat4', 'cat5'],
            'cat:(1-4)': ['cat1', 'cat2', 'cat3', 'cat4'],
            'cat:(1-3)': ['cat1', 'cat2', 'cat3'],
            'cat:(1-2)': ['cat1', 'cat2'],
            'cat:(1-1)': ['cat1'],
        }
        if wind_categories in mapping:
            allowed_categories = mapping[wind_categories]

            # Extract the category token (e.g., 'cat1') from Name (support both 'name' and 'Name')
            name_col = 'name' if 'name' in df.columns else 'Name'
            cats = df[name_col].astype(str).str.extract(r'(cat[1-5])', expand=False)

            # Keep only rows whose extracted category is allowed
            mask = cats.isin(allowed_categories)
            filtered_df = df.loc[mask].reset_index(drop=True)
            return filtered_df
        else:
            logging.warning(f"Unknown wind data scenario '{wind_categories}', no filtering applied \n"
                            f"Possible options are: {list(mapping.keys())}")
            return df
    
    # def __define_scenario_path_bio_public_acceptance(self, tech) -> tuple[Path, Path]:
    #     bio_scenario = self.__settings.biodiversity_scenario
    #     public_acceptance_scenario = self.__settings.public_acceptance_scenario

    #     # Scenario name used for file naming
    #     scenario_name = f"bio_{bio_scenario}_public{public_acceptance_scenario}"

    #     # Scenario path for folder structure
    #     scenario_path = Path(f"bio_{bio_scenario}") / f"public{public_acceptance_scenario}"

    #     # Full file paths
    #     base_dir = Path(__file__).parent / f"{tech}_data" / scenario_path
    #     profiles_path = base_dir / f"{tech}_profiles_{scenario_name}.csv"
    #     gens_path = base_dir / f"{tech}_gens_{scenario_name}.csv"

    #     return profiles_path, gens_path
    def __add_new_tech_data_profiles(self, new_profiles_file_path, tech_type="None") -> None:
        # Load the data
        df = pd.read_csv(new_profiles_file_path)
        self.__add_new_tech_data_profiles_from_df(df, tech_type)
    
    def __add_new_tech_data_profiles_from_df(self, df: pd.DataFrame, tech_type="None") -> None:
        # if dataset is empty return
        if df.empty:
            logging.info(f"No profile data found, skipping profile upload.")
            return
        _, df_profiledata = self.__prepareData_profiles(df)
        self.__add_new_tech_data_profiles_preprocessed(df_profiledata, tech_type)
    
    def __add_new_tech_data_profiles_preprocessed(self, df_profiledata: pd.DataFrame, tech_type="None") -> None:
        # This method takes already preprocessed profile data
        if df_profiledata.empty:
            logging.info(f"No profile data found, skipping profile upload.")
            return



        # Get the next available idProfile to avoid duplicates
        with Session(self.__engine) as session:
            # Find the maximum existing idProfile
            max_id_result = session.exec(select(Profiledata.idProfile).order_by(Profiledata.idProfile.desc()).limit(1))
            max_id = max_id_result.first()
            next_id = (max_id + 1) if max_id is not None else 1
            
            # Push profile data to SQL table with new idProfile values
            for _, row in df_profiledata.iterrows():
                gen_name = row['name']
                profile = Profiledata(
                    idProfile=next_id,
                    name=gen_name,
                    Country='CH',
                    year=row['year'],
                    type=row['type'],
                    resolution=row['resolution'],
                    unit=row['unit'],
                    timeSeries=row['timeSeries']
                )
                session.add(profile)
                
                # Store the mapping from GenName to idProfile
                self.__genname_to_idprofile[gen_name] = next_id
                
                next_id += 1
            session.commit()
            logging.debug(f"[{tech_type}] Mapped {len(df_profiledata)} generators (GenName -> idProfile)")



    def __prepareData_profiles(self, df):
        # --------------------------------------------------------------------------------------------------------------------------------------------
        # hourly_values = df.columns[digit_mask]
        df = df.rename(columns=lambda x: int(x) if str(x).isdigit() else x)

        hourly_values = [col for col in df.columns if isinstance(col, (int, float, np.number))and pd.notna(col)]
        profiles_text = []  # List to store the processed text for each profile

        for _, row in df.iterrows():
            # Extract the hourly values
            profile_num = row[hourly_values].values

            # Remove any NaN values
            profile_num = profile_num[~pd.isna(profile_num)]

            # format the profile to have 4 decimal places
            profile_list = [round(x, 4) for x in profile_num]

            # Append the json list to the profiles_text list
            profiles_text.append(profile_list)
            # if profile_list empty # rasise error
            if not profile_list:
                raise ValueError(f"Profile data is empty for profile {row['Name']} with hourly values {hourly_values}")


        # create dfs
        # colNames of table in original function = {'idProfile','ProfileName','Year','Type','Resolution','Unit','timeSeries'};
        df_profileInfoTable = pd.DataFrame({
            'idProfile': df['Profile Number'],
            'ProfileName': df['Name'],
            'Year': df['year'],
            'Type': df['type'],
            'Resolution': df['resolution'],
            'Unit': df['unit'],
            'timeSeries': profiles_text
        })

        # --------------------------------------------------------------------------------------------------------------------------------------------
        # colNames of table for sql: colNames = {'idProfile','name','year','type','resolution','unit','timeSeries'};
        df_profiledata = pd.DataFrame({
            'idProfile': df['Profile Number'],
            'name': df['Name'],
            'year': df['year'],
            'type': df['type'],
            'resolution': df['resolution'],
            'unit': df['unit'],
            'timeSeries': profiles_text
        })
        # --------------------------------------------------------------------------------------------------------------------------------------------
        return df_profileInfoTable, df_profiledata
    

    

    def __prepareData_profiles(self, df):
        # --------------------------------------------------------------------------------------------------------------------------------------------
        # Identify hourly columns - exclude metadata columns and find numeric column names
        metadata_columns = ['Profile Number', 'Name', 'Country', 'year', 'type', 'resolution', 'unit']
        hourly_values = []
        
        for col in df.columns:
            if col not in metadata_columns:
                try:
                    # Try to convert column name to int - if successful, it's a time column
                    int(col)
                    hourly_values.append(col)
                except (ValueError, TypeError):
                    # Skip non-numeric column names
                    pass
        
        profiles_text = []  # List to store the processed text for each profile

        for _, row in df.iterrows():
            # Extract the hourly values
            profile_num = row[hourly_values].values

            # Remove any NaN values
            profile_num = profile_num[~pd.isna(profile_num)]

            # format the profile to have 4 decimal places
            profile_list = [round(float(x), 4) for x in profile_num]

            # Append the json list to the profiles_text list
            profiles_text.append(profile_list)


        # create dfs
        # colNames of table in original function = {'idProfile','ProfileName','Year','Type','Resolution','Unit','timeSeries'};
        df_profileInfoTable = pd.DataFrame({
            'idProfile': df['Profile Number'],
            'ProfileName': df['Name'],
            'Year': df['year'],
            'Type': df['type'],
            'Resolution': df['resolution'],
            'Unit': df['unit'],
            'timeSeries': profiles_text
        })

        # --------------------------------------------------------------------------------------------------------------------------------------------
        # colNames of table for sql: colNames = {'idProfile','name','year','type','resolution','unit','timeSeries'};
        df_profiledata = pd.DataFrame({
            'idProfile': df['Profile Number'],
            'name': df['Name'],
            'year': df['year'],
            'type': df['type'],
            'resolution': df['resolution'],
            'unit': df['unit'],
            'timeSeries': profiles_text
        })
        # --------------------------------------------------------------------------------------------------------------------------------------------
        return df_profileInfoTable, df_profiledata
    
    def __upload_gendata(self, new_gendata_file_path, tech_type=None):
        df = pd.read_csv(new_gendata_file_path)
        self.__upload_gendata_from_df(df, tech_type)
    
    def __upload_gendata_from_df(self, df: pd.DataFrame, tech_type=None):
        if df.empty:
            logging.info("No Gendata found in the provided file, skipping Gendata upload.")
            return
        
        logging.debug(f"Uploading {len(df)} {tech_type} generators to Gendata table")
        with Session(self.__engine) as session:
            # Find the maximum existing idGen to avoid duplicates
            max_id_result = session.exec(select(Gendata.idGen).order_by(Gendata.idGen.desc()).limit(1))
            max_id = max_id_result.first()
            next_id = (max_id + 1) if max_id is not None else 1
            
            for _, row in df.iterrows():
                gendata = Gendata(
                    idGen=next_id,
                    GenName=row["Gen_ID"] if "Gen_ID" in row and not pd.isna(row["Gen_ID"]) else row["name"],
                    GenType=row["UnitType"] if not pd.isna(row["UnitType"]) else None,
                    Technology=row["SubType"] if not pd.isna(row["SubType"]) else None,
                    UnitType=row["UnitType_2"] if not pd.isna(row["UnitType"]) else None,
                    StartYr=float(row["start_year"]) if not pd.isna(row["start_year"]) else None,
                    EndYr=float(row["end_year (50yrNucl)"]) if not pd.isna(row["end_year (50yrNucl)"]) else None,
                    GenEffic=float(row["Efficiency/HeatRate (MWh elec / MWh heat)"]) if "Efficiency/HeatRate (MWh elec / MWh heat)" in row and not pd.isna(row["Efficiency/HeatRate (MWh elec / MWh heat)"]) else None,
                    CO2Rate=float(row["CO2 Emission Rate (ton CO2 / MWh elec)"]) if "CO2 Emission Rate (ton CO2 / MWh elec)" in row and not pd.isna(row["CO2 Emission Rate (ton CO2 / MWh elec)"]) else None,
                    eta_dis=float(row["eta_dis (fraction)"]) if "eta_dis (fraction)" in row and not pd.isna(row["eta_dis (fraction)"]) else None,
                    eta_ch=float(row["eta_ch (fraction)"]) if "eta_ch (fraction)" in row and not pd.isna(row["eta_ch (fraction)"]) else None,
                    RU=float(row["Ramp Rate Up (MW/hr)"]) if "Ramp Rate Up (MW/hr)" in row and not pd.isna(row["Ramp Rate Up (MW/hr)"]) else None,
                    RD=float(row["Ramp Rate Down (MW/hr)"]) if "Ramp Rate Down (MW/hr)" in row and not pd.isna(row["Ramp Rate Down (MW/hr)"]) else None,
                    RU_start=float(row["Ramp Rate Up StartUp (MW/hr)"]) if "Ramp Rate Up StartUp (MW/hr)" in row and not pd.isna(row["Ramp Rate Up StartUp (MW/hr)"]) else None,
                    RD_shutd=float(row["Ramp Rate Down ShutDown (MW/hr)"]) if "Ramp Rate Down ShutDown (MW/hr)" in row and not pd.isna(row["Ramp Rate Down ShutDown (MW/hr)"]) else None,
                    UT=int(row["Min Up Time (hr)"]) if "Min Up Time (hr)" in row and not pd.isna(row["Min Up Time (hr)"]) else None,
                    DT=int(row["Min Down Time (hr)"]) if "Min Down Time (hr)" in row and not pd.isna(row["Min Down Time (hr)"]) else None,
                    Pini=float(row["Initial Pgen at T0 (MW)"]) if "Initial Pgen at T0 (MW)" in row and not pd.isna(row["Initial Pgen at T0 (MW)"]) else None,
                    Tini=float(row["Initial Up/Down Time at T0 (hr)"]) if "Initial Up/Down Time at T0 (hr)" in row and not pd.isna(row["Initial Up/Down Time at T0 (hr)"]) else None,
                    meanErrorForecast24h=float(row["Mean Error Forecast 24h"]) if "Mean Error Forecast 24h" in row and not pd.isna(row["Mean Error Forecast 24h"]) else None,
                    sigmaErrorForecast24h=float(row["Sigma Error Forecast 24 hr"]) if "Sigma Error Forecast 24 hr" in row and not pd.isna(row["Sigma Error Forecast 24 hr"]) else None,
                    Lifetime=float(row["Lifetime (yr)"]) if "Lifetime (yr)" in row and not pd.isna(row["Lifetime (yr)"]) else None,
                    # RoR-specific fields - explicitly set to None if not provided
                    WASTA=int(row["WASTA"]) if "WASTA" in row and not pd.isna(row["WASTA"]) else None,
                    QT=float(row["QT"]) if "QT" in row and not pd.isna(row["QT"]) else None,
                    QP=float(row["QP"]) if "QP" in row and not pd.isna(row["QP"]) else None,
                    idResUp=str(row["idResUp"]) if "idResUp" in row and not pd.isna(row["idResUp"]) else None,
                    idResLow=str(row["idResLow"]) if "idResLow" in row and not pd.isna(row["idResLow"]) else None,
                    idCasc=int(row["idCasc"]) if "idCasc" in row and not pd.isna(row["idCasc"]) else None,
                    HydroCHAgg=int(row["HydroCHAgg"]) if "HydroCHAgg" in row and not pd.isna(row["HydroCHAgg"]) else None,
                    HydroCHCasc=int(row["HydroCHCasc"]) if "HydroCHCasc" in row and not pd.isna(row["HydroCHCasc"]) else None,
                )
                session.add(gendata)
                next_id += 1
            session.commit()
            logging.debug(f"[OK] Uploaded {len(df)} {tech_type} generators to Gendata table")
        
        
    def __upload_genconfiguration(self, new_gens_file_path, tech_type = None):
        df = pd.read_csv(new_gens_file_path)
        self.__upload_genconfiguration_from_df(df, tech_type)
    
    def __upload_genconfiguration_from_df(self, df: pd.DataFrame, tech_type = None):
        if df.empty:
            logging.info("No GenConfiguration data found in the provided file, skipping GenConfiguration upload.")
            return
        
        logging.debug(f"Processing {len(df)} {tech_type} configurations")
        with Session(self.__engine) as session:
            # First, load the Busdata table to create a mapping from NodeCode to idBus
            busdata_statement = select(Busdata.idBus, Busdata.SwissgridNodeCode)
            busdata_results = session.exec(busdata_statement).all()
            
            # Create mapping with trimmed keys to handle whitespace issues
            nodecode_to_idbus = {}
            for row in busdata_results:
                if row.SwissgridNodeCode:
                    nodecode_to_idbus[row.SwissgridNodeCode.strip()] = row.idBus
            
            # Get unique NodeCodes from CSV for diagnostic purposes
            csv_nodecodes = set(str(nc).strip() for nc in df['NodeCode'].unique() if pd.notna(nc))
            db_nodecodes = set(nodecode_to_idbus.keys())
            missing_nodecodes = csv_nodecodes - db_nodecodes
            
            if missing_nodecodes:
                logging.warning(f"[{tech_type}] {len(missing_nodecodes)}/{len(csv_nodecodes)} NodeCodes from CSV not found in Busdata")
            else:
                logging.debug(f"[{tech_type}] All {len(csv_nodecodes)} CSV NodeCodes matched in Busdata")
            
            # Load Gendata table to get idGen based on GenName and StartYr
            gendata_statement = select(Gendata.idGen, Gendata.GenName, Gendata.StartYr)
            gendata_results = session.exec(gendata_statement)
            genname_startyear_to_idgen = {(row.GenName, row.StartYr): row.idGen for row in gendata_results}
            
            successful_entries = 0
            skipped_entries = 0
            cascade_generators = 0  # Track generators without idProfile (part of cascades)
            for index, row in df.iterrows():
                cascade_gen = False
                
                # Get idBus from NodeCode mapping (trim whitespace)
                node_code = str(row["NodeCode"]).strip() if pd.notna(row["NodeCode"]) else None
                id_bus = nodecode_to_idbus.get(node_code) if node_code else None
                
                if id_bus is None:
                    logging.warning(f"Could not find idBus for NodeCode {node_code}, skipping generator {row['name']}")
                    skipped_entries += 1
                    continue
                
                
                # Calculate idGenConfig based on start_year
                start_year = row["start_year"] if not pd.isna(row["start_year"]) else None
                if start_year == 2020:
                    id_gen_config = 2
                elif start_year == 2030:
                    id_gen_config = 3
                elif start_year == 2040:
                    id_gen_config = 4
                elif start_year == 2050:
                    id_gen_config = 5
                else:
                    # Default to 2 for other years (e.g., 2012, 2015, etc.)
                    id_gen_config = 2
                
                
                # Get idGen from Gendata table based on GenName and StartYr
                # Use Gen_ID if available, otherwise fall back to name
                gen_name = row["Gen_ID"] if "Gen_ID" in row and not pd.isna(row["Gen_ID"]) else row["name"]
                id_gen = genname_startyear_to_idgen.get((gen_name, start_year))
                
                if id_gen is None:
                    logging.warning(f"Could not find idGen for GenName {gen_name} with StartYr {start_year}, skipping")
                    skipped_entries += 1
                    continue
                
                
                # Find matching profile using GenName as the key identifier
                # For profile matching, use the name field (not Gen_ID)
                profile_lookup_name = row["name"]
                
                # Check if CSV has idProfile specified
                csv_idprofile = row.get('idProfile')
                if pd.isna(csv_idprofile) or csv_idprofile == '' or csv_idprofile == 0:
                    # No profile in CSV - need to find matching profile
                    # For PV-roof: match using GenName AND year
                    # For other technologies (RoR, etc.): use existing logic (no year matching)
                    
                    if tech_type == 'PV-roof':
                        # First try to use the mapping from the current upload session
                        if profile_lookup_name in self.__genname_to_idprofile:
                            profile_id = self.__genname_to_idprofile[profile_lookup_name]
                        else:
                            # Use no_autoflush to prevent premature database commits during query
                            with session.no_autoflush:
                                # For PV-roof, match using both GenName and year
                                profile_name = profile_lookup_name
                                matching_profiles_statement = select(Profiledata.idProfile, Profiledata.name, Profiledata.year).where(
                                    (Profiledata.name == profile_name) &
                                    (Profiledata.year == start_year)
                                )
                                matching_profiles = session.exec(matching_profiles_statement).all()
                                
                                # If no exact match found, try matching using Gen_ID with year
                                if not matching_profiles and "Gen_ID" in row and not pd.isna(row["Gen_ID"]):
                                    gen_id = row["Gen_ID"]
                                    matching_profiles_statement = select(Profiledata.idProfile, Profiledata.name, Profiledata.year).where(
                                        (Profiledata.name.like(f"%{gen_id}%")) &
                                        (Profiledata.year == start_year)
                                    )
                                    matching_profiles = session.exec(matching_profiles_statement).all()
                            
                            # If multiple matches found, use the one with the biggest idProfile number
                            if matching_profiles:
                                profile_id = max(matching_profiles, key=lambda x: x.idProfile).idProfile
                            else:
                                # No matching profile found in database - upload with NULL idProfile
                                logging.debug(f"[{tech_type}] No matching profile for GenName '{profile_lookup_name}' with year {start_year} (Gen_ID: {row.get('Gen_ID', 'N/A')}), uploading with NULL idProfile")
                                profile_id = None
                                cascade_generators += 1
                                cascade_gen = True
                    else:
                        # For non-PV-roof technologies (RoR, etc.): this is a cascade generator
                        logging.debug(f"No idProfile in CSV for GenName '{profile_lookup_name}' (Gen_ID: {row.get('Gen_ID', 'N/A')}), uploading with NULL idProfile")
                        profile_id = None
                        cascade_generators += 1
                        cascade_gen = True
                else:
                    # First try to use the mapping from the current upload session
                    if profile_lookup_name in self.__genname_to_idprofile:
                        profile_id = self.__genname_to_idprofile[profile_lookup_name]
                    else:
                        # Use no_autoflush to prevent premature database commits during query
                        with session.no_autoflush:
                            # First try exact match with generator name
                            profile_name = profile_lookup_name
                            matching_profiles_statement = select(Profiledata.idProfile, Profiledata.name).where(
                                Profiledata.name == profile_name
                            )
                            matching_profiles = session.exec(matching_profiles_statement).all()
                            
                            # If no exact match found, try matching using Gen_ID (profiles contain Gen_ID in their name)
                            if not matching_profiles and "Gen_ID" in row and not pd.isna(row["Gen_ID"]):
                                gen_id = row["Gen_ID"]
                                matching_profiles_statement = select(Profiledata.idProfile, Profiledata.name).where(
                                    Profiledata.name.like(f"%{gen_id}%")
                                )
                                matching_profiles = session.exec(matching_profiles_statement).all()
                        
                        # If multiple matches found, use the one with the biggest idProfile number
                        if matching_profiles:
                            profile_id = max(matching_profiles, key=lambda x: x.idProfile).idProfile
                        else:
                            # No matching profile found in database - upload with NULL idProfile
                            logging.debug(f"No matching profile for GenName '{profile_lookup_name}' (Gen_ID: {row.get('Gen_ID', 'N/A')}), uploading with NULL idProfile")
                            profile_id = None
                            cascade_generators += 1
                            cascade_gen = True
                
                if not cascade_gen:
                    gen_config = GenConfiguration(
                        idGenConfig=id_gen_config,
                        idBus=id_bus,
                        idGen=id_gen,
                        GenName=row["Gen_ID"] if "Gen_ID" in row and not pd.isna(row["Gen_ID"]) else row["name"],
                        idProfile=profile_id,
                        CandidateUnit=int(row["Candidate Unit"]) if not pd.isna(row["Candidate Unit"]) else None,
                        Pmax=float(row["P_gen_max in 2015 (MW)"]) if not pd.isna(row["P_gen_max in 2015 (MW)"]) else None,
                        Pmin=float(row["Pmin (MW)"]) if not pd.isna(row["Pmin (MW)"]) else None,
                        Qmax=float(row["Qmax"]) if "Qmax" in row and not pd.isna(row["Qmax"]) else None,
                        Qmin=float(row["Qmin"]) if "Qmin" in row and not pd.isna(row["Qmin"]) else None,
                        Emax=float(row["Emax Jared"]) if not pd.isna(row["Emax Jared"]) else None,
                        Emin=float(row["Emin"]) if not pd.isna(row["Emin"]) else None,
                        E_ini=float(row["E_ini (fraction)"]) if not pd.isna(row["E_ini (fraction)"]) else None,
                        VOM_Cost=float(row["non Fuel VOM (Euro/MWh)"]) if not pd.isna(row["non Fuel VOM (Euro/MWh)"]) else None,
                        FOM_Cost=float(row["Fixed O&M Costs (Euro/MW/yr)"]) if not pd.isna(row["Fixed O&M Costs (Euro/MW/yr)"]) else None,
                        InvCost=float(row["Investment Cost (Euro/MWel/yr)"]) if not pd.isna(row["Investment Cost (Euro/MWel/yr)"]) else None,
                        InvCost_E=float(row["Investment Cost for Energy (Euro/MWh-el/yr)"]) if not pd.isna(row["Investment Cost for Energy (Euro/MWh-el/yr)"]) else None,
                        InvCost_Charge=float(row["Investment Cost for Charging (Euro/MWe/yr)"]) if "Investment Cost for Charging (Euro/MWe/yr)" in row and not pd.isna(row["Investment Cost for Charging (Euro/MWe/yr)"]) else None,
                        StartCost=float(row["StartUp Cost (Euro/MW/start)"]) if not pd.isna(row["StartUp Cost (Euro/MW/start)"]) else None,
                        TotVarCost=float(row["2018 Total Variable Cost (Euro/MWh-el)"]) if "2018 Total Variable Cost (Euro/MWh-el)" in row and not pd.isna(row["2018 Total Variable Cost (Euro/MWh-el)"]) else None,
                        FuelType=row["Fuel Type"] if not pd.isna(row["Fuel Type"]) else None,
                        CO2Type=row["CO2 Price Indicator"] if "CO2 Price Indicator" in row and not pd.isna(row["CO2 Price Indicator"]) else None,
                        status=float(row["Status"]) if not pd.isna(row["Status"]) else None,
                        HedgeRatio=float(row["HedgeRatio"]) if not pd.isna(row["HedgeRatio"]) else None,
                    )
                    session.add(gen_config)
                    successful_entries += 1
            
            session.commit()
            
            total_attempted = successful_entries + skipped_entries
            if skipped_entries > 0:
                logging.warning(f"[{tech_type}] Skipped {skipped_entries}/{total_attempted} GenConfiguration entries due to missing data")
            if cascade_generators > 0:
                logging.info(f"[{tech_type}] {cascade_generators} generators are part of a hydro cascade (no individual profiles)")
            logging.info(f"[OK] Uploaded {successful_entries}/{total_attempted} {tech_type} configurations")


    

        #make sure all generators are reordered correctly
    
    def __verify_idprofile_uniqueness(self, tech_type: str = "Unknown") -> None:
        """Verify that all idProfile numbers in the database are unique and log results"""
        logging.debug(f"[{tech_type}] Verifying idProfile uniqueness...")
        
        with Session(self.__engine) as session:
            # Get all idProfile values from Profiledata
            statement = select(Profiledata.idProfile)
            results = session.exec(statement).all()
            
            if not results:
                logging.warning(f"[{tech_type}] No profiles found in database")
                return
            
            # Check for duplicates
            id_profiles = list(results)
            unique_ids = set(id_profiles)
            
            if len(id_profiles) == len(unique_ids):
                logging.debug(f"[{tech_type}] ✓ All {len(id_profiles)} idProfile numbers are unique")
            else:
                # Find duplicates
                from collections import Counter
                id_counter = Counter(id_profiles)
                duplicates = {id_prof: count for id_prof, count in id_counter.items() if count > 1}
                
                logging.error(f"[{tech_type}] ✗ Found {len(duplicates)} duplicate idProfile numbers:")
                for id_prof, count in duplicates.items():
                    logging.error(f"  - idProfile {id_prof} appears {count} times")
                    
                    # Get the names of profiles with this duplicate idProfile
                    dup_statement = select(Profiledata.name).where(Profiledata.idProfile == id_prof)
                    dup_names = session.exec(dup_statement).all()
                    logging.error(f"    Profile names: {list(dup_names)}")
    
    def __verify_ror_upload(self, gens_df: pd.DataFrame, profiles_df: pd.DataFrame) -> None:
        """Verify RoR data upload integrity"""
        logging.debug("=" * 60)
        logging.debug("[RoR] Verifying upload integrity...")
        
        # Get list of generator names from the CSV to verify against
        # Use Gen_ID if available, otherwise fall back to name
        expected_gen_names = set(
            row['Gen_ID'] if 'Gen_ID' in gens_df.columns and pd.notna(row.get('Gen_ID')) else row['name']
            for _, row in gens_df.iterrows()
        )
        expected_count = len(gens_df)
        
        with Session(self.__engine) as session:
            # Check 1a: Verify generators in Gendata table
            gendata_statement = select(Gendata.GenName).where(Gendata.Technology == 'RoR')
            all_gendata_names = set(session.exec(gendata_statement).all())
            gendata_found = expected_gen_names & all_gendata_names
            gendata_missing = expected_gen_names - all_gendata_names
            
            logging.debug(f"[RoR] Gendata table: {len(gendata_found)}/{expected_count} generators found")
            if gendata_missing:
                logging.warning(f"[RoR]   {len(gendata_missing)} missing from Gendata: {list(gendata_missing)[:5]}...")
            
            # Check 1b: Verify generators in GenConfiguration table
            genconfig_statement = select(GenConfiguration.GenName).distinct().join(
                Gendata, GenConfiguration.idGen == Gendata.idGen
            ).where(Gendata.Technology == 'RoR')
            all_genconfig_names = set(session.exec(genconfig_statement).all())
            genconfig_found = expected_gen_names & all_genconfig_names
            genconfig_missing = expected_gen_names - all_genconfig_names
            
            logging.debug(f"[RoR] GenConfiguration table: {len(genconfig_found)}/{expected_count} generators found")
            if genconfig_missing:
                logging.warning(f"[RoR]   {len(genconfig_missing)} missing from GenConfiguration")
                # Show first 10 missing
                for name in list(genconfig_missing)[:10]:
                    logging.warning(f"     - '{name}'")
                if len(genconfig_missing) > 10:
                    logging.warning(f"     ... and {len(genconfig_missing) - 10} more")
            
            # Overall verdict
            if len(gendata_found) == expected_count and len(genconfig_found) == expected_count:
                logging.debug(f"[RoR] ✓ All {expected_count} generators verified in both tables")
            else:
                logging.error(f"[RoR] ✗ Upload incomplete: Gendata={len(gendata_found)}, GenConfiguration={len(genconfig_found)}, Expected={expected_count}")
            
            # Check 2: Verify non-zero capacity (only for generators from CSV)
            # Create set of (GenName, StartYr) tuples from CSV for exact matching
            # Use Gen_ID if available, otherwise fall back to name
            expected_gen_name_year = set(
                (row['Gen_ID'] if 'Gen_ID' in gens_df.columns and pd.notna(row.get('Gen_ID')) else row['name'], row['start_year']) 
                for _, row in gens_df.iterrows()
            )
            
            genconfig_statement = select(GenConfiguration, Gendata.StartYr).join(
                Gendata, GenConfiguration.idGen == Gendata.idGen
            ).where(
                (Gendata.Technology == 'RoR') &
                (Gendata.GenName.like('CH%'))
            )
            all_ror_genconfigs = session.exec(genconfig_statement).all()
            
            # Filter to only the configurations from CSV (matching both name and year)
            uploaded_genconfigs = []
            for gc, start_yr in all_ror_genconfigs:
                if (gc.GenName, start_yr) in expected_gen_name_year:
                    uploaded_genconfigs.append(gc)
            
            zero_capacity_gens = [gc for gc in uploaded_genconfigs if gc.Pmax is None or gc.Pmax <= 0]
            total_capacity = sum(gc.Pmax for gc in uploaded_genconfigs if gc.Pmax is not None and gc.Pmax > 0)
            
            # Calculate capacity per year
            capacity_by_year = {}
            for gc, start_yr in all_ror_genconfigs:
                if (gc.GenName, start_yr) in expected_gen_name_year and gc.Pmax is not None and gc.Pmax > 0:
                    year = int(start_yr)
                    capacity_by_year[year] = capacity_by_year.get(year, 0) + gc.Pmax
            
            if zero_capacity_gens:
                logging.warning(f"[RoR] ⚠ Found {len(zero_capacity_gens)} generators with zero/null capacity:")
                for gc in zero_capacity_gens[:5]:  # Show first 5
                    logging.warning(f"  - {gc.GenName}: Pmax={gc.Pmax}")
                if len(zero_capacity_gens) > 5:
                    logging.warning(f"  ... and {len(zero_capacity_gens) - 5} more")
            
            # Check 3: Verify idProfile uniqueness
            self.__verify_idprofile_uniqueness(tech_type="RoR")
            
            # Check 4: Verify generators without profiles (expected)
            gens_with_no_profile = [gc for gc in uploaded_genconfigs if gc.idProfile == 0 or gc.idProfile is None]
            
            # Summary statistics
            logging.debug(f"[RoR] Upload summary:")
            logging.debug(f"  - Generators in Gendata: {len(gendata_found)}/{expected_count}")
            logging.debug(f"  - Generators in GenConfiguration: {len(genconfig_found)}/{expected_count}")
            logging.debug(f"  - Profiles uploaded: {len(profiles_df)}")
            logging.debug(f"  - Total capacity: {total_capacity:.2f} MW")
            for year in sorted(capacity_by_year.keys()):
                logging.debug(f"    * Year {year}: {capacity_by_year[year]:.2f} MW")
            logging.debug(f"  - Generators with zero capacity: {len(zero_capacity_gens)}")
            logging.debug(f"  - Generators without profiles (skipped): {len(gens_with_no_profile)}")
        
        logging.debug("=" * 60)
    
    def __reorder_generators(self):
        with Session(self.__engine) as session:
            # Get all generators ordered by idGen
            gendata_statement = select(Gendata).order_by(Gendata.idGen)
            all_generators = session.exec(gendata_statement).all()
            
            if not all_generators:
                logging.debug("No generators found in Gendata table")
                return
            
            # Check if idGen is continuous
            expected_id = 1
            gaps_found = []
            
            for gen in all_generators:
                if gen.idGen != expected_id:
                    gaps_found.append((expected_id, gen.idGen - 1))
                expected_id = gen.idGen + 1
            
            if gaps_found:
                logging.debug(f"Found {len(gaps_found)} gaps in idGen, reordering...")

                # Create a mapping from old idGen to new idGen
                old_to_new_idgen = {}
                new_id = 1

                for gen in all_generators:
                    old_to_new_idgen[gen.idGen] = new_id
                    new_id += 1

                # Disable foreign key checks temporarily
                session.exec(text("SET foreign_key_checks = 0"))
                
                # Update Gendata table with new continuous idGen values
                for gen in all_generators:
                    old_id = gen.idGen
                    new_id = old_to_new_idgen[old_id]
                    if old_id != new_id:
                        gen.idGen = new_id

                # Update GenConfiguration table with new idGen values
                genconfig_statement = select(GenConfiguration)
                all_genconfigs = session.exec(genconfig_statement).all()
                
                for genconfig in all_genconfigs:
                    old_idgen = genconfig.idGen
                    if old_idgen in old_to_new_idgen:
                        genconfig.idGen = old_to_new_idgen[old_idgen]

                # Re-enable foreign key checks
                session.exec(text("SET foreign_key_checks = 1"))
                session.commit()
                logging.debug(f"[OK] Reordered {len(all_generators)} generators")
            else:
                logging.debug("idGen sequence is already continuous")

    
    def __reorder_profiles(self):
        
        with Session(self.__engine) as session:
            # Get all profiles ordered by idProfile
            profiledata_statement = select(Profiledata).order_by(Profiledata.idProfile)
            all_profiles = session.exec(profiledata_statement).all()
            
            if not all_profiles:
                logging.debug("No profiles found in Profiledata table")
                return
            
            # Check for gaps in idProfile sequence
            expected_id = 1
            gaps = []
            for profile in all_profiles:
                while expected_id < profile.idProfile:
                    gaps.append(expected_id)
                    expected_id += 1
                expected_id = profile.idProfile + 1

            if gaps:
                logging.debug(f"Found {len(gaps)} gaps in idProfile, filling from end...")

                # Disable foreign key checks temporarily
                session.exec(text("SET foreign_key_checks = 0"))

                # Fill gaps with last profiles
                profiles_to_move = all_profiles[-len(gaps):] if len(gaps) > 0 else []
                old_to_new_mapping = {}
                
                for gap_id, profile in zip(gaps, profiles_to_move):
                    old_id = profile.idProfile
                    old_to_new_mapping[old_id] = gap_id
                    profile.idProfile = gap_id

                # Update GenConfiguration table for moved profiles
                genconfig_statement = select(GenConfiguration)
                all_genconfigs = session.exec(genconfig_statement).all()
                
                updated_count = 0
                for genconfig in all_genconfigs:
                    if genconfig.idProfile in old_to_new_mapping:
                        old_id = genconfig.idProfile
                        new_id = old_to_new_mapping[old_id]
                        genconfig.idProfile = new_id
                        updated_count += 1
                
                logging.debug(f"Total GenConfiguration entries updated: {updated_count}")

                # Re-enable foreign key checks
                session.exec(text("SET foreign_key_checks = 1"))
                session.commit()
                logging.debug(f"[OK] Filled {len(gaps)} gaps in idProfile")
            else:
                logging.debug("idProfile sequence is already continuous")

    def __verify_database_content(self, tech_type: str) -> None:
        """Verify the capacity and generation in database for each year for a given technology"""
        logging.info(f"[{tech_type}] Verifying database content - capacity per year")
        
        with Session(self.__engine) as session:
            # Query to get capacity per year from database
            # Join Gendata (for StartYr and Technology) with GenConfiguration (for Pmax)
            capacity_query = select(
                Gendata.StartYr,
                func.sum(GenConfiguration.Pmax).label('total_capacity')
            ).join(
                GenConfiguration, Gendata.idGen == GenConfiguration.idGen
            ).where(
                (Gendata.Technology == tech_type) &
                (Gendata.GenName.like('CH%'))
            ).group_by(
                Gendata.StartYr
            ).order_by(
                Gendata.StartYr
            )
            
            capacity_results = session.exec(capacity_query).all()
            
            if capacity_results:
                logging.info(f"[{tech_type}] Database capacity by year (MW):")
                for row in capacity_results:
                    year = int(row.StartYr) if row.StartYr else 'Unknown'
                    capacity = row.total_capacity if row.total_capacity else 0
                    logging.info(f"  Year {year}: {capacity:.2f} MW")
            else:
                logging.warning(f"[{tech_type}] No capacity data found in database")

    
class Gendata(SQLModel, table=True):
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
    WASTA: int | None = Field(default=None)
    QT: float | None = Field(default=None)
    QP: float | None = Field(default=None)
    idResUp: str | None = Field(default=None, max_length=100)
    idResLow: str | None = Field(default=None, max_length=100)
    idCasc: int | None = Field(default=None)
    HydroCHAgg: int | None = Field(default=None)
    HydroCHCasc: int | None = Field(default=None)
    

class GenConfiguration(SQLModel, table=True):
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

class Busdata(SQLModel, table=True):
    idBus: int = Field(primary_key=True, description="primary identifier for buses")
    SwissgridNodeCode: str | None = Field(default=None, max_length=100, description="Swissgrid node code")
    Country: str | None = Field(default=None, max_length=45, description="country code")

class Profiledata(SQLModel, table=True):
    idProfile: int = Field(primary_key=True, description="primary identifier for profiles")
    name: str | None = Field(default=None, max_length=100, description="descriptive name for given profile")
    Country: str | None = Field(default=None, max_length=45)
    year: int | None = Field(default=None, description="year associated with given profile")
    type: str | None = Field(default=None, max_length=45, description="defines the type of profile (Load, Generation, Water Inflow, Refueling/Maintenance Status, Reserve Requirement, etc.)")
    resolution: str | None = Field(default=None, max_length=45, description="# hrs each entry in the profile covers (1 = hourly, 24 = daily, 168 = weekly, etc.)")
    unit: str | None = Field(default="MW", max_length=45, description="associated units of the given profile")
    timeSeries: dict = Field(default_factory=dict, sa_column=Column(JSON), description="time series values of the profile")