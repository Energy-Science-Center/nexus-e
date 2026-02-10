import copy
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import itertools
import json
import logging
import os
import re
import subprocess
from typing import Any

import mysql
import mysql.connector
from nexus_e_interface import Plugin, Scenario
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

class NtcType(Enum):
    """
    ref_grid:
    NTCs are based on the reference grid, fixed values, do not change for 2040
    and 2050 (no upgrades)
    
    Expansion1:
    NTCs are based on the reference grid + real investment candidates

    Expansion2:
    NTCs are based on the reference grid + real investment candidates + concept
    investment candidates

    modelResults:
    NTCs are based on the modelling results from TYNDP24 (depend on scenario)
    """

    ref_grid = "ref_grid"
    Expansion1 = "Expansion1"
    Expansion2 = "Expansion2"
    modelResults = "modelResults"

@dataclass
class Config:
    excel_file_path: str = "src/plugins/data/source_excel/input.xlsx"
    push_to_mysql: bool = True
    database_name: str = "project_date_version"
    database_author: str = "FirstName LastName"
    host: str = "localhost"
    user: str = "root"
    password: str = "root"
    dump_file_path: str = "src/plugins/upload_scenario/schemas/Dump_STRUC_clean_v9.sql"

    include_flex_params: bool = False
    demand_profiles_path: str = "src/data/source_excel/Nexuse_DB-Input_v47_TYNDP22-GA08_CentIvPV_base_DemandProfiles"

    include_tyndp24: bool = False
    tyndp24_data_path: str = "src/data/TYNDP24_datasets/TYNDP24_nbc"
    tyndp24_scope: str = "nbc"
    tyndp24_policy: str = "DE"
    tyndp24_climate_year: int = 2009

    ntc_type: str | NtcType = "modelResults"

    use_new_line_types: bool = True
    preset_line_types: bool = True
    new_line_types: str = "NTC"

    config_years: list[str] = field(
        default_factory=lambda: 
        ["2018", "2020", "2030", "2040", "2050"]
    )
    config_years_int: list[int] = field(
        default_factory=lambda: 
        [2018, 2020, 2030, 2040, 2050]
    )

    create_excel: bool = False
    excel_file_target_path: str = "output.xlsx"

    include_branch_sheet: bool = True

    update_only_specific_columns: bool = False
    columns_to_update: dict[str, list[str]] = field(
        default_factory=lambda:
        {"branch": ["line_id", "start_year", "S_max_win (MVA)"]}
    )

class NexusePlugin(Plugin):
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Config())

    def __init__(self, parameters: dict, scenario: Scenario | None = None):
        self.settings = Config(**parameters)

    def run(self) -> dict[str, Any]:
        output = {}
        # Load the data
        self.data_loader = DataLoader(
            excel_file_path=self.settings.excel_file_path
        )
        logging.info(f"Using Excel file: {self.settings.excel_file_path}")
        logging.info("Load data from Excel file...")
        inputDB = self.data_loader.load_data()
        logging.info("Done")

        # add TYNDP24 data if include_tyndp24 is True
        if self.settings.include_tyndp24:
            self.data_merger = DataMerger(
                inputDB_excel=inputDB,
                tyndp24_data_path=self.settings.tyndp24_data_path,
                tyndp24_policy=self.settings.tyndp24_policy,
                tyndp24_climate_year=self.settings.tyndp24_climate_year,
                include_branch_sheet=self.settings.include_branch_sheet,
                preset_line_types=self.settings.preset_line_types,
                new_line_types=self.settings.new_line_types,
                update_only_specific_columns=self.settings.update_only_specific_columns,
                columns_to_update=self.settings.columns_to_update,
                ntc_type=self.settings.ntc_type,
                tyndp24_scope=self.settings.tyndp24_scope
            )
            logging.info(f"Using TYNDP24 data from {self.settings.tyndp24_data_path}")
            logging.info("Inject TYNDP24 data...")
            inputDB = self.data_merger.upsert_tyndp24_data()
            logging.info("Done")
        
        # create excel file if create_excel is True
        if self.settings.create_excel:
            logging.info(f"Using Excel file: {self.settings.excel_file_target_path}")
            logging.info("Create Excel file...")
            self.__create_excel(self.settings.excel_file_target_path, inputDB)
            logging.info("Done")

        # prepare data
        schema_version = self.settings.dump_file_path.strip(".sql").split("_")[-1]
        version_number = float(schema_version.replace("v", ""))
        self.data_preparer = DataPreparer(
            inputDB=inputDB,
            excel_file_path=self.settings.excel_file_path,
            database_author=self.settings.database_author,
            use_new_line_types=self.settings.use_new_line_types,
            demand_profiles_path=self.settings.demand_profiles_path,
            include_flex_params=self.settings.include_flex_params,
            schema_version=version_number,
            config_years_int=self.settings.config_years_int,
            config_years=self.settings.config_years
        )
        logging.info("Prepare data for MySQL...")
        inputDB = self.data_preparer.prepare_data()
        logging.info("Done")

        # load data to MySQL
        self.my_sql_connector = MysqlConnector(
            push_to_mysql=self.settings.push_to_mysql,
            inputDB=inputDB,
            host=self.settings.host,
            user=self.settings.user,
            password=self.settings.password,
            database_name=self.settings.database_name,
            dump_file_path=self.settings.dump_file_path,
            include_flex_params=self.settings.include_flex_params
        )
        logging.info(f"Using MySQL database: {self.settings.database_name}")
        logging.info("Push data to database...")
        self.my_sql_connector.push_DB_to_mysql()
        logging.info("Done")
        output["input_data_name"] = self.settings.database_name
        return output

    def __create_excel(
        self,
        excel_file_path: str,
        inputDB: dict[str, pd.DataFrame]
    ):
        logging.debug("Creating excel file...")
        # save the combined data to a new excel file
        with pd.ExcelWriter(excel_file_path) as writer:
            for sheet_name, df in inputDB.items():
                # 'sheet_name' comes from the key of each DataFrame in the dictionary
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        logging.debug(f"Excel file saved to: {excel_file_path}")

class DataLoader:
    def __init__(self, excel_file_path: str):
        self.__excel_file_path = excel_file_path
        self.__header_columns = {
            'Description': ['Sheet', 'Description'],
            'bus': ['node_id'],
            'branch': ['line_id', 'start_year'],
            'gens': ['Gen_ID','start_year'],
            'gens_extra': ['GenNum','Gen_ID'],
            'gentypedata': ['GenType','Technology'],
            'profiles': ['Name','year'],
            'DistProfiles': ['Profile Number','Name (node_id)'],
            'MarketCoupl': ['Name','Year'],
            'SwissAnnualTargets': ['Name','Year'],
            'projections': ['Item','Scenario'],
            'workforce': ['year','Val'],
            'fuelprices': ['Fuel'],
            'fuelprices_idProfiles': ['Fuel'],
            'DistGens': ['GenName','GenType'],
            'DistGenCosts': ['GenName','InvCost_P'],
            'DistABGenCosts': ['GenName','InvCost_P'],
            'DistRegionData': ['RegionID','RegionName'],
            'DistFlexPotential': ['Name','flex_type'],
            'CentFlexPotential': ['Country','Year','flex_type'],
            'SecurityRef': ['DNS','NLF'],
            'VOMcosts': ['Country','UnitType'],
            'Notes': [],
            'Units': [],
            'Removed': []
        }
        self.__main_sheets = {'branch', 'profiles', 'CentFlexPotential', 'gens', 'bus'}

    
    # function to return all tables in the excel file
    def load_data(self) -> dict:
        logging.debug(f"Loading data from {self.__excel_file_path}...")
        # create list of sheets based on the keys of the header_columns dictionary
        list_of_sheets = list(self.__header_columns.keys())

        # read excel file into dictionary, exclude nan rows
        excel_file = pd.read_excel(self.__excel_file_path, sheet_name=list_of_sheets)
        inputDB_excel = {}

        # set correct header for each sheet and store in dictionary
        for sheet, df in excel_file.items():
            inputDB_excel[sheet] = self.__process_and_store(sheet, df)
        
        logging.debug("Data loaded successfully.")
        return inputDB_excel


    def __process_and_store(
        self,
        sheet_name: str,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        # Set the correct header 
        df = self.__set_header(df, self.__header_columns[sheet_name])

        # delete empty rows and columns
        df = df.dropna(axis=0, how='all')

        # clean rows which have no values for key columns
        if sheet_name in self.__main_sheets and sheet_name in self.__header_columns:
            df = df.dropna(subset=self.__header_columns[sheet_name])
        
        # if sheet_name == "gens", rename duplicate column 'UnitType'
        if sheet_name == "gens":
            new_columns = []
            for col in df.columns:
                if col in new_columns:
                    new_columns.append(f"{col}_2")
                    logging.debug(f"Renaming duplicate column: {col} to {col}_2 in {sheet_name}")
                else:
                    new_columns.append(col)

            df.columns = new_columns
        
        if sheet_name == "CentFlexPotential":
            # clean data from table CentFlexPotential
            df = df[['Country', 'Year', 'flex_type', 'PowerShift_Hrly', 'EnergyShift_Daily', 'EnergyShift_Cost']]
            df = df.dropna(how='all')

        return df


    def __set_header(
        self,
        df: pd.DataFrame,
        expected_header: list[str]
    ) -> pd.DataFrame:
        # Convert expected_header to a set for easier comparison, using lower case
        expected_header_set = set(header.strip().lower() for header in expected_header)
        
        # check if the expected elements are already in the initial header
        current_header = set(str(col).strip().lower() for col in df.columns)

        if expected_header_set.issubset(current_header):
            return df
        
        # Set this row as the header
        for i, row in df.iterrows():
            # Create a set of the non-null values in the row, using lower case
            row_set = set(row.dropna().astype(str).str.lower())
            # Check if all expected headers are present in the row
            if expected_header_set.issubset(row_set):
                header_row_index = i
                break
        else:
            raise ValueError(f"Expected header {expected_header} not found in any row. Expected headers: {expected_header_set}, Current headers: {current_header}")

        # Set the header
        df.columns = df.iloc[header_row_index]
        # Drop all rows up to and including the header row
        df = df.drop(df.index[:header_row_index + 1])
        # Reset the index
        df = df.reset_index(drop=True)
        # df.columns = [str(col).replace('.0', '') for col in df.columns]
        
        return df
    
class DataMerger:
    def __init__(
        self,
        inputDB_excel: dict,
        tyndp24_data_path: str,
        tyndp24_policy: str,
        tyndp24_climate_year: int,
        include_branch_sheet: bool,
        preset_line_types: bool,
        new_line_types: str,
        update_only_specific_columns: bool,
        columns_to_update: dict,
        ntc_type: str | NtcType,
        tyndp24_scope: str
    ):
        self.__inputDB = inputDB_excel
        self.__input_path = tyndp24_data_path
        self.__policy = tyndp24_policy
        self.__climate_year = tyndp24_climate_year
        self.__include_branch_sheet = include_branch_sheet
        self.__preset_line_types = preset_line_types
        self.__new_line_types = new_line_types
        self.__update_only_specific_columns = update_only_specific_columns
        self.__columns_to_update = columns_to_update
        if type(ntc_type) is int:
            error_message = (
                "The use of ntc_type config parameter as an int is deprecated. "
                f"Please choose among {[item.value for item in NtcType]} instead. "
            )
            raise TypeError(error_message)
        self.__ntc_type = NtcType[ntc_type] if type(ntc_type) is str else ntc_type
        self.__scope = tyndp24_scope

    def upsert_tyndp24_data(self) -> dict:
        logging.debug("Upserting TYNDP24 data to the existing data")

        # set selected countries
        if self.__scope == 'EU':
            selected_countries = ['AL', 'AT', 'BA', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 
                                'ES', 'FI', 'FR', 'GR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 
                                'LV', 'ME', 'MK', 'MT', 'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 
                                'SE', 'SI', 'SK', 'UK']
        elif self.__scope == 'nbc':
            selected_countries = ['AT','DE','FR','IT']
        else:
            raise ValueError("Scope must be either 'EU' or 'nbc'")

        # define which files correspond to which sheets in the excel file
        mapping_files_to_excel = {
            'TYNDP24_branch_diffTypes.csv': 'branch', # TYNDP24_branch.csv
            'TYNDP24_demand.csv': 'profiles',
            'TYNDP24_flex_potential.csv': 'CentFlexPotential',
            'TYNDP24_gens.csv': 'gens',
            'TYNDP24_hydroinflows.csv': 'profiles',
            'TYNDP24_nodes.csv': 'bus',
            'TYNDP24_resprofiles.csv': 'profiles'
        }

        # define the key columns for each dataset (sheets from the nexuse excelDB), key columns are given without Policy and Climate Year (is added automatically if available)
        key_columns_of_sheets = {
            'branch': ['line_id', 'start_year'],
            'profiles': ['Name','year'],
            'CentFlexPotential': ['Country','Year','flex_type'],
            'gens': ['Gen_ID','start_year'],
            'bus': ['node_id']
        }

        # organize data from TYNDP24 in dictionary
        inputDB_tyndp24 = {}

        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # Load the data from TYNDP24, add NT 2030 values for all scenarios
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        for file, table_name in mapping_files_to_excel.items():
            # check if sheet 'branch' should be included
            if not self.__include_branch_sheet and table_name == 'branch':
                continue
            logging.debug(f"loading {table_name} ")

            # Load the DataFrame 
            df = pd.read_csv(os.path.join(self.__input_path, file))

            # 'branch' table prep
            # select chosen NTC type
            if table_name == 'branch':
                df = df[df['Type'] == self.__ntc_type.value]
                if self.__ntc_type != NtcType.modelResults:
                    # drop columns 'Policy' and 'Climate Year' if NTC type is not 4 (no different values for different policies and climate years)
                    df = df.drop(columns=['Policy', 'Climate Year'])
                if self.__preset_line_types:
                    df['line_type'] = self.__new_line_types
                if self.__update_only_specific_columns and table_name in self.__columns_to_update:
                    # swap values from TYNDP24 'branch' sheet to ensure matching ecxel sheets line_id's
                    # swap line_id, from_node_id & to_node_id columns, Start Country & End Country, From Node Code & To Node Code, From Node Name & To Node Name
                    swapped_df = df.copy()
                    swapped_df['line_id'] = df['line_id'].apply(lambda x: '--'.join(x.split('--')[::-1]))
                    # Swap the columns
                    columns_to_swap = [
                        ('from_node_id', 'to_node_id'),
                        ('Start Country', 'End Country'),
                        ('From Node Code', 'To Node Code'),
                        ('From Node Name', 'To Node Name')
                    ]
                    for col1, col2 in columns_to_swap:
                        swapped_df[col1], swapped_df[col2] = swapped_df[col2], swapped_df[col1]
                    # Append the swapped data to the original data
                    df = pd.concat([df, swapped_df], ignore_index=True)

            # convert houlry_values columns into floats
            original_columns = df.columns
            new_columns = [float(col) if str(col).isdigit() else col for col in original_columns]
            df.columns = new_columns
            df_filtered = df.copy()
            df_NT_2030_CY = None
            year_columns = ['Year', 'year', 'start_year']
            # filter dataframes by policy and climate year, add NT 2030 values to all cases
            if 'Policy' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['Policy'] == self.__policy]
                df_NT = df[df['Policy'] == 'NT']
        
                # define df_NT_2030_CY 
                if not df_NT.empty:
                    for col in year_columns:
                        if col in df_NT.columns:
                            df_NT_2030 = df_NT[df_NT[col] == 2030]
                            break
                    else:
                        logging.debug(f"Warning: {year_columns} columns are not available for the policy 'NT' in the file {file}.")
                        df_NT_2030 = df_NT  
                    if 'Climate Year' in df_NT_2030.columns:
                        df_NT_2030_CY = df_NT_2030[df_NT_2030['Climate Year'] == self.__climate_year]
                        # Fallback mechanism for Climate Year if the selected year is not available
                        if df_NT_2030_CY.empty:
                            fallback_years = [2009, 2008, 1995]
                            for year in fallback_years:
                                df_NT_2030_CY = df_NT_2030[df_NT_2030['Climate Year'] == year]
                                if not df_NT_2030_CY.empty:
                                    break
                    else:
                        logging.debug(f"Warning: 'Climate Year' column is not available for the policy 'NT' in the file {file}.")
                        df_NT_2030_CY = df_NT_2030


            if 'Climate Year' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['Climate Year'] == self.__climate_year]
            
            # Append the filtered dataframes if df_NT_2030_CY exists and is not empty
            if df_NT_2030_CY is not None and not df_NT_2030_CY.empty:
                df = pd.concat([df_filtered, df_NT_2030_CY], ignore_index=True).drop_duplicates().reset_index(drop=True)
            else:
                df = df_filtered

            # add or append dataframe to the dictionary
            if table_name in inputDB_tyndp24:
                inputDB_tyndp24[table_name] = pd.concat([inputDB_tyndp24[table_name], df], ignore_index=True)
            else:
                inputDB_tyndp24[table_name] = df

        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # split inputDB_excel into parts of unchanged and to be modified data
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        inputDB_excel_unchanged = {}
        inputDB_excel_to_modify = {}
        for key, df in self.__inputDB.items():
            if key in inputDB_tyndp24.keys():
                # find columns that exists in the dataframe and in columns_containing_country
                existing_columns_about_countries = [col for col in ['Country', 'Start Country', 'End Country', 'country'] if col in df.columns]
                if existing_columns_about_countries:
                    """
                    # Create a mask for rows where any of the existing country columns contain 'CH'
                    mask_unchanged = df[existing_columns_about_countries].isin(['CH']).any(axis=1)

                    # Add the split data to the corresponding dictionaries
                    inputDB_excel_unchanged[key] = df[mask_unchanged]
                    inputDB_excel_to_modify[key] = df[~mask_unchanged]
                    """
                    # to exlude branches from/to 'CH' 
                    if key == 'branch':
                        # Create a mask for rows where any of the existing country columns contain 'CH'
                        mask_unchanged = df[existing_columns_about_countries].isin(['CH']).any(axis=1)
                        inputDB_excel_unchanged[key] = df[mask_unchanged]
                        inputDB_excel_to_modify[key] = df[~mask_unchanged]
                        
                        if not (self.__update_only_specific_columns and key in self.__columns_to_update):
                            # remove rows where start_year >= 2030 and Start Country != End Country, to avoid having same branches but with different direction (e.g. DE->IT and IT->DE)
                            inputDB_excel_to_modify[key] = inputDB_excel_to_modify[key][
                                ~((inputDB_excel_to_modify[key]['start_year'] >= 2030) & (inputDB_excel_to_modify[key]['Start Country'] != inputDB_excel_to_modify[key]['End Country']))]
                    else:
                        # Create a mask for rows where any of the existing country columns contain 'CH'
                        mask_data_to_change = df[existing_columns_about_countries].isin(selected_countries).any(axis=1)
                        inputDB_excel_unchanged[key] = df[~mask_data_to_change]
                        inputDB_excel_to_modify[key] = df[mask_data_to_change]
                    
                else:
                    logging.debug(f'Info: No columns containing infos about country found in the file {file}')
                    logging.debug('Trying to find selected countries in column: Name')
                    # Generate the prefixes and create the mask
                    prefixes = tuple(country + "_" for country in selected_countries)
                    mask_data_to_change = df['Name'].str.startswith(prefixes, na=False)
                    # add column 'Country' to the dataframe
                    df.loc[mask_data_to_change, 'Country'] = df.loc[mask_data_to_change, 'Name'].str.split('_').str[0]
                    df.loc[~mask_data_to_change, 'Country'] = 'CH'
                    # write the data to the corresponding dictionaries
                    inputDB_excel_unchanged[key] = df[~mask_data_to_change]
                    inputDB_excel_to_modify[key] = df[mask_data_to_change]
            else:
                inputDB_excel_unchanged[key] = df

        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # create a copy of excel data to be modified, check for uniqueness and upsert data from TYNDP24
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        inputDB_with_tyndp24 = copy.deepcopy(inputDB_excel_to_modify)
        # upsert data from TYNDP24 to inputDB_excel_to_modify
        for key in inputDB_tyndp24.keys():
            corresponding_excel_table = key
            logging.debug(f"Upserting {key} from TYNDP24 to {corresponding_excel_table} of the existing data")
            
            # Check for uniqueness in the TYNDP24 dataset
            if set(key_columns_of_sheets[key]).issubset(inputDB_tyndp24[key].columns):
                if inputDB_tyndp24[key][key_columns_of_sheets[key]].duplicated().any():
                    logging.debug(f'Key columns {key_columns_of_sheets[key]} are not unique in the file {key} of TYNDP24 data') 
                    logging.debug(f"duplicate rows:\n {inputDB_tyndp24[key][inputDB_tyndp24[key][key_columns_of_sheets[key]].duplicated()]}")
            else:
                logging.debug(f'Key columns {key_columns_of_sheets[key]} do not exist in the file {key}')
                logging.debug(f'Columns in the file {key}: {inputDB_tyndp24[key].columns}')

            # Check for uniqueness in the corresponding existing table
            if set(key_columns_of_sheets[key]).issubset(inputDB_with_tyndp24[corresponding_excel_table].columns):
                if inputDB_with_tyndp24[corresponding_excel_table][key_columns_of_sheets[key]].duplicated().any():
                    logging.debug(f'Key columns {key_columns_of_sheets[key]} are not unique in the existing table {corresponding_excel_table}')
                    logging.debug(f"duplicate rows:\n {inputDB_with_tyndp24[corresponding_excel_table][inputDB_with_tyndp24[corresponding_excel_table][key_columns_of_sheets[key]].duplicated()]}")
            else:
                logging.debug(f'Key columns {key_columns_of_sheets[key]} do not exist in the existing table {corresponding_excel_table}')
                logging.debug(f"columns in the existing table {corresponding_excel_table}: {inputDB_with_tyndp24[corresponding_excel_table].columns}")
            
            # ---------------------------------------------------------
            # Load the data from TYNDP24 to the existing data in the excel file
            # ---------------------------------------------------------
            # define special case for key == 'gens' where all values need to be replaced
            if key == 'gens':
                # add new gens for 2030 upwards while keeping older gens
                gens_to_keep = inputDB_with_tyndp24['gens'][inputDB_with_tyndp24['gens']['start_year'] < 2030]
                inputDB_with_tyndp24['gens'] = pd.concat([gens_to_keep, inputDB_tyndp24['gens']], ignore_index=True)

            # upsert new data from TYNDP24 to the existing data
            else:
                # get index columns
                index_columns = key_columns_of_sheets[key]
                
                # get tables and set index without dropping columns
                df_newData = inputDB_tyndp24[key].set_index(index_columns, drop=False).sort_index()
                df_existingData = inputDB_with_tyndp24[key].set_index(index_columns, drop=False).sort_index()

                if self.__update_only_specific_columns and key in self.__columns_to_update:
                    specified_columns = self.__columns_to_update[key]
                    df_combined = df_existingData
                else:
                    # get matching columns
                    matching_columns = (df_newData.columns).intersection(df_existingData.columns)

                    # Concenating the tables, avoiding duplicate indexes
                    df_combined = pd.concat([df_existingData, 
                                            df_newData[matching_columns][~df_newData.index.isin(df_existingData.index)]])

                    specified_columns = matching_columns

                # update the combined dataframe with the TYNDP24 data
                df_combined.update(df_newData[specified_columns])
                
                # reset index without adding the old index as a column (drop=True)
                df_combined.reset_index(drop=True, inplace=True)
                
                # write df_combined to inputDB_with_tyndp24
                inputDB_with_tyndp24[key] = df_combined
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        #  add missing demand profiles if needed
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # defined required scenarios for profiles
        required_years = [2018, 2020, 2030, 2040, 2050]
        required_types = ['load', 'load_emobility', 'load_H2', 'load_heatpump']
        required_countries = selected_countries
        # Generate all combinations with countries and types
        required_names = [f"{country}_{load_type}" for country, load_type in itertools.product(required_countries, required_types)]

        # generate all possible scenarios from required_lists and selected_countries
        required_scenarios = set(itertools.product(required_names, required_years))

        # get all scenarios from inputDB_with_tyndp24['profiles'] where 'type' == 'Load' and 'Country' is in selected_countries
        df_demand_profiles = inputDB_with_tyndp24['profiles'][
            (inputDB_with_tyndp24['profiles']['type'] == 'Load') & 
            (inputDB_with_tyndp24['profiles']['Country'].isin(selected_countries))
        ] 
        # get all scenarios from df
        existing_scenarios = set(df_demand_profiles[['Name', 'year']].itertuples(index=False, name=None))
        # missing scenarios
        missing_scenarios = required_scenarios - existing_scenarios
        logging.debug(f"Missing scenarios: {missing_scenarios}")
        # add missing profiles
        missing_profiles = []
        hourly_values_demand = [col for col in df_demand_profiles.columns if isinstance(col, (int, float))]

        for scenario in missing_scenarios:
            new_row = {
                'Name': scenario[0],
                'year': scenario[1],
                'Country': scenario[0].split('_')[0],
                'type': 'Load',
                'resolution': 1,
                'unit': 'MW'
            }
            # add zeros for all hourly values
            for col in hourly_values_demand:
                new_row[col] = 0

            missing_profiles.append(new_row)

        # create df out of missing profiles
        missing_profiles = pd.DataFrame(missing_profiles)
        # logging.debug(f"Missing profiles: {missing_profiles}")

        # add to inputDB_with_tyndp24['profiles']
        inputDB_with_tyndp24['profiles'] = pd.concat([inputDB_with_tyndp24['profiles'], missing_profiles], ignore_index=True)
        # check for duplicates
        if inputDB_with_tyndp24['profiles'][['Name', 'year']].duplicated().any():
            logging.debug("Duplicate profiles in 'profiles' table")
            logging.debug(f"Duplicates:\n {inputDB_with_tyndp24['profiles'][inputDB_with_tyndp24['profiles'][['Name', 'year']].duplicated()]}")
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------    
        # linking profiles to bus for modified data
        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
        logging.debug("Linking new profiles to buses...")

        # add profile numbers to new profiles
        inputDB_with_tyndp24['profiles'] = self.__set_ids(inputDB_excel_unchanged['profiles'], inputDB_with_tyndp24['profiles'], 'Profile Number')
    
        # add node_numbers to new nodes
        inputDB_with_tyndp24['bus'] = self.__set_ids(inputDB_excel_unchanged['bus'], inputDB_with_tyndp24['bus'], 'node_number')

        # add 'zone' number to new nodes
        inputDB_with_tyndp24['bus'] = self.__set_ids(inputDB_excel_unchanged['bus'], inputDB_with_tyndp24['bus'], 'Zone')

        # add 'DistIvProfile' to new nodes
        # inputDB_with_tyndp24['bus'] = set_ids(inputDB_excel_unchanged['bus'], inputDB_with_tyndp24['bus'], 'DistIvProfile')
        max_idDistProfile_new = inputDB_with_tyndp24['bus']['DistIvProfile'].max()
        max_idDistProfile_existing = inputDB_excel_unchanged['bus']['DistIvProfile'].max()
        max_idDistProfile = max(max_idDistProfile_new, max_idDistProfile_existing)
        missing_idDistProfile_mask = inputDB_with_tyndp24['bus']['DistIvProfile'].isna()
        inputDB_with_tyndp24['bus'].loc[missing_idDistProfile_mask, 'DistIvProfile'] = range(int(max_idDistProfile) + 1, int(max_idDistProfile) + 1 + missing_idDistProfile_mask.sum())

        # columns with loadprofiles in 'bus' 
        loadprofile_columns_bus = ['2018 LoadProfile', '2020 LoadProfile', '2030 LoadProfile', '2040 LoadProfile', '2050 LoadProfile', 
                                    '2018 eMobility LoadProfile', '2020 eMobility LoadProfile', '2030 eMobility LoadProfile', '2040 eMobility LoadProfile', '2050 eMobility LoadProfile',
                                    '2018 eHeatPump LoadProfile', '2020 eHeatPump LoadProfile', '2030 eHeatPump LoadProfile', '2040 eHeatPump LoadProfile', '2050 eHeatPump LoadProfile',
                                    '2018 eHydrogen LoadProfile', '2020 eHydrogen LoadProfile', '2030 eHydrogen LoadProfile', '2040 eHydrogen LoadProfile', '2050 eHydrogen LoadProfile']
                                    # 'DistIvProfile',

        # create dict to map loadprofile type to 'name' of 'profiles'
        loadprofile_dict = {'LoadProfile':'load', 
                            'eMobility LoadProfile':'load_emobility', 
                            'eHeatPump LoadProfile':'load_heatpump', 
                            'eHydrogen LoadProfile':'load_H2'}
        
        # melt 'bus' table to have each loadprofile in a separate row
        df_nodes_melt = inputDB_with_tyndp24['bus'].melt(id_vars=['node_id','country'], value_vars=loadprofile_columns_bus, var_name='Profiles', value_name='Profile Number')
        # Split by space and expand into two new columns
        df_nodes_melt[['Year', 'ProfileType']] = df_nodes_melt['Profiles'].str.split(' ', n=1, expand=True)
        # fix type of 'Year' column
        df_nodes_melt.loc[:,'Year'] = df_nodes_melt['Year'].astype(int)
        # map ProfileType to 'name' of 'profiles'
        df_nodes_melt['ProfileType'] = df_nodes_melt['ProfileType'].map(loadprofile_dict)
        # add country to 'ProfileType' to create column called profile_name
        df_nodes_melt['profile_name'] = df_nodes_melt['country'] + '_' + df_nodes_melt['ProfileType']

        # merge profiles and bus table based on 'profile_name'/'Name'  and year
        df_nodes_merged = pd.merge(df_nodes_melt[['node_id','country','Profiles','profile_name','Year']], inputDB_with_tyndp24['profiles'][['Profile Number','Name','year']], left_on=['profile_name','Year'], right_on=['Name','year'], how='left')

        # function to fill missing profile numbers
        def fill_closest_year(group):
            # convert Profile Number to integer
            group['Profile Number'] = group['Profile Number'].astype(int)
            # Forward-fill and backward-fill within the group to get the closest available Profile Number
            group['Profile Number'] = group['Profile Number'].ffill().bfill()
            return group
        # sort by profile_name and year
        df_nodes_merged = df_nodes_merged.sort_values(by=['profile_name','Year'])
        # fill missing profile numbers
        df_nodes_merged = df_nodes_merged.groupby(
            ['profile_name'],
            group_keys=False
        ).apply(
            fill_closest_year
        )
        # reset index
        df_nodes_merged = df_nodes_merged.reset_index(drop=True)
        # select only relevant columns
        df_nodes_final = df_nodes_merged[['node_id','Profiles','Profile Number']].copy()
        # set type of 'Profile Number' to integer
        df_nodes_final['Profile Number'] = df_nodes_final['Profile Number'].astype(pd.Int64Dtype())
        # pivot back to original format
        df_nodes_final_pivot = df_nodes_final.pivot(index='node_id', columns='Profiles', values='Profile Number').reset_index()

        # update 'bus' table with new loadprofiles
        # set index to 'node_id' in both tables
        df_nodes_newData = df_nodes_final_pivot.set_index('node_id', drop=False).sort_index()
        df_nodes_existingData = inputDB_with_tyndp24['bus'].set_index('node_id', drop=False).sort_index()

        # upsert 'bus' table with new loadprofiles
        common_columns_nodes = df_nodes_newData.columns.intersection(df_nodes_existingData.columns)
        combined_nodes = pd.concat([df_nodes_existingData, 
                                    df_nodes_newData[common_columns_nodes][~df_nodes_newData.index.isin(df_nodes_existingData.index)]])
                                
        combined_nodes.update(df_nodes_newData[common_columns_nodes])
        combined_nodes.reset_index(drop=True, inplace=True)
        combined_nodes['Load Name'] = combined_nodes['country'] + '_load'

        # when other countries are added...
        """
        # find max idDistProfile 
        max_idDistProfile = df_nodes_existingData['DistIvProfile'].max()
        # get rows where idDistProfile is NaN
        mask_idDistProfile = df_nodes_existingData['DistIvProfile'].isna()
        # fill NaN values with new idDistProfile
        df_nodes_existingData.loc[mask_idDistProfile, 'DistIvProfile'] = range(max_idDistProfile + 1, max_idDistProfile + 1 + mask_idDistProfile.sum())
        """

        # write df_nodes_existingData to inputDB_with_tyndp24
        inputDB_with_tyndp24['bus'] = combined_nodes

        logging.debug("Linking new profiles to buses is done")
        # ---------------------------------------------------------
        # linking profiles to gens for modified data
        # ---------------------------------------------------------
        logging.debug("Linking new profiles to gens...")
        df_gens = copy.deepcopy(inputDB_with_tyndp24['gens'])
        df_profiles = copy.deepcopy(inputDB_with_tyndp24['profiles'])

        # add column to 'profiles' table to link to 'gens' table
        # remove 'country' from 'Name'
        df_profiles['profile_type'] = df_profiles['Name'].str.replace(r'^[A-Z]{2}_', '', regex=True)
        df_profiles_filtered = df_profiles[['Profile Number','profile_type','Name','Country','year']]

        # create dict to map SubType of 'gens' to 'Name' of 'profiles'
        gens_dict = {
            'RoR':'inflows_run_of_river',
            'Pump-Open':'inflows_pumped_hydro',
            'Dam':'inflows_hydro_dam',
            'WindOn':'wind',
            'WindOff':'windOff',
            'PV-roof':'solar'
        }
        # add column to 'gens' table to link to 'profiles' table based on dict
        df_gens['profile_type'] = df_gens['SubType'].map(gens_dict)
        # filter out rows in 'gens' with SubType within dict
        df_gens = df_gens[df_gens['SubType'].isin(gens_dict.keys())]
        # filter gens table to only relevant columns and remove gens where 'P_gen_max in 2015 (MW)' == 0
        df_gens = df_gens[df_gens['P_gen_max in 2015 (MW)'] != 0]
        df_gens_filtered = df_gens[['Gen_ID','start_year','Country','profile_type']]

        # cross join tables 'gens' and 'profiles' to get all possible combinations
        df_cross = pd.merge(df_gens_filtered, df_profiles_filtered, left_on=['profile_type','Country'], right_on=['profile_type','Country'], how='left')
        # save df_cross to csv
        df_cross = df_cross.sort_values(by=['profile_type','Country'])
        df_cross['year_difference'] = (df_cross['year'] - df_cross['start_year']).abs()
        df_closest_match = df_cross.loc[df_cross.groupby(['Gen_ID','start_year','Country'])['year_difference'].idxmin()]
        df_gens_final = df_closest_match[['Gen_ID','Country','start_year','Profile Number']]
        df_gens_final = df_gens_final.rename(columns={'Profile Number':'idProfile'})

        # give every gen a unique 'GenNum'
        # get max 'GenNum' from unchanged data
        max_GenId = inputDB_excel_unchanged['gens']['GenNum'].max()
        number_of_rows_gens = len(inputDB_with_tyndp24['gens'])
        inputDB_with_tyndp24['gens']['GenNum'] = range(int(max_GenId) + 1, int(max_GenId) + 1 + number_of_rows_gens)

        # get original gens table
        df_gens_existingData = copy.deepcopy(inputDB_with_tyndp24['gens'])
        # set index to 'Gen_ID' and 'start_year' in both tables
        df_gens_final = df_gens_final.set_index(['Gen_ID','start_year'], drop=False).sort_index()
        df_gens_existingData = df_gens_existingData.set_index(['Gen_ID','start_year'], drop=False).sort_index()
        matching_columns_gens = (df_gens_final.columns).intersection(df_gens_existingData.columns)
        df_gens_existingData.update(df_gens_final[matching_columns_gens])
        df_gens_existingData.reset_index(drop=True, inplace=True)
        # save updated gens table
        inputDB_with_tyndp24['gens'] = df_gens_existingData

        logging.debug("Linking new profiles to gens is done")
        # ---------------------------------------------------------
        # set idFromBus and idToBus in branch table
        # ---------------------------------------------------------
        if self.__include_branch_sheet:
            # add 'From Node Number' and 'To Node Number' to 'branch' table
            df_branch = copy.deepcopy(inputDB_with_tyndp24['branch'])
            df_bus = copy.deepcopy(inputDB_with_tyndp24['bus'])
            merge_from = df_branch.merge(df_bus[['node_id','node_number']], left_on='from_node_id', right_on='node_id', how='left')
            merge_to = df_branch.merge(df_bus[['node_id','node_number']], left_on='to_node_id', right_on='node_id', how='left')
            df_branch['From Node Number'] = merge_from['node_number']
            df_branch['To Node Number'] = merge_to['node_number']
            inputDB_with_tyndp24['branch'] = df_branch
        else:
            logging.debug("Line type is not included in the data")
        # ---------------------------------------------------------
        #  set 'AfemNodeNum' in 'gens' table
        # ---------------------------------------------------------
        df_gens = copy.deepcopy(inputDB_with_tyndp24['gens'])
        merge_afem = df_gens.merge(inputDB_with_tyndp24['bus'][['node_id','node_number']], left_on='NodeId', right_on='node_id', how='left')
        df_gens['AfemNodeNum'] = merge_afem['node_number']
        inputDB_with_tyndp24['gens'] = df_gens
        # ---------------------------------------------------------
        # set DE_380 as slack node -> 'Type' = 3
        # ---------------------------------------------------------
        inputDB_with_tyndp24['bus'].loc[inputDB_with_tyndp24['bus']['node_id'] == 'DE_380', 'Type'] = 3
        # ---------------------------------------------------------
        # combine modified data with unchanged data
        # ---------------------------------------------------------
        logging.debug("Combine new data with existing...")
        inputDB_combined = inputDB_excel_unchanged.copy()

        # Merge modified data back into the unchanged parts
        for key, modified_df in inputDB_with_tyndp24.items():
            logging.debug(f"Appending {key} to unchanged excel")
            # logging.debug(f"Modified df: {modified_df}")
            # get matching columns
            matching_columns = (inputDB_excel_unchanged[key].columns).intersection(modified_df.columns)
            # Append the modified DataFrame to the unchanged DataFrame
            inputDB_combined[key] = pd.concat([inputDB_excel_unchanged[key], modified_df[matching_columns]], ignore_index=True)
        # ---------------------------------------------------------
        # sort 'profiles' for 'Profile Number'
        # ---------------------------------------------------------
        inputDB_combined['profiles'] = inputDB_combined['profiles'].sort_values(by='Profile Number')
        # ---------------------------------------------------------
        # adjust table 'description' for number of items/rows
        # ---------------------------------------------------------
        # adjust table 'Description'
        df_description = inputDB_combined['Description']  
        # adjust column 'NumberOfItems' to the number of rows in the each sheet
        for sheet, df in inputDB_combined.items():
            # Remove all rows where all values are NaN
            df.dropna(how='all', inplace=True)
            
            # Update the df_description DataFrame with the number of non-NaN items for each sheet
            df_description.loc[df_description['Sheet'] == sheet, 'NumberOfItems'] = len(df)

        # save the adjusted table back to the dictionary
        inputDB_combined['Description'] = df_description
        # ---------------------------------------------------------
        logging.debug("Upserting tyndp24 data is done")
        # return updated inputDB
        return inputDB_combined

    def __set_ids(self,existingdata, newdata, id_column):
        # Ensure existingdata is not empty
        if existingdata.empty:
            existing_ids = set()
            max_existing_ids = 0
        else:
            existing_ids = set(existingdata[id_column].dropna())
            max_existing_ids = int(max(existing_ids)) if existing_ids else 0
        # Ensure newdata is not empty
        if newdata.empty:
            raise ValueError("newdata cannot be empty.")
        max_new_id = int(newdata[id_column].max(skipna=True))
        max_id = max(max_existing_ids, max_new_id)
        id_used_in_newdata = set(newdata[id_column].dropna())
        all_existing_ids = existing_ids.union(id_used_in_newdata)

        # mask for rows with missing values in newdata
        missing_mask = newdata[id_column].isna()
        # assign new ids to newdata    
        all_possible_ids = set(range(1, max_id + 1))
        available_ids = sorted(all_possible_ids - all_existing_ids)
        new_ids = available_ids + list(range(max_id + 1, max_id + 1 + missing_mask.sum()))
        newdata.loc[missing_mask, id_column] = new_ids[:missing_mask.sum()] 

        return newdata

class DataPreparer:
    def __init__(
        self,
        inputDB: dict,
        excel_file_path: str,
        database_author: str,
        use_new_line_types: bool,
        demand_profiles_path: str,
        include_flex_params: bool,
        schema_version: int,
        config_years_int: list,
        config_years: list
    ):
        self.__inputDB = inputDB
        self.__filename = os.path.basename(excel_file_path) 
        self.__inputDB_modified = {}
        self.__database_author = database_author
        self.__use_new_line_types = use_new_line_types
        self.__demand_profiles_path = demand_profiles_path
        self.__include_flex_params = include_flex_params
        self.__schema_version = schema_version
        self.__config_years_int = config_years_int
        self.__config_years = config_years

    # convert timeseries of data to string
    def __convert_timeseries_to_string(self, df, number_of_digits):
        
        hourly_values = [col for col in df.columns if isinstance(col, (int, float, np.number))and pd.notna(col)]
        profiles_text = []  # List to store the processed text for each profile

        for index, row in df.iterrows():
            # Extract the hourly values 
            profile_num = row[hourly_values].values
            
            # Remove any NaN values from profile_num
            profile_num = profile_num[~pd.isna(profile_num)]
            
            # Convert the profile to a string with up to 16 digits after the decimal
            profile_text = ','.join([f'{x:.{number_of_digits}f}' for x in profile_num])
            
            # Add square brackets and store in the profiles_text list
            profiles_text.append(f'[{profile_text}]')
        
        return profiles_text

    def __prepareData_branch(self, df, use_new_line_types):
        # create df_branch
        df_branch = pd.DataFrame({
            'fbus': df['From Node Number'],
            'tbus': df['To Node Number'],
            'line_type': df['line_type'],
            'loss_factor': df['DC line loss assumption (% of Power injected that is lost)'], 
            'r': df['Resistance (p.u.)'],
            'x': df['Reactance (p.u.)'],
            'b': df['Susceptance (p.u.)'],
            'rateA': df['S_max_win (MVA)'],
            'rateA2': df['S_max_win_d2 (MVA)'],
            'rateB': 0,
            'rateC': 0,
            'ratio': df['tap_mag (p.u.)'], # = TAP in idx_branch
            'angle': df['tap_ang (Radian)'],
            'status': df['Status'],
            'angmin': 0,
            'angmax': 0,
            'Pf': 0,
            'Qf': 0,
            'Pt': 0,
            'Qt': 0,
            'mu_Sf': 0,
            'mu_St': 0,
            'mu_angmin': 0,
            'mu_angmax': 0
        })
        # replace NaNs with ZEROS for: R,X,B, and shift angle
        df_branch[['r', 'x', 'b', 'angle']] = df_branch[['r', 'x', 'b', 'angle']].replace(np.nan, 0)

        # replace NaN with 1 for 'ratio'
        df_branch['ratio'] = df_branch['ratio'].replace(np.nan, 1 , regex=True)

        # test for NaNs in line capacity
        NaNs = df_branch['rateA'].isna().sum()
        if NaNs != 0:
            logging.debug(f'Error: {NaNs} NaNs detected in df_branch')
            raise ValueError('Error. NaNs in df_branch')
        
        # Create a mask for rows where TAP is not equal to 0 (transformers)
        transformer_mask = df_branch['ratio'] != 0
        line_mask = ~transformer_mask
        
        # create df_branchInfoTable
        df_branchInfoTable = pd.DataFrame({
            'LineNum':  range(1, len(df) + 1),
            'LineID': df['line_id'],
            'FromNodeNum': df['From Node Number'],
            'ToNodeNum': df['To Node Number'],
            'FromNodeID': df['from_node_id'],
            'ToNodeID': df['to_node_id'],
            'FromNodeCode': df['From Node Code'],
            'ToNodeCode': df['To Node Code'],
            'FromNodeCountry': df['Start Country'],
            'ToNodeCountry': df['End Country'],
            'StartYr': df['start_year'],
            'EndYr': df['end_year'],
            'Voltage': df['Line Voltage (kV)'],
            'MVA_Limit_Winter': df['S_max_win (MVA)'],
            'MVA_Limit_Summer': df['S_max_sum (MVA)'],
            'MVA_Limit_SpringFall': df['S_max_spr (MVA)'],
            'Resistance(p.u.)': df['Resistance (p.u.)'],
            'Reactance(p.u.)': df['Reactance (p.u.)'],
            'Susceptance(p.u.)': df['Susceptance (micro Siemens)'],
            'TapMag': df['tap_mag (p.u.)'],
            'TapAngle(deg)': df['tap_ang (Radian)'],
            'Status': df['Status'],
            'Ind_CrossBorder': df['Indicator Cross Border line'],
            'Ind_Trafo': df['Indicator Transformer'],
            'Ind_Aggreg': df['Indicator Aggreg Line'],
            'Ind_HVDC': df['Indicator HVDC'],
            'CH_XB_Name': df['CH XB: X name'],
            'CH_XB_Code': df['CH XB: X code'],
            'Candidate': df['Candidate Line'],
            'CandCost': df['Cand Cost (EUR/km/yr)'],
            'Length': df['length']
        })

        # create transfomer and line table to be pushed to the sql database
        # line data
        idLine_sequence = range(1, len(df_branch[line_mask]) + 1)

        df_linedata = pd.DataFrame({
            'idLine': idLine_sequence,
            'LineName': df_branchInfoTable['LineID'][line_mask],
            'line_type': df_branch['line_type'][line_mask],
            'loss_factor': df_branch['loss_factor'][line_mask],
            'r': df_branch['r'][line_mask],
            'x': df_branch['x'][line_mask],
            'b': df_branch['b'][line_mask],
            'rateA': df_branch['rateA'][line_mask],
            'rateA2': df_branch['rateA2'][line_mask],
            'rateB': df_branch['rateB'][line_mask],
            'rateC': df_branch['rateC'][line_mask],
            'StartYr': df_branchInfoTable['StartYr'][line_mask],
            'EndYr': df_branchInfoTable['EndYr'][line_mask],
            'kV': df_branchInfoTable['Voltage'][line_mask],
            'MVA_Winter': df_branchInfoTable['MVA_Limit_Winter'][line_mask],
            'MVA_Summer': df_branchInfoTable['MVA_Limit_Summer'][line_mask],
            'MVA_SprFall': df_branchInfoTable['MVA_Limit_SpringFall'][line_mask],
            'length': df_branchInfoTable['Length'][line_mask]
        })

        # create transformer data df
        # idTransformer goes from 1 to the total number of transformers for the transformer mask
        idTransformer_sequence = range(1, len(df_branch[transformer_mask]) + 1)
        df_transfomerdata = pd.DataFrame({
            'idTransformer': idTransformer_sequence,
            'TrafoName': df_branchInfoTable['LineID'][transformer_mask],
            'line_type': df_branch['line_type'][transformer_mask],
            'loss_factor': df_branch['loss_factor'][transformer_mask],
            'r': df_branch['r'][transformer_mask],
            'x': df_branch['x'][transformer_mask],
            'b': df_branch['b'][transformer_mask],
            'rateA': df_branch['rateA'][transformer_mask],
            'rateA2': df_branch['rateA2'][transformer_mask],
            'rateB': df_branch['rateB'][transformer_mask],
            'rateC': df_branch['rateC'][transformer_mask],
            'tapRatio': df_branch['ratio'][transformer_mask],
            'angle': df_branch['angle'][transformer_mask],
            'StartYr': df_branchInfoTable['StartYr'][transformer_mask],
            'EndYr': df_branchInfoTable['EndYr'][transformer_mask],
            'MVA_Winter': df_branchInfoTable['MVA_Limit_Winter'][transformer_mask],
            'MVA_Summer': df_branchInfoTable['MVA_Limit_Summer'][transformer_mask],
            'MVA_SprFall': df_branchInfoTable['MVA_Limit_SpringFall'][transformer_mask],
            'length': df_branchInfoTable['Length'][transformer_mask]
        })

        # depending on the use_new_line_types, remove columns
        if not use_new_line_types:
            df_branch.drop(columns=['line_type', 'loss_factor','rateA2'], inplace=True)
            df_linedata.drop(columns=['line_type', 'loss_factor', 'rateA2'], inplace=True)
            df_transfomerdata.drop(columns=['line_type', 'loss_factor', 'rateA2'], inplace=True)

        # save new df_branch and df_branchInfoTable to inputDB
        return {
            'branch': df_branch,
            'branchInfoTable': df_branchInfoTable,
            'linedata': df_linedata,
            'transformerdata': df_transfomerdata
        }

    def __prepareData_nodes(self, df):
        # check for NaNs in the data
        nan_rows = df[df['node_number'].isna()]
        if not nan_rows.empty:
            logging.debug('Warning: NaNs detected in bus data:')
            logging.debug(f"Nan rows:\n{nan_rows}")
            logging.debug(df[df['node_number'].isna()][['node_id', 'node_number','country','start_year']])
            # remove rows with NaNs
            df = df.dropna(subset=['node_number'])
            logging.debug('Warning: NaNs removed from bus data')

        # Creating a new DataFrame
        df_bus = pd.DataFrame({
            'bus_i': df['node_number'],
            'type': df['Type'],
            'Pd': df['Pd'],
            'Qd': df['Qd'],
            'Gs': 0,
            'Bs': 0,
            'area': df['Zone'],
            'Vm': 1,
            'Va': 0,
            'baseKV': df['base_voltage'],
            'zone': df['Zone'],
            'Vmax': df['Vmax'],
            'Vmin': df['Vmin'],
            'lam_P': 0,
            'lam_Q': 0,
            'mu_Vmax': 0,
            'mu_Vmin': 0
        })
        
        # Count total NaNs
        NaNs = df_bus.isna().sum().sum()

        # Check for any NaNs
        if NaNs != 0:
            # Display error message
            logging.debug(f'Error: {NaNs} NaNs detected in df_bus:')
            logging.debug(f'{df_bus[df_bus.isna().any(axis=1)]}')

            # Stop execution
            raise ValueError('Error. NaNs in df_bus')

        
        # set slack node Vmax and Vmin to 1.0 
        idx_slack = df_bus[df_bus['type'] == 3].index
        df_bus.loc[idx_slack, 'Vmax'] = 1.0
        df_bus.loc[idx_slack, 'Vmin'] = 1.0

        # redefine bus types, type = 1 'PV', type = 2 'PQ', type = 3 'Slack'
        df_bus['type'] = df_bus['type'].replace({1: 'PV', 2: 'PQ', 3: 'SL'})

        # create df_busInfoTable
        df_busInfoTable = pd.DataFrame({
            'NodeNum': df['node_number'],
            'NodeID': df['node_id'],
            'NodeCode': df['TO USE Node Codes'],
            'Country': df['country'],
            'SubRegion': df['canton'],
            'MarketZone': df['Zone'],
            'LoadRegion': df['load_canton'],
            'X_coord': df['coord_x'],
            'Y_coord': df['coord_y'],
            'StartYr': df['start_year'],
            'EndYr': df['end_year'],
            'Voltage': df['base_voltage'],
            'BusType': df['Type'],
            'Vmax': df['Vmax'],
            'Vmin': df['Vmin'],
            'LoadName': df['Load Name'],
            'Pd': df['Pd'],
            'Qd': df['Qd'],
            'LoadHedgeR': df['Load HedgeRatio (fraction)'],
            'LoadMeanErr': df['Load Mean Forecast Error ()'],
            'LoadSigmaErr': df['Load Sigma Forecast Error ()'],
            'DemandShare_2018': df['2015 Load Share (% by Country)'],
            'DemandShare_2020': df['2020 Load Share (% by Country)'],  
            'DemandShare_2025': df['2025 Load Share (% by Country)'],
            'WindShare_2018': df['2015 Wind Gen Share (% by Country)'],
            'WindShare_2020': df['2020 Wind Gen Share (% by Country)'],
            'WindShare_2025': df['2025 Wind Gen Share (% by Country)'],
            'SolarShare_2018': df['2015 PV Gen Share (% by Country)'],
            'SolarShare_2020': df['2020 PV Gen Share (% by Country)'],
            'SolarShare_2025': df['2025 PV Gen Share (% by Country)'],
            'LoadProfile_2018': df['2018 LoadProfile'],
            'LoadProfile_2020': df['2020 LoadProfile'],
            'LoadProfile_2030': df['2030 LoadProfile'],
            'LoadProfile_2040': df['2040 LoadProfile'],
            'LoadProfile_2050': df['2050 LoadProfile'],
            'DistIvProfile': df['DistIvProfile'],
            'LoadProfile_eMobility_2018': df['2018 eMobility LoadProfile'],
            'LoadProfile_eMobility_2020': df['2020 eMobility LoadProfile'],
            'LoadProfile_eMobility_2030': df['2030 eMobility LoadProfile'],
            'LoadProfile_eMobility_2040': df['2040 eMobility LoadProfile'],
            'LoadProfile_eMobility_2050': df['2050 eMobility LoadProfile'],
            'LoadProfile_eHeatPump_2018': df['2018 eHeatPump LoadProfile'],
            'LoadProfile_eHeatPump_2020': df['2020 eHeatPump LoadProfile'],
            'LoadProfile_eHeatPump_2030': df['2030 eHeatPump LoadProfile'],
            'LoadProfile_eHeatPump_2040': df['2040 eHeatPump LoadProfile'],
            'LoadProfile_eHeatPump_2050': df['2050 eHeatPump LoadProfile'],
            'LoadProfile_eHydrogen_2018': df['2018 eHydrogen LoadProfile'],
            'LoadProfile_eHydrogen_2020': df['2020 eHydrogen LoadProfile'],
            'LoadProfile_eHydrogen_2030': df['2030 eHydrogen LoadProfile'],
            'LoadProfile_eHydrogen_2040': df['2040 eHydrogen LoadProfile'],
            'LoadProfile_eHydrogen_2050': df['2050 eHydrogen LoadProfile'],
            'WindShare_2030': df['2030 Wind Gen Share (% by Country)'],
            'WindShare_2040': df['2040 Wind Gen Share (% by Country)'],
            'WindShare_2050': df['2050 Wind Gen Share (% by Country)'],
            'SolarShare_2030': df['2030 PV Gen Share (% by Country)'],
            'SolarShare_2040': df['2040 PV Gen Share (% by Country)'],
            'SolarShare_2050': df['2050 PV Gen Share (% by Country)']
        })

        # make sure that the table variable names match with the DB attributes
        df_busdata = pd.DataFrame({
            'idBus': df_bus['bus_i'],
            'internalBusId': df_bus['bus_i'],
            'BusName': df_busInfoTable['NodeID'],
            'SwissgridNodeCode': df_busInfoTable['NodeCode'],
            'ZoneId': df_bus['zone'],
            'X_Coord': df_busInfoTable['X_coord'],
            'Y_Coord': df_busInfoTable['Y_coord'],
            'BusType': df_bus['type'],
            'Qd': df_bus['Qd'],
            'Pd': df_bus['Pd'],
            'Gs': df_bus['Gs'],
            'Bs': df_bus['Bs'],
            'baseKV': df_bus['baseKV'],
            'Country': df_busInfoTable['Country'],
            'SubRegion': df_busInfoTable['SubRegion'],
            'StartYr': df_busInfoTable['StartYr'],
            'EndYr': df_busInfoTable['EndYr']
        })

        # create 'loaddata table' based on bus and busInfoTable data
        df_loaddata = pd.DataFrame({
            'idLoad': df_bus['bus_i'],
            'LoadType': df_busInfoTable['LoadName'],
            'Pd': df_bus['Pd'],
            'Qd': df_bus['Qd'],
            'hedgeRatio': df_busInfoTable['LoadHedgeR'],
            'meanForecastError24h': df_busInfoTable['LoadMeanErr'],
            'sigmaForecastError24h': df_busInfoTable['LoadSigmaErr']
        })

        return {
            'bus': df_bus,
            'busInfoTable': df_busInfoTable,
            'busdata': df_busdata,
            'loaddata': df_loaddata
        }
    
    def __prepareData_gens(self, df):
        # create df_gen
        df_gen = pd.DataFrame({
            'bus': df['AfemNodeNum'],
            'PG': 0,
            'QG': 0,
            'Qmax': 0,
            'Qmin': 0,
            'Vg': 1,
            'mBase': 0,
            'status': df['Status'],
            'Pmax': df['P_gen_max in 2015 (MW)'],
            'Pmin': df['Pmin (MW)'],
            'Pc1': 0,
            'Pc2': 0,
            'Qc1min': 0,
            'Qc1max': 0,
            'Qc2min': 0,
            'Qc2max': 0,
            'ramp_agc': 0,
            'ramp_10': 0,
            'ramp_30': 0,
            'ramp_q': 0,
            'apf': 0
        })

        # create df_genInfoTable
        df_genInfoTable = pd.DataFrame({
            'GenNum': df['GenNum'],
            'GenID': df['Gen_ID'],
            'GenType': df['UnitType'],
            'GenSubType': df['SubType'],
            'NodeNum': df['AfemNodeNum'],
            'NodeID': df['NodeId'],
            'NodeCode': df['NodeCode'],
            'Country': df['Country'],
            'Canton': df['Canton'],
            'StartYr': df['start_year'],
            'EndYr': df['end_year (50yrNucl)'],
            'Type': df['UnitType_2'], # this column was renamed from 'UnitType' to 'UnitTyp2' since 'UnitType' is already used in df_gen
            'Pgen_max': df['P_gen_max in 2015 (MW)'],
            'Pgen_min': df['Pmin (MW)'],
            'Pmin_db': df['Pmin_database (MW)'],
            'VOM_Cost': df['non Fuel VOM (Euro/MWh)'],
            'Effic': df['Efficiency/HeatRate (MWh elec / MWh heat)'],
            'CO2Rate': df['CO2 Emission Rate (ton CO2 / MWh elec)'],
            'FOM_Cost': df['Fixed O&M Costs (Euro/MW/yr)'],
            'InvCost': df['Investment Cost (Euro/MWel/yr)'],
            'InvCost_E': df['Investment Cost for Energy (Euro/MWh-el/yr)'],
            'StartCost': df['StartUp Cost (Euro/MW/start)'],
            'FuelType': df['Fuel Type'],
            'MinUpTime': df['Min Up Time (hr)'],
            'MinDnTime': df['Min Down Time (hr)'],
            'RR_up': df['Ramp Rate Up (MW/hr)'],
            'RR_dn': df['Ramp Rate Down (MW/hr)'],
            'RR_up_SU': df['Ramp Rate Up StartUp (MW/hr)'],
            'RR_dn_SD': df['Ramp Rate Down ShutDown (MW/hr)'],
            'InitUpTime': df['Initial Up/Down Time at T0 (hr)'],
            'InitPgen': df['Initial Pgen at T0 (MW)'],
            'Status': df['Status'],
            'Ppump_max': df['P_pump_max (MW)'],
            'Emax': df['Emax Jared'],
            'Emin': df['Emin'],
            'Eini': df['E_ini (fraction)'],
            'eta_dis': df['eta_dis (fraction)'],
            'eta_ch': df['eta_ch (fraction)'],
            'idProfile': df['idProfile'],
            'Candidate': df['Candidate Unit'],
            'GenMeanErr': df['Mean Error Forecast 24h'],
            'GenSigmaErr': df['Sigma Error Forecast 24 hr'],
            'Lifetime': df['Lifetime (yr)'],
            'GenHedgeR_2018': df['2018 HedgeRatio'],
            'GenHedgeR_2020': df['2020 HedgeRatio'],
            'GenHedgeR_2030': df['2030 HedgeRatio'],
            'GenHedgeR_2040': df['2040 HedgeRatio'],
            'GenHedgeR_2050': df['2050 HedgeRatio'],
            'InvCost_Charge': df['Investment Cost for Charging (Euro/MWe/yr)'],
            'CO2_PriceInd': df['CO2 Price Indicator']
        })

        # create df_gendata
        df_gendata = pd.DataFrame({
            'idGen': df['GenNum'],
            'GenName': df['Gen_ID'],
            'GenType': df['UnitType'],
            'Technology': df['SubType'],
            'UnitType': df['UnitType_2'],
            'StartYr': df['start_year'],
            'EndYr': df['end_year (50yrNucl)'],
            'GenEffic': df['Efficiency/HeatRate (MWh elec / MWh heat)'],
            'CO2Rate': df['CO2 Emission Rate (ton CO2 / MWh elec)'],
            'eta_dis': df['eta_dis (fraction)'],
            'eta_ch': df['eta_ch (fraction)'],
            'RU': df['Ramp Rate Up (MW/hr)'],
            'RD': df['Ramp Rate Down (MW/hr)'],
            'RU_start': df['Ramp Rate Up StartUp (MW/hr)'],
            'RD_shutd': df['Ramp Rate Down ShutDown (MW/hr)'],
            'UT': df['Min Up Time (hr)'],
            'DT': df['Min Down Time (hr)'],
            'Pini': df['Initial Pgen at T0 (MW)'],
            'Tini': df['Initial Up/Down Time at T0 (hr)'],
            'meanErrorForecast24h': df['Mean Error Forecast 24h'],
            'sigmaErrorForecast24h': df['Sigma Error Forecast 24 hr'],
            'Lifetime': df['Lifetime (yr)']
        })

        return {
            'gen': df_gen,
            'genInfoTable': df_genInfoTable,
            'gendata': df_gendata
        }

    def __prepareData_centIv(self, df):
        df_cent_flex_potential = df.copy()
        # filter out the columns that are not needed
        df_cent_flex_potential = df_cent_flex_potential[['Country', 'Year', 'flex_type', 'PowerShift_Hrly', 'EnergyShift_Daily', 'EnergyShift_Cost']].copy()
        
        # colNames = {'Country','Year','flex_type','PowerShift_Hrly','EnergyShift_Daily','EnergyShift_Cost'};
        df_centFlexPotential = pd.DataFrame({
            'Country': df['Country'],
            'Year': df['Year'],
            'flex_type': df['flex_type'],
            'PowerShift_Hrly': df['PowerShift_Hrly'],
            'EnergyShift_Daily': df['EnergyShift_Daily'],
            'EnergyShift_Cost': df['EnergyShift_Cost']
        })

        # remove rows with missing values
        df_cent_flex_potential = df_cent_flex_potential.dropna(how='all')
        df_centFlexPotential = df_centFlexPotential.dropna(how='all')
        
        # return dfs
        return df_cent_flex_potential, df_centFlexPotential
    
    def __prepareData_description(self, input_filename, database_author, schema_version):
        # create date string
        date = datetime.now()
        date_str = date.strftime('%d-%b-%Y_%H-%M')
        # create df
        # colNames = {'Date','Excel_file_used','Matlab_file_used','created_by_user', 'Schema_version','notes'};
        df_desc = pd.DataFrame({
            'Date': [date_str],
            'Excel_file_used': [input_filename],
            'Matlab_file_used': os.path.basename(__file__),
            'created_by_user': [database_author],
            'Schema_version': [schema_version],
            'notes': ['']
        })

        return df_desc
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------------
    def __prepareData_distAB(self, df):
        # create df
        df_distabgencosts = pd.DataFrame({
            'Year': df['Year'],
            'idDistABGen': df['Index'],
            'GenName': df['GenName'],
            'InvCost_P': df['InvCost_P'],
            'VOM_Cost': df['VOM_Cost'],
            'Subsidy_Base': df['Subsidy_Base'],
            'Subsidy_1_kW': df['Subsidy_1_kW'],
            'Subsidy_2_kW': df['Subsidy_2_kW']
        })

        return df, df_distabgencosts
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------------
    def __prepareData_distIv(self, df_distgen, df_distgencosts, df_distregiondata, df_distflexpotential, config_years):
        results_dict = {}
        # create table based on DistGens
        df_distgendata = pd.DataFrame({
            'idDistGen': df_distgen['Index'],
            'GenName': df_distgen['GenName'],
            'GenType': df_distgen['GenType'],
            'Technology': df_distgen['Technology'],
            'UnitType': df_distgen['UnitType'],
            'Type': df_distgen['Type'],
            'CandidateUnit': df_distgen['CandidateUnit'],
            'InvestmentType': df_distgen['InvestmentType'],
            'min_Size_kW': df_distgen['min_Size_kW'],
            'Pmax_kW': df_distgen['Pmax_kW'],
            'Pmin_kW': df_distgen['Pmin_kW'],
            'Dischrg_max': df_distgen['Dischrg_max'],
            'Chrg_max': df_distgen['Chrg_max'],
            'eta_dis': df_distgen['eta_dis'],
            'eta_ch': df_distgen['eta_ch'],
            'Self_dischrg': df_distgen['Self_dischrg'],
            'Emax': df_distgen['Emax'],
            'Emin': df_distgen['Emin'],
            'E_ini': df_distgen['E_ini'],
            'E_final': df_distgen['E_final'],
            'Pini': df_distgen['Pini'],
            'RU': df_distgen['RU'],
            'RD': df_distgen['RD'],
            'Lifetime': df_distgen['Lifetime'],
            'GenEffic': df_distgen['GenEffic'],
            'ThmlEffic': df_distgen['ThmlEffic'],
            'CapFactor': df_distgen['CapFactor'],
            'CO2Rate': df_distgen['CO2Rate'],
            'Emax_kWh': df_distgen['Emax_kWh'],
            'FuelType': df_distgen['FuelType'],
            'ElecOwnUseFactor': df_distgen['ElecOwnUseFactor']
        })


        # distregiondata contains multiple tables within the sheet
        # clean distregiondata - drop rows with all NaN values
        df_distregiondata = df_distregiondata.dropna(how='all').reset_index(drop=True)
        # Find row indices where 'parameter' appears 
        start_table_indices = df_distregiondata[
            df_distregiondata.apply(lambda row: row.astype(str).str.contains('parameter', case=False, na=False).any(), axis=1)
        ].index.tolist()

        tables = []

        # Loop through the indices to slice the DataFrame
        for idx in range(len(start_table_indices)):
            # Determine start and end rows for each table
            start_idx = start_table_indices[idx] 
        
            # If we're at the last index, slice until the end of the DataFrame
            if idx < len(start_table_indices) - 1:
                end_idx = start_table_indices[idx + 1]
            else:
                end_idx = len(df_distregiondata)
            
            # Extract the table (slice from start_idx to end_idx)
            table = df_distregiondata.iloc[start_idx:end_idx]
            # .dropna(how='all', axis=1).reset_index(drop=True)
            
            # Append the extracted table to the list
            tables.append(table)

        # create list to store dist_region_byPVsize_byIrradLevel tables
        dist_region_byPVsize_byIrradLevel_dfs = []
        # loop trough all tables in tables
        for j in range(len(tables)):
            df = tables[j]
            # remove empty rows
            df = df.dropna(how='all', axis=0)
            # transpose the dataframe
            df = df.T
            # reset index
            df = df.reset_index(drop=True)

            # find row that contains 'Parameter' and set it as header
            header_row_index = 0
            for i, row in df.iterrows():
                if 'parameter' in str(row).lower():
                    header_row_index = i
                    break
            
            # Set the header
            df.columns = df.iloc[header_row_index]
            # Drop all rows up to and including the header row
            df = df.drop(df.index[:header_row_index + 1])
            # Reset the index
            df = df.dropna(how='all', axis=0).reset_index(drop=True)

            # forward fill the columns 'Parameter' and 'PVsize'
            df['Parameter'] = df['Parameter'].ffill()
            df['PVsize'] = df['PVsize'].ffill()
            if j == 0:
                # melt the dataframe
                df = df.melt(id_vars=['Parameter','PVsize'], var_name='idRegion', value_name='value')
                # rename column 'PVsize' to 'GenName'
                df = df.rename(columns={'PVsize':'GenName'})
                # add column with idDistGen (index of the generator in the DistGens table)
                df['idDistGen'] = df['GenName'].map(df_distgen.set_index('GenName')['Index'])
                # reorder columns
                df = df[['Parameter', 'idRegion', 'idDistGen','GenName', 'value']]
                # save the dataframe in the dictionary
                results_dict['dist_region_byPVsize'] = df
                results_dict['distregionbygentypedata'] = df
            else:
                # melt the dataframe
                df = df.melt(id_vars=['Parameter','PVsize','IrradLevel'], var_name='idRegion', value_name='value')
                # rename column 'PVsize' to 'GenName'
                df = df.rename(columns={'PVsize':'GenName'})
                # add column with idDistGen (index of the generator in the DistGens table)
                df['idDistGen'] = df['GenName'].map(df_distgen.set_index('GenName')['Index'])
                # reorder columns
                df = df[['Parameter', 'idRegion', 'idDistGen','GenName', 'IrradLevel', 'value']]
                # append the dataframe to the list
                dist_region_byPVsize_byIrradLevel_dfs.append(df)

        # combine all dfs of dist_region_byPVsize_byIrradLevel
        df_dist_region_byPVsize_byIrradLevel = pd.concat(dist_region_byPVsize_byIrradLevel_dfs)
        
        # separat first table from the rest
        df_distregiondata = df_distregiondata.iloc[:start_table_indices[0]].dropna(how='all', axis=1).reset_index(drop=True)
        # drop rows with all NaN values
        df_distregiondata = df_distregiondata.dropna(how='all', axis=0)

        # prepare tables for sql
        # colNames = {'idRegion','RegionName','idProfile_Irrad','idProfile_PVCapFactor', 'GridTariff','PVInjTariff'};
        # create table based on DistRegionData
        df_distregiondata = pd.DataFrame({
            'idRegion': df_distregiondata['RegionID'],
            'RegionName': df_distregiondata['RegionName'],
            'idProfile_Irrad': df_distregiondata['idProfile_Irrad'],
            'idProfile_PVCapFactor': df_distregiondata['idProfile_PVCapFactor'],
            'GridTariff': df_distregiondata['GridTariff'],
            'PVInjTariff': df_distregiondata['PVInjTariff']
        })
        
        # DistFlexPotential
        df_distflexpotential = df_distflexpotential[['flex_type','Year','PowerShift_Hrly','EnergyShift_Daily']]
        # remove empty rows
        df_distflexpotential = df_distflexpotential.dropna(how='all', axis=0)
        # melt the dataframe
        df_distflexpotential = df_distflexpotential.melt(id_vars=['flex_type','Year'], var_name='Parameter', value_name='value')
        # reorder columns
        df_distflexpotential = df_distflexpotential[['flex_type', 'Parameter', 'Year', 'value']]


        # DistGenCosts        
        # list of unique years from the DistGenCosts table
        years = df_distgencosts['Year'].unique()
        # take only overlapping years between DistGenCosts and config_years
        years = np.intersect1d(years, config_years)
        # create list in range of the number of years
        idDistGenConfig = list(range(1, len(years)+1))
        # create df with idDistGenConfig, Year, Name
        df_distgenconfiginfo = pd.DataFrame({'idDistGenConfig': idDistGenConfig, 'Year': years})
        # add Name column, corresponds to year followed by '_GenCandidates'
        df_distgenconfiginfo['Name'] = df_distgenconfiginfo['Year'].astype(str) + '_GenCandidates'

        #  setup distgenconfiguration table
        # get unique years from DistGenCosts
        years_dist = df_distgencosts['Year'].unique()
        years_dist.sort()
        # map each year to its index in years_dist
        year_to_index = {year: idx + 1 for idx, year in enumerate(years_dist)}
        
        # create table based on DistGenCosts
        df_distgenconfiguration = pd.DataFrame({
            'idDistGenConfig': df_distgencosts['Year'].map(year_to_index),
            'idDistGen': df_distgencosts['Index'],
            'Year': df_distgencosts['Year'],
            'GenName': df_distgencosts['GenName'],
            'InvCost_P': df_distgencosts['InvCost_P'],
            'InvCost_E': df_distgencosts['InvCost_E'],
            'FOM_Cost': df_distgencosts['FOM_Cost'],
            'VOM_Cost': df_distgencosts['VOM_Cost'],
            'Fuel_Cost': df_distgencosts['Fuel_Cost'],
            'Heat_Credit': df_distgencosts['Heat_Credit'],
            'KEV': df_distgencosts['KEV'],
            'WACC': df_distgencosts['WACC'],
            'LCOE': df_distgencosts['LCOE'],
            'Heat_Value': df_distgencosts['Heat_Value']
        })

        # remove empty rows
        df_distgenconfiguration = df_distgenconfiguration.dropna(how='all', axis=0)

        # add tables to results_dict
        results_dict.update({
            'distgendata': df_distgendata,
            'dist_region_data': df_distregiondata,
            'distregiondata': df_distregiondata,
            'dist_region_byPVsize_byIrradLevel': df_dist_region_byPVsize_byIrradLevel,
            'distregionbyirradleveldata': df_dist_region_byPVsize_byIrradLevel,
            'dist_flex_potential': df_distflexpotential,
            'distflexpotential': df_distflexpotential,
            'distgenconfiginfo': df_distgenconfiginfo,
            'distgenconfiguration': df_distgenconfiguration
        })

        return results_dict

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
    def __prepareData_gencost(self, df_fuelprice, df_fuelprice_idProfiles, df_genInfoTable):
        # additional function needed in function prepareData_gencost
        def is_year_column(col_name):
            # Check if column name is a 4-digit number between 1900 and 2100
            return bool(re.match(r'^\d{4}$', str(col_name))) and 1900 <= int(col_name) <= 2200

        # Apply the function to filter columns that are years in both dataframes
        year_columns_fuelprice = [col for col in df_fuelprice.columns if is_year_column(col)]
        year_columns_fuelprice_idProfiles = [col for col in df_fuelprice_idProfiles.columns if is_year_column(col)]

        # convert both dataframes to long format with years in extra column
        df_fuelprice_long = df_fuelprice.melt(id_vars=['Fuel','Units'], value_vars=year_columns_fuelprice, var_name='Year', value_name='Price')
        df_fuelprice_idProfiles_long = df_fuelprice_idProfiles.melt(id_vars='Fuel', value_vars=year_columns_fuelprice_idProfiles, var_name='Year', value_name='idProfiles')

        # merge both dataframes on fuel and year
        # left join to keep all rows from df_fuelprice_long
        df_fuelprice_merged = pd.merge(df_fuelprice_long, df_fuelprice_idProfiles_long, on=['Fuel', 'Year'], how='left')

        # take the columns GenNum, GenID, VOM_Cost from df_genInfoTable
        df_gencostInfoTable = df_genInfoTable[['GenNum', 'GenID', 'VOM_Cost']].copy()

        # get list of unique years from df_fuelprice_merged
        years = df_fuelprice_merged['Year'].unique()

        for year in years:
            # get prices of the current year
            df_fuelprice_year = df_fuelprice_merged[df_fuelprice_merged['Year'] == year]

            # Initialize empty lists for storing fuel and CO2 prices
            gen_FuelPrice = []
            gen_FuelPrice_idProfile = []
            gen_CO2Price = []
            gen_CO2Price_idProfile = []
            
            # iterate trough number of generators in df_genInfoTable
            for i in range(len(df_genInfoTable)):
                # get fuel type, price and idProfile for the current generator
                fuel_type = df_genInfoTable.loc[i, 'FuelType']
                fuel_price = df_fuelprice_year[df_fuelprice_year['Fuel'] == fuel_type]['Price'].values[0]
                fuel_price_idProfile = df_fuelprice_year[df_fuelprice_year['Fuel'] == fuel_type]['idProfiles'].values[0]
                # append values to list
                gen_FuelPrice.append(fuel_price)
                gen_FuelPrice_idProfile.append(fuel_price_idProfile)

                # get CO2 price and idProfile for the current generator
                CO2_type = df_genInfoTable.loc[i, 'CO2_PriceInd']
                CO2_price = df_fuelprice_year[df_fuelprice_year['Fuel'] == CO2_type]['Price'].values[0]
                CO2_price_idProfile = df_fuelprice_year[df_fuelprice_year['Fuel'] == CO2_type]['idProfiles'].values[0]
                # append values to list
                gen_CO2Price.append(CO2_price)
                gen_CO2Price_idProfile.append(CO2_price_idProfile)

            # Calculate the variable fuel cost for each generator (fuel price / efficiency)
            gen_FuelCost = pd.Series(gen_FuelPrice) / df_genInfoTable['Effic']

            # Calculate the variable CO2 cost for each generator (CO2 price * CO2 rate)
            gen_CO2Cost = pd.Series(gen_CO2Price) * df_genInfoTable['CO2Rate']

            # Compute the total variable cost for each generator (fuel cost + CO2 cost + VOM cost), rounded to 2 decimal places
            gen_TotVarCost = gen_FuelCost + gen_CO2Cost + df_gencostInfoTable['VOM_Cost']
            gen_TotVarCost = gen_TotVarCost.round(2)

            # Add columns for the calculated costs with the year as a suffix
            df_gencostInfoTable['FuelCost_' + str(year)] = gen_FuelCost
            df_gencostInfoTable['CO2Cost_' + str(year)] = gen_CO2Cost
            df_gencostInfoTable['TotVarCost_' + str(year)] = gen_TotVarCost
            df_gencostInfoTable['FuelPrice_MultHrlyProfile_' + str(year)] = gen_FuelPrice_idProfile
            df_gencostInfoTable['CO2Price_MultHrlyProfile_' + str(year)] = gen_CO2Price_idProfile

        # prepare pricesInfoTable to be pushed to SQL Server
        # colNames = {'fuel','year','price','price_mult_idProfile','unit'};
        # rename and reorder columns
        df_fuelprices = df_fuelprice_merged.rename(columns={'Fuel':'fuel', 'Year':'year', 'Price':'price', 'idProfiles':'price_mult_idProfile', 'Units':'unit'})
        df_fuelprices = df_fuelprices[['fuel', 'year', 'price', 'price_mult_idProfile', 'unit']]

        # replace NaN values in price_mult_idProfile with empty string
        df_fuelprices['price_mult_idProfile'] = df_fuelprices['price_mult_idProfile'].replace(np.nan, '', regex=True)
        
        return {
            'gencostInfoTable': df_gencostInfoTable,
            'pricesInfoTable': df_fuelprice_merged,
            'fuelprices': df_fuelprices
        }

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
    def __prepareData_flexParamsEV(self, config_years, path_to_demand_profiles):
        # ----------------------------------------- define parameters -----------------------------------------
        Years = config_years
        config_nums = list(range(1, len(config_years) + 1))
        idLoadConfigs = [str(i) for i in config_nums]
        Filename_Params = ['eMobility_Flex_PU','eMobility_Flex_PD','eMobility_Flex_FE']
        Param_Names = ['Demand_Max','Demand_Min','DailyShift_Max']
        RestOfName1 = ['NodalHourly_MW']
        RestOfName2 = ['NodalDaily_MWh']

        # Assuming Years, idLoadConfigs, Filename_Params, Param_Names are already defined
        Years_all = np.repeat(Years, len(Filename_Params)).tolist()  # Repeat each element of Years)
        idLoadConfigs_all = np.repeat(idLoadConfigs, len(Filename_Params)).tolist()  # Repeat each element of idLoadConfigs
        Filename_Params_all = np.tile(Filename_Params, len(Years)).tolist()  # Repeat each element of Filename_Params
        Param_Names_all = np.tile(Param_Names, len(Years)).tolist()  # Repeat each element of Param_Names
        
        # create list to store data
        data = []
        # -------------------- read files --------------------
        # reading files
        for i in range(len(Years) * len(Filename_Params)):
            # separat scenario for filename == 'eMobility_Flex_FE'
            if Filename_Params_all[i] == 'eMobility_Flex_FE':
                # setup filename and path
                filename = f"{Years_all[i]}_{Filename_Params_all[i]}_{RestOfName2[0]}.csv"
                # logging.debug(f"filename: {filename}")
                path_to_read = os.path.join(path_to_demand_profiles, filename)

                # read data
                df = pd.read_csv(path_to_read, header=None, dtype={0: str})
                # rename columns
                df.columns = ['BusName'] + list(range(1, len(df.columns)))

                # convert time series to string
                profiles_text = self.__convert_timeseries_to_string(df, 3)

                # Assign metadata columns
                df = df.assign(
                    year=int(Years_all[i]),
                    idLoadConfig=int(idLoadConfigs_all[i]),
                    Parameter=Param_Names_all[i],
                    # BusName=df.iloc[:, 0],
                    resolution="daily", 
                    unit="MWh",
                    timeSeries = profiles_text
                )
                # drop first column
                # df = df.drop(columns=[0])
                # append to data
                data.append(df)
            else:
                filename = f"{Years_all[i]}_{Filename_Params_all[i]}_{RestOfName1[0]}.csv"
                # setup filename and path
                # logging.debug(f"filename: {filename}")
                path_to_read = os.path.join(path_to_demand_profiles, filename)
                # read data
                df = pd.read_csv(path_to_read, header=None, dtype={0: str})
                # rename first column to busname
                df.columns = ['BusName'] + list(range(1, len(df.columns)))
                
                # convert time series to string
                profiles_text = self.__convert_timeseries_to_string(df, 3)

                # Assign metadata columns
                df = df.assign(
                    year=int(Years_all[i]),
                    idLoadConfig=int(idLoadConfigs_all[i]),
                    Parameter=Param_Names_all[i],
                    # BusName=df.iloc[:, 0],
                    resolution="hourly", 
                    unit="MW",
                    timeSeries = profiles_text
                )
                # drop first column
                # df = df.drop(columns=[0])
                # append to data
                data.append(df)

        # ----------------------------------------- store info -------------------------------------------
        final_df = pd.concat(data, ignore_index=True)
        df_NodalEVflexParamsProfileInfoTable = pd.DataFrame({
            'idLoadConfig': final_df['idLoadConfig'],
            'Parameter': final_df['Parameter'],
            'year': final_df['year'],
            'BusName': final_df['BusName'],
            'resolution': final_df['resolution'],
            'unit': final_df['unit'],
            'timeSeries': final_df['timeSeries']
        })
        # sort table by paramter
        df_NodalEVflexParamsProfileInfoTable = df_NodalEVflexParamsProfileInfoTable.sort_values(by=['Parameter'])
        
        return df_NodalEVflexParamsProfileInfoTable

    def __prepareData_flexParamsHP(self, config_years, path_to_demand_profiles):
        # --------------------- define parameters --------------------------
        Years = config_years
        config_nums = list(range(1, len(config_years) + 1))
        idLoadConfigs = [str(i) for i in config_nums]
        Filename_Params = ['HeatPump_Flex_EmaxCumulPerDay','HeatPump_Flex_EminCumulPerDay','HeatPump_Flex_Pmax']
        Param_Names = ['EnergyCumulPerDay_Max','EnergyCumulPerDay_Min','PowerCapacity_Max']
        RestOfName1 = ['NodalHourly_MWh']
        RestOfName2 = ['Nodal_MW']

        # Assuming Years, idLoadConfigs, Filename_Params, Param_Names are already defined
        Years_all = np.repeat(Years, len(Filename_Params)).tolist()  # Repeat each element of Years)
        idLoadConfigs_all = np.repeat(idLoadConfigs, len(Filename_Params)).tolist()  # Repeat each element of idLoadConfigs
        Filename_Params_all = np.tile(Filename_Params, len(Years)).tolist()  
        Param_Names_all = np.tile(Param_Names, len(Years)).tolist()  

        # create list to store data
        data_HP_profiles = []
        data_HP_pmax = []

        for i in range(len(Years) * len(Filename_Params)):
            if Filename_Params_all[i] == 'HeatPump_Flex_Pmax':
                # ----------------------- read files -------------------------
                filename = f"{Years_all[i]}_{Filename_Params_all[i]}_{RestOfName2[0]}.csv"
                # logging.debug(f"filename: {filename}")
                path_to_read = os.path.join(path_to_demand_profiles, filename) 
                # read data
                df = pd.read_csv(path_to_read, header=None, dtype={0: str})
                # rename columns
                df.columns = ['BusName', 'value']

                # Assign metadata columns
                df = df.assign(
                    year=int(Years_all[i]),
                    idLoadConfig=int(idLoadConfigs_all[i]),
                    Parameter=Param_Names_all[i],
                    # BusName=df.iloc[:, 0],
                    # resolution="daily", 
                    unit="MW"
                )
                
                # append to data
                data_HP_pmax.append(df)

            else:
                filename = f"{Years_all[i]}_{Filename_Params_all[i]}_{RestOfName1[0]}.csv"
                # logging.debug(f"filename: {filename}")
                path_to_read = os.path.join(path_to_demand_profiles, filename) 
                # read data
                df = pd.read_csv(path_to_read, header=None, dtype={0: str})
                # rename columns
                df.columns = ['BusName'] + list(range(1, len(df.columns)))

                # convert time series to string
                profiles_text = self.__convert_timeseries_to_string(df, 3)

                # Assign metadata columns
                df = df.assign(
                    year=int(Years_all[i]),
                    idLoadConfig=int(idLoadConfigs_all[i]),
                    Parameter=Param_Names_all[i],
                    # BusName=df.iloc[:, 0],
                    resolution="hourly", 
                    unit="MWh",
                    timeSeries = profiles_text
                )
                
                # append to data
                data_HP_profiles.append(df)
        # ----------------------------------------- store info -------------------------------------------
        final_df_HP_profiles = pd.concat(data_HP_profiles, ignore_index=True)
        df_flex_profiles_hp = pd.DataFrame({
            'idLoadConfig': final_df_HP_profiles['idLoadConfig'],
            'Parameter': final_df_HP_profiles['Parameter'],
            'year': final_df_HP_profiles['year'],
            'BusName': final_df_HP_profiles['BusName'],
            'resolution': final_df_HP_profiles['resolution'],
            'unit': final_df_HP_profiles['unit'],
            'timeSeries': final_df_HP_profiles['timeSeries']
        })
        final_df_HP_pmax = pd.concat(data_HP_pmax, ignore_index=True)
        df_flex_params_hp = pd.DataFrame({
            'idLoadConfig': final_df_HP_pmax['idLoadConfig'],
            'Parameter': final_df_HP_pmax['Parameter'],
            'year': final_df_HP_pmax['year'],
            'BusName': final_df_HP_pmax['BusName'],
            'unit': final_df_HP_pmax['unit'],
            'value': final_df_HP_pmax['value']
        })
        
        return df_flex_profiles_hp, df_flex_params_hp

        
    def __prepareData_nodalLoad(self, config_years, path_to_demand_profiles):
        # define parameters
        Years = config_years
        config_nums = list(range(1, len(config_years) + 1))
        idLoadConfigs = [str(i) for i in config_nums]
        Loads = ['Conventional','eMobility','HeatPump','Electrolysis']
        RestOfName = ['Load_NodalHourly_MWh']

        # setup for each year-load combination
        Years_all = np.repeat(Years, len(Loads)).tolist()  # Repeat each element of Years
        idLoadConfigs_all = np.repeat(idLoadConfigs, len(Loads)).tolist()  # Repeat each element of idLoadConfigs
        Loads_all = Loads * len(Years)  # Repeat entire Loads list for each year
        

        # create list to store data
        data = []

        for i in range(len(Years) * len(Loads)):
            # construct filename and path
            filename = f"{Years_all[i]}_{Loads_all[i]}_{RestOfName[0]}.csv"
            # logging.debug(filename)
            path_to_read = os.path.join(path_to_demand_profiles, filename)
            
            # read data
            df = pd.read_csv(path_to_read, header=None, dtype={0: str})
            # Assign metadata columns
            df = df.assign(
                LoadType=Loads_all[i],
                year=int(Years_all[i]),
                idLoadConfig=int(idLoadConfigs_all[i]),
                BusName=df.iloc[:, 0],  # First column contains Bus names
                unit="MWh",
                Resolution="hourly"
            )
            # drop first column
            df = df.drop(columns=[0])

            data.append(df)
        
        final_df = pd.concat(data, ignore_index=True)

        # ------------------------------ create profiles_text ------------------------------
        # convert time series to string
        profiles_text = self.__convert_timeseries_to_string(final_df, 3)

        # ----------------------------------------- store info -------------------------------------------
        # Define column names
        df_NodalLoadProfileInfoTable = pd.DataFrame({
            'idLoadConfig': final_df['idLoadConfig'],
            'LoadType': final_df['LoadType'],
            'year': final_df['year'],
            'BusName': final_df['BusName'],
            'unit': final_df['unit'],
            'timeSeries': profiles_text
        })
        # sort data by loadtype
        df_NodalLoadProfileInfoTable = df_NodalLoadProfileInfoTable.sort_values(by='LoadType')

        return df_NodalLoadProfileInfoTable

    def __prepareData_securityRef(self, df):
        # Step 2: Convert numerics into a comma-separated string
        vals_num_DNS = df['DNS'].dropna()  
        vals_num_NLF = df['NLF'].dropna()  

        # Convert numerics to string with comma separation
        vals_text_DNS = ','.join([f'{x:.2f}' for x in vals_num_DNS])
        vals_text_NLF = ','.join([f'{x:.2f}' for x in vals_num_NLF])

        # Step 3: Add brackets and convert to a list with one long string
        vals_cell_DNS = f'[{vals_text_DNS}]'
        vals_cell_NLF = f'[{vals_text_NLF}]'

        # Step 4: Store the information into a DataFrame (equivalent to table in MATLAB)
        security_refs_df = pd.DataFrame({
            'DNS_vals': [vals_cell_DNS],
            'NLF_vals': [vals_cell_NLF]
        })

        return security_refs_df

    def __prepareData_distProfiles(self, df, df_busInfoTable):
        # get the 'hourly values' from df
        timeseries_index = df.columns.get_loc('timeSeries')
        
        hourly_values_df = df.iloc[:, timeseries_index:]
        profiles_text = []  # List to store the processed text for each profile
        
        for index, row in hourly_values_df.iterrows():
                    # Extract the hourly values
            profile_num = row.values

            # Remove any NaN values
            profile_num = profile_num[~pd.isna(profile_num)]

            # format the profile to have 4 decimal places
            profile_list = [round(x, 4) for x in profile_num]

            # Append the json list to the profiles_text list
            profiles_text.append(json.dumps(profile_list))  

            

        # create df with the profile information
        df_distprofileInfoTable = pd.DataFrame({
            'idProfile': df['Profile Number'],
            'ProfileName': df['Name (node_id)'],
            'Type': df['type'],
            'Resolution': df['resolution'],
            'Unit': df['unit'],
            'timeSeries': profiles_text
        })
        df_distprofileInfoTable_bus = df_busInfoTable[['DistIvProfile', 'NodeID']].copy()
        # rename columns
        df_distprofileInfoTable_bus.columns = ['idProfile', 'ProfileName']

        # create series with all zeros
        all_zeros_series = pd.Series(np.zeros(8760))
        # convert the series to a string with up to 16 digits after the decimal
        all_zeros_text = ','.join([f'{x:.2f}' for x in all_zeros_series])

        # add columns
        df_distprofileInfoTable_bus['Type'] = 'DistIvGen'
        df_distprofileInfoTable_bus['Resolution'] = 1.0
        df_distprofileInfoTable_bus['Unit'] = 'MWh'
        df_distprofileInfoTable_bus['timeSeries'] = f'[{all_zeros_text}]'

        # concatenate the two dataframes
        df_distprofileInfoTable = pd.concat([df_distprofileInfoTable, df_distprofileInfoTable_bus], ignore_index=True)

        # create df for sql table
        # colNames = {'idDistProfile','name','type','resolution','unit','timeSeries'};
        df_distprofiles = pd.DataFrame({
            'idDistProfile': df_distprofileInfoTable['idProfile'],
            'name': df_distprofileInfoTable['ProfileName'],
            'type': df_distprofileInfoTable['Type'],
            'resolution': df_distprofileInfoTable['Resolution'],
            'unit': df_distprofileInfoTable['Unit'],
            'timeSeries': df_distprofileInfoTable['timeSeries']
        })

        return {
            'distprofileInfoTable': df_distprofileInfoTable,
            'distprofiles': df_distprofiles
        }

    def __prepareData_gemel(self, df_projection, df_workforce):
        # create dfs
        df_projection = pd.DataFrame({
            'item': df_projection['Item'],
            'scenario': df_projection['Scenario'],
            'year': df_projection['year'],
            'value': df_projection['Val']
        })

        # workforce
        # colNames = {'popscen','year','value'};
        df_workforce = pd.DataFrame({
            'popscen': df_workforce['popscen'],
            'year': df_workforce['year'],
            'value': df_workforce['Val']
        })
        
        return df_projection, df_workforce

    def __prepareData_gensExtra(self, df):
        # clean duplicates in header
        # Rename columns to avoid duplicates
        new_columns = []
        for col in df.columns:
            if col in new_columns:
                new_columns.append(f"{col}_2")
                logging.debug(f"Renaming duplicate column: {col} to {col}_2 in gens_extra")
            else:
                new_columns.append(col)

        df.columns = new_columns

        # create new df
        # colNames = {'GenNum','GenName','GenType','Technology','UnitType','NodeNum','NodeID','StartYr','EndYr','Pmax_methdac','Pmin_methdac','Emax_h2stor','Emin_h2stor','VOM_methdac','InvCost_h2stor','InvCost_methdac','FOM_elzr','FOM_h2stor','FOM_methdac','Conv_elzr','Conv_fc','Conv_methdac_h2','Conv_methdac_el','Conv_methdac_co2','MaxInjectionRate_h2stor','MaxWithdrawalRate_h2stor','FuelType_methdac','FuelType_ch4_import','FuelType_h2_domestic','FuelType_h2_import','Ind_h2_MarketConnect','h2Stor_Type','ElecGen_Type'};
        df_gen_extra_InfoTable = pd.DataFrame({
            'GenNum': df['GenNum'],
            'GenName': df['Gen_ID'],
            'GenType': df['UnitType'],
            'Technology': df['SubType'],
            'UnitType': df['UnitType.1'],
            'NodeNum': df['NodeNum'],
            'NodeID': df['NodeId'],
            'StartYr': df['start_year'],
            'EndYr': df['end_year (50yrNucl)'],
            'Pmax_methdac': df['P_max_methdac (MW-th-Gas)'],
            'Pmin_methdac': df['P_min_methdac (MW-th-Gas)'],
            'Emax_h2stor': df['Emax (tonne H2)'],
            'Emin_h2stor': df['Emin (tonne H2)'],
            'VOM_methdac': df['VOM_methdac (Euro/MWh-th-Gas)'],
            'InvCost_h2stor': df['Investment Cost H2Store (Euro/tonne-H2/yr)'],
            'InvCost_methdac': df['Investment Cost MethDac (Euro/MW-th-Gas/yr)'],
            'FOM_elzr': df['FOM Cost Electrolyzer (Euro/MW-el/yr)'],
            'FOM_h2stor': df['FOM Cost H2Store (Euro/tonne-H2/yr)'],
            'FOM_methdac': df['FOM Cost MethDac (Euro/MW-th-Gas/yr)'],
            'Conv_elzr': df['Conv_elect (tonne-H2/MWh-el)'],
            'Conv_fc': df['Conv_fc (MWh-el/tonne-H2)'],
            'Conv_methdac_h2': df['Conv_methdac1 (MWh-th-Gas/tonne-H2)'],
            'Conv_methdac_el': df['Conv_methdac2 (MWh-th-Gas/MWh-el)'],
            'Conv_methdac_co2': df['Conv_methdac3 (MWh-th-Gas/tonCO2)'],
            'MaxInjectionRate_h2stor': df['Max Injection Rate H2Stor (% Emax / day)'],
            'MaxWithdrawalRate_h2stor': df['Max Withdrawal Rate H2Stor (% Emax / day)'],
            'FuelType_methdac': df['Fuel price paid for domestic Gas-Syn'],
            'FuelType_ch4_import': df['Fuel price paid for imported Gas-Syn'],
            'FuelType_h2_domestic': df['Fuel price paid for domestic H2'],
            'FuelType_h2_import': df['Fuel price paid for imported H2'],
            'Ind_h2_MarketConnect': df['Indicator connection to H2 market infrastructure'],
            'h2Stor_Type': df['H2Stor Type'],
            'ElecGen_Type': df['Generator_Type']
        })

        return df_gen_extra_InfoTable

    def __prepareData_genTypeData(self, df):
        # replace spaces in front of column names
        df.columns = [col[1:] if col.startswith(' ') else col for col in df.columns]
        # create dfs
        # colNames = {'GenType','Technology','Component','Year','Subsidy_Indicator','InvCost_UpFront','InvCost_Annual_NoSubsidy','InvCost_Annual_Subsidy','WACC','Lifetime','AnnuityFactor','Subsidy_Fraction'};
        df_GenTypeData = pd.DataFrame({
            'GenType': df['GenType'],
            'Technology': df['Technology'],
            'Component': df['Component'],
            'Year': df['Year'],
            'Subsidy_Indicator': df['Subsidized'],
            'InvCost_UpFront': df['InvestmentCosts [EUR/kW] [EUR/kg]'],
            'InvCost_Annual_NoSubsidy': df['Annualized InvCost (without subsidies) [EUR/kW/yr] [EUR/kg/yr]'],
            'InvCost_Annual_Subsidy': df['Annualized InvCost (with subsidies) [EUR/kW/yr] [EUR/kg/yr]'],
            'WACC': df['WACC'],
            'Lifetime': df['Lifetime'],
            'AnnuityFactor': df['Annuity Factor'],
            'Subsidy_Fraction': df['Subsidies']
        })

        # table for sql
        # colNames = {'GenType','Technology','Component','Year','Subsidy_Indicator','InvCost_UpFront','InvCost_Annual_NoSubsidy','InvCost_Annual_Subsidy','WACC','Lifetime','AnnuityFactor','Subsidy_Fraction'};
        df_gentypedata = pd.DataFrame({
            'GenType': df['GenType'],
            'Technology': df['Technology'],
            'Component': df['Component'],
            'Year': df['Year'],
            'Subsidy_Indicator': df['Subsidized'],
            'InvCost_UpFront': df['InvestmentCosts [EUR/kW] [EUR/kg]'],
            'InvCost_Annual_NoSubsidy': df['Annualized InvCost (without subsidies) [EUR/kW/yr] [EUR/kg/yr]'],
            'InvCost_Annual_Subsidy': df['Annualized InvCost (with subsidies) [EUR/kW/yr] [EUR/kg/yr]'],
            'WACC': df['WACC'],
            'Lifetime': df['Lifetime'],
            'AnnuityFactor': df['Annuity Factor'],
            'Subsidy_Fraction': df['Subsidies']
        })
        
        return df_GenTypeData, df_gentypedata

    def __prepareData_profiles(self, df):
        # --------------------------------------------------------------------------------------------------------------------------------------------
        # hourly_values = df.columns[digit_mask]
        hourly_values = [col for col in df.columns if isinstance(col, (int, float, np.number))and pd.notna(col)]
        profiles_text = []  # List to store the processed text for each profile

        for index, row in df.iterrows():
            # Extract the hourly values
            profile_num = row[hourly_values].values

            # Remove any NaN values
            profile_num = profile_num[~pd.isna(profile_num)]

            # format the profile to have 4 decimal places
            profile_list = [round(x, 4) for x in profile_num]

            # Append the json list to the profiles_text list
            profiles_text.append(json.dumps(profile_list))  


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
    
    def __prepareData_swissAnnualTarget(self, df):
        # create dfs
        # --------------------------------------------------------------------------------------------------------------------------------------------
        # pull_excel
        # colNames = {'TargetName','Year','Type','Value','Units','idProfile'};
        df_SwissAnnualTargetsInfoTable = pd.DataFrame({
            'TargetName': df['Name'],
            'Year': df['Year'],
            'Type': df['Type'],
            'Value': df['Value'],
            'Units': df['Units'],
            'idProfile': df['idProfile']
        })
        # --------------------------------------------------------------------------------------------------------------------------------------------
        # for sql table
        # colNames = {'idAnnualTargetsConfig','name','Year'};
        unique_years = df['Year'].unique().tolist()
        
        df_annualtargetsconfiguration = pd.DataFrame({
            'idAnnualTargetsConfig': range(1, len(unique_years) + 1),
            'Year': unique_years
        })
        # add the name dynamically for each year
        df_annualtargetsconfiguration['name'] = df_annualtargetsconfiguration['Year'].apply(lambda x: f'{x}_SwissTargets')
        # reorder columns to match sql table
        df_annualtargetsconfiguration = df_annualtargetsconfiguration[['idAnnualTargetsConfig', 'name', 'Year']]

        # --------------------------------------------------------------------------------------------------------------------------------------------
        # colNames = {'idAnnualTargetsConfig','TargetName','Year','Type','Value','Units','idProfile'};
        df_annualtargetsdata = df_SwissAnnualTargetsInfoTable
        # add the idAnnualTargetsConfig where the year matches to the df_annualtargetsdata
        df_annualtargetsdata = df_annualtargetsdata.merge(df_annualtargetsconfiguration[['idAnnualTargetsConfig', 'Year']], on='Year')
        # reorder columns to match sql table
        df_annualtargetsdata = df_annualtargetsdata[['idAnnualTargetsConfig', 'TargetName', 'Year', 'Type', 'Value', 'Units', 'idProfile']]
        # replace NaN with blanks in idProfile
        # df_annualtargetsdata['idProfile'] = df_annualtargetsdata['idProfile'].replace(np.nan, '')
        
        # --------------------------------------------------------------------------------------------------------------------------------------------
        # remove empty rows
        df_SwissAnnualTargetsInfoTable = df_SwissAnnualTargetsInfoTable.dropna(how='all')
        df_annualtargetsdata = df_annualtargetsdata.dropna(how='all')
        df_annualtargetsconfiguration = df_annualtargetsconfiguration.dropna(how='all')
        
        return {
            'SwissAnnualTargetsInfoTable': df_SwissAnnualTargetsInfoTable,
            'swiss_annual_targets_configinfo': df_annualtargetsconfiguration,
            'swiss_annual_targets_configuration': df_annualtargetsdata
        }
    
    def __prepareData_market(self, df):
        # create dfs
        df_scenarioInfoTable = pd.DataFrame({
            'MarketName': df['Name'],
            'year': df['Year'],
            'JSON_info': df['JSON_Info'],
            'runParamData': df['runParamData']
        })

        # delete empty rows
        df_scenarioInfoTable = df_scenarioInfoTable.dropna(how='all')

        # colNames of sql table = {'idMarketsConfig','name','year','MarketsConfigDataStructure'};
        df_marketsconfiguration = pd.DataFrame({
            'idMarketsConfig': range(1, len(df_scenarioInfoTable) + 1),
            'name': df_scenarioInfoTable['MarketName'],
            'year': df_scenarioInfoTable['year'],
            'MarketsConfigDataStructure': df_scenarioInfoTable['runParamData']
        })

        # delete empty rows
        df_marketsconfiguration = df_marketsconfiguration.dropna(how='all')

        return {
            'scenarioInfoTable': df_scenarioInfoTable,
            'marketsconfiguration': df_marketsconfiguration
        }

    def __prepareData_scenarioConfig(self, df_scenarioInfoTable):
        # Define columns as lists
        Scen_names = {
            2018: '2018_Hist',
            2020: '2020_Hist',
            2030: '2030_PATHFNDR',
            2040: '2040_PATHFNDR',
            2050: '2050_PATHFNDR'
        }

        Scen_names_list = list(Scen_names.values())
        IDs_ScenarioConfig = list(range(1, len(Scen_names) + 1))
        IDs_NetworkConfig = list(range(1, len(Scen_names) + 1))
        IDs_LoadConfig = list(range(1, len(Scen_names) + 1))
        IDs_GenConfig = list(range(1, len(Scen_names) + 1))
        IDs_MarketConfig = list(range(1, len(Scen_names) + 1))
        IDs_SwissAnnualTargetsConfig = list(range(1, len(Scen_names) + 1))
        IDs_DistGenConfig = list(range(1, len(Scen_names) + 1))

        # get data from inputDB_modified['scenarioInfoTable'] and use as list
        Scen_runParam = list(df_scenarioInfoTable['runParamData'])
        Scen_year = list(df_scenarioInfoTable['year'])

        # create new df
        df_scenarioconfiguration = pd.DataFrame({
            'idScenario': IDs_ScenarioConfig,
            'idNetworkConfig': IDs_NetworkConfig,
            'idLoadConfig': IDs_LoadConfig,
            'idGenConfig': IDs_GenConfig,
            'idMarketsConfig': IDs_MarketConfig,
            'idAnnualTargetsConfig': IDs_SwissAnnualTargetsConfig,
            'idDistGenConfig': IDs_DistGenConfig,
            'name': Scen_names_list,
            'runParamDataStructure': Scen_runParam,
            'Year': Scen_year
        })

        return df_scenarioconfiguration

    def __prepareData_networkConfig(self, config_years_int):
        # define network config names baeed on years
        network_config_names = {}
        for year in config_years_int:
            network_config_names[year] = f"{year}_DisAgg_EuNTCs"
        # Create a mask for rows where TAP is not equal to 0 (transformers)
        df_branch = self.__inputDB_modified['branch']
        transformer_mask_total = df_branch['ratio'] != 0
        line_mask_total = ~transformer_mask_total

        for year in config_years_int:
            # initialize default value for idStartNetConfig
            idStartNetConfig = 1

            # Get existing data from networkConfigInfo
            if 'networkconfiginfo' in self.__inputDB_modified:
                df_networkconfiginfotemp = self.__inputDB_modified['networkconfiginfo']
                if not df_networkconfiginfotemp.empty:
                    idStartNetConfig = df_networkconfiginfotemp['idNetworkConfig'].max() + 1

            # Config3 oder Config1????
            datatableNetworkConfig1 = pd.DataFrame({
                'idNetworkConfig': [idStartNetConfig],
                'name': [network_config_names[year]],
                'year': [year],
                'baseMVA': 100,
                'MatpowerVersion': ''
            })

            # save to inputDB_modified
            if 'networkconfiginfo' in self.__inputDB_modified:
                self.__inputDB_modified['networkconfiginfo'] = pd.concat([self.__inputDB_modified['networkconfiginfo'], datatableNetworkConfig1], ignore_index=True)
            else:
                self.__inputDB_modified['networkconfiginfo'] = datatableNetworkConfig1

            # mask to filter data for the year
            # node mask # Create masks for nodes with Start/End year in 'busInfoTable'
            node_mask = (self.__inputDB_modified['busInfoTable']['StartYr'] <= year) & (self.__inputDB_modified['busInfoTable']['EndYr'] >= year)

            # Create masks for lines with Start/End year in 'branchInfoTable'
            line_mask =  line_mask_total & (self.__inputDB_modified['branchInfoTable']['StartYr'] <= year) & (self.__inputDB_modified['branchInfoTable']['EndYr'] >= year)
            
            # Create masks for transformers with Start/End year in 'branchInfoTable'
            trafo_mask = transformer_mask_total & (self.__inputDB_modified['branchInfoTable']['StartYr'] <= year) & (self.__inputDB_modified['branchInfoTable']['EndYr'] >= year)

            # create separate masks for lines (only lines, not transformers)
            line_mask2 = (self.__inputDB_modified['branchInfoTable']['StartYr'][line_mask_total] <= year) & (self.__inputDB_modified['branchInfoTable']['EndYr'][line_mask_total] >= year)

            # create separate masks for transformers
            trafo_mask2 = (self.__inputDB_modified['branchInfoTable']['StartYr'][transformer_mask_total] <= year) & (self.__inputDB_modified['branchInfoTable']['EndYr'][transformer_mask_total] >= year)

            # Bus Configuration
            logging.debug(f" --Setup busconfiguration table for year {year}...")
            # make table of data to push to MySQL
            datatableBusConfig1 = pd.DataFrame({
                'idNetworkConfig': idStartNetConfig,
                'idBus': self.__inputDB_modified['bus'].loc[node_mask, 'bus_i'],
                'BusName': self.__inputDB_modified['busInfoTable'].loc[node_mask, 'NodeID'],  
                'Vmax': self.__inputDB_modified['bus'].loc[node_mask, 'Vmax'],
                'Vmin': self.__inputDB_modified['bus'].loc[node_mask, 'Vmin'],
                'WindShare': self.__inputDB_modified['busInfoTable'].loc[node_mask, 'WindShare_{}'.format(year)],
                'SolarShare': self.__inputDB_modified['busInfoTable'].loc[node_mask, 'SolarShare_{}'.format(year)],
                'idDistProfile': self.__inputDB_modified['busInfoTable'].loc[node_mask, 'DistIvProfile']
            })

            # save to inputDB_modified
            if 'busconfiguration' in self.__inputDB_modified:
                self.__inputDB_modified['busconfiguration'] = pd.concat([self.__inputDB_modified['busconfiguration'], datatableBusConfig1], ignore_index=True)
            else:
                self.__inputDB_modified['busconfiguration'] = datatableBusConfig1

            # Line configuration
            logging.debug(f" --Setup lineconfiguration table for year {year}...")
            # make table of data to push to MySQL
            datatableLineConfig1 = pd.DataFrame({
                'idNetworkConfig': idStartNetConfig,
                'idLine': self.__inputDB_modified['linedata'].loc[line_mask2, 'idLine'],
                'LineName': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'LineID'],
                'idFromBus': self.__inputDB_modified['branch'].loc[line_mask, 'fbus'],
                'idToBus': self.__inputDB_modified['branch'].loc[line_mask, 'tbus'],
                'angmin': self.__inputDB_modified['branch'].loc[line_mask, 'angmin'],
                'angmax': self.__inputDB_modified['branch'].loc[line_mask, 'angmax'],
                'status': self.__inputDB_modified['branch'].loc[line_mask, 'status'],
                'FromBusName': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'FromNodeID'],
                'ToBusName': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'ToNodeID'],
                'FromCountry': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'FromNodeCountry'],
                'ToCountry': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'ToNodeCountry'],
                'Ind_CrossBord': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'Ind_CrossBorder'],
                'Ind_Agg': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'Ind_Aggreg'],
                'Ind_HVDC': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'Ind_HVDC'],
                'Candidate': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'Candidate'],
                'CandCost': self.__inputDB_modified['branchInfoTable'].loc[line_mask, 'CandCost']
            }) 
            
            # save to inputDB_modified
            if 'lineconfiguration' in self.__inputDB_modified:
                self.__inputDB_modified['lineconfiguration'] = pd.concat([self.__inputDB_modified['lineconfiguration'], datatableLineConfig1], ignore_index=True)
            else:
                self.__inputDB_modified['lineconfiguration'] = datatableLineConfig1

            # Transformer configuration
            logging.debug(f" --Setup transformerconfiguration table for year {year}...")
            # make table of data to push to MySQL
            datatableTransConfig3 = pd.DataFrame({
                'idNetworkConfig': idStartNetConfig,
                'idTransformer': self.__inputDB_modified['transformerdata'].loc[trafo_mask2, 'idTransformer'],  # Ensure that this is a 1D array
                'TrafoName': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'LineID'],  # Adjust according to your actual column
                'idFromBus': self.__inputDB_modified['branch'].loc[trafo_mask, 'fbus'],
                'idToBus': self.__inputDB_modified['branch'].loc[trafo_mask, 'tbus'],
                'angmin': self.__inputDB_modified['branch'].loc[trafo_mask, 'angmin'],
                'angmax': self.__inputDB_modified['branch'].loc[trafo_mask, 'angmax'],
                'status': self.__inputDB_modified['branch'].loc[trafo_mask, 'status'],
                'FromBusName': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'FromNodeID'],
                'ToBusName': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'ToNodeID'],
                'FromCountry': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'FromNodeCountry'],
                'ToCountry': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'ToNodeCountry'],
                'Candidate': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'Candidate'],
                'CandCost': self.__inputDB_modified['branchInfoTable'].loc[trafo_mask, 'CandCost']
            })
            
            # save to inputDB_modified
            if 'transformerconfiguration' in self.__inputDB_modified:
                self.__inputDB_modified['transformerconfiguration'] = pd.concat([self.__inputDB_modified['transformerconfiguration'], datatableTransConfig3], ignore_index=True)
            else:
                self.__inputDB_modified['transformerconfiguration'] = datatableTransConfig3


    def __prepareData_genConfig(self, config_years_int):
        
        GenConfigName = {
            2018: '2018_Hist',
            2020: '2020_Hist',
            2030: '2030_PATHFNDR',
            2040: '2040_PATHFNDR',
            2050: '2050_PATHFNDR'
        }
        for year in config_years_int:
            # initialize default value for idStartGenConfig
            idStartGenConfig = 1

            # Get existing data from networkConfigInfo
            if 'genconfiginfo' in self.__inputDB_modified:
                df_genconfiginfotemp = self.__inputDB_modified['genconfiginfo']
                if not df_genconfiginfotemp.empty:
                    idStartGenConfig = df_genconfiginfotemp['idGenConfig'].max() + 1

            # Config3 oder Config1????
            datatableNetworkConfig1 = pd.DataFrame({
                'idGenConfig': [idStartGenConfig],
                'name': [GenConfigName[year]],
                'year': [year]
            })

            # save to inputDB_modified
            if 'genconfiginfo' in self.__inputDB_modified:
                self.__inputDB_modified['genconfiginfo'] = pd.concat([self.__inputDB_modified['genconfiginfo'], datatableNetworkConfig1], ignore_index=True)
            else:
                self.__inputDB_modified['genconfiginfo'] = datatableNetworkConfig1

            # gen mask to identify all gens with Start/End year for the specific year
            gen_mask = (self.__inputDB_modified['genInfoTable']['StartYr'] <= year) & (self.__inputDB_modified['genInfoTable']['EndYr'] >= year)

            gen_extra_mask = (self.__inputDB_modified['gen_extra_InfoTable']['StartYr'] <= year) & (self.__inputDB_modified['gen_extra_InfoTable']['EndYr'] >= year)

            # Gen Configuration
            logging.debug(f" --Setup genconfiguration table for year {year}...")
        
            datatableGenConfig = pd.DataFrame({
                'idGenConfig': idStartGenConfig,  
                'idBus': self.__inputDB_modified['gen'].loc[gen_mask, 'bus'],
                'idGen': self.__inputDB_modified['gendata'].loc[gen_mask, 'idGen'],
                'GenName': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'GenID'],
                'idProfile': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'idProfile'],
                'CandidateUnit': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'Candidate'],
                'Pmax': self.__inputDB_modified['gen'].loc[gen_mask, 'Pmax'],
                'Pmin': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'Pmin_db'],
                'Qmax': self.__inputDB_modified['gen'].loc[gen_mask, 'Qmax'],
                'Qmin': self.__inputDB_modified['gen'].loc[gen_mask, 'Qmin'],
                'Emax': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'Emax'],
                'Emin': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'Emin'],
                'E_ini': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'Eini'],
                'VOM_Cost': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'VOM_Cost'],
                'FOM_Cost': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'FOM_Cost'],
                'InvCost': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'InvCost'],
                'InvCost_E': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'InvCost_E'],
                'InvCost_Charge': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'InvCost_Charge'],
                'StartCost': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'StartCost'],
                'TotVarCost': self.__inputDB_modified['gencostInfoTable'].loc[gen_mask, 'TotVarCost_{}'.format(year)],
                'FuelType': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'FuelType'],
                'CO2Type': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'CO2_PriceInd'],
                'status': self.__inputDB_modified['gen'].loc[gen_mask, 'status'],
                'HedgeRatio': self.__inputDB_modified['genInfoTable'].loc[gen_mask, 'GenHedgeR_{}'.format(year)]
            })
            
            # save to inputDB_modified
            if 'genconfiguration' in self.__inputDB_modified:
                self.__inputDB_modified['genconfiguration'] = pd.concat([self.__inputDB_modified['genconfiguration'], datatableGenConfig], ignore_index=True)
            else:
                self.__inputDB_modified['genconfiguration'] = datatableGenConfig


            datatableGenConfig_Extra = pd.DataFrame({
                'idGenConfig': idStartGenConfig,
                'GenNum': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'GenNum'],
                'GenName': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'GenName'],
                'NodeNum': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'NodeNum'],
                'GenType': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'GenType'],
                'Technology': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Technology'],
                'UnitType': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'UnitType'],
                'Pmax_methdac': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Pmax_methdac'],
                'Pmin_methdac': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Pmin_methdac'],
                'Emax_h2stor': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Emax_h2stor'],
                'Emin_h2stor': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Emin_h2stor'],
                'VOM_methdac': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'VOM_methdac'],
                'InvCost_h2stor': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'InvCost_h2stor'],
                'InvCost_methdac': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'InvCost_methdac'],
                'FOM_elzr': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FOM_elzr'],
                'FOM_h2stor': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FOM_h2stor'],
                'FOM_methdac': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FOM_methdac'],
                'Conv_elzr': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Conv_elzr'],
                'Conv_fc': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Conv_fc'],
                'Conv_methdac_h2': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Conv_methdac_h2'],
                'Conv_methdac_el': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Conv_methdac_el'],
                'Conv_methdac_co2': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Conv_methdac_co2'],
                'MaxInjectionRate_h2stor': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'MaxInjectionRate_h2stor'],
                'MaxWithdrawalRate_h2stor': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'MaxWithdrawalRate_h2stor'],
                'FuelType_methdac': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FuelType_methdac'],
                'FuelType_ch4_import': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FuelType_ch4_import'],
                'FuelType_h2_domestic': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FuelType_h2_domestic'],
                'FuelType_h2_import': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'FuelType_h2_import'],
                'Ind_h2_MarketConnect': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'Ind_h2_MarketConnect'],
                'h2Stor_Type': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'h2Stor_Type'],
                'ElecGen_Type': self.__inputDB_modified['gen_extra_InfoTable'].loc[gen_extra_mask, 'ElecGen_Type']
            })

            # save to inputDB_modified
            if 'genconfiguration_extra' in self.__inputDB_modified:
                self.__inputDB_modified['genconfiguration_extra'] = pd.concat([self.__inputDB_modified['genconfiguration_extra'], datatableGenConfig_Extra], ignore_index=True)
            else:
                self.__inputDB_modified['genconfiguration_extra'] = datatableGenConfig_Extra


    def __prepareData_loadConfig(self, config_years_int):

        load_config_names = {}
        for year in config_years_int:
            load_config_names[year] = f"{year}_DisAggByNode_AddRestEULoads"

        # DemandShare_2030, 2040 and _2050 is not avaialble in the database, use DemandShare_2025 instead
        # add column DemandShare_2030 to busInfoTable temporarily
        self.__inputDB_modified['busInfoTable']['DemandShare_2030'] = self.__inputDB_modified['busInfoTable']['DemandShare_2025']
        self.__inputDB_modified['busInfoTable']['DemandShare_2040'] = self.__inputDB_modified['busInfoTable']['DemandShare_2025']
        self.__inputDB_modified['busInfoTable']['DemandShare_2050'] = self.__inputDB_modified['busInfoTable']['DemandShare_2025']

        for year in config_years_int:
            # initialize default value for idStartLoadConfig
            idStartLoadConfig = 1

            # Get existing data from loadConfigInfo
            if 'loadconfiginfo' in self.__inputDB_modified:
                df_loadconfiginfotemp = self.__inputDB_modified['loadconfiginfo']
                if not df_loadconfiginfotemp.empty:
                    idStartLoadConfig = df_loadconfiginfotemp['idLoadConfig'].max() + 1

            # Config3 oder Config1????
            datatableLoadConfigInfo = pd.DataFrame({
                'idLoadConfig': [idStartLoadConfig],
                'name': [load_config_names[year]],
                'year': [year]
            })

            # save to inputDB_modified
            if 'loadconfiginfo' in self.__inputDB_modified:
                self.__inputDB_modified['loadconfiginfo'] = pd.concat([self.__inputDB_modified['loadconfiginfo'], datatableLoadConfigInfo], ignore_index=True)
            else:
                self.__inputDB_modified['loadconfiginfo'] = datatableLoadConfigInfo

            # load mask to identify all loads with Start/End year for the specific year
            bus_mask = (self.__inputDB_modified['busInfoTable']['StartYr'] <= year) & (self.__inputDB_modified['busInfoTable']['EndYr'] >= year)

            logging.debug(f" --Setup loadconfiguration table for year {year}...")

            # make table of data to push to MySQL
            # colNames = {'idLoadConfig','idBus','idLoad','idProfile','DemandShare','idProfile_eMobility','idProfile_eHeatPump','idProfile_eHydrogen'}; 
            datatableLoadConfig = pd.DataFrame({
                'idLoadConfig': idStartLoadConfig,
                'idBus': self.__inputDB_modified['bus'].loc[bus_mask, 'bus_i'],
                'idLoad': self.__inputDB_modified['loaddata'].loc[bus_mask, 'idLoad'],
                'idProfile': self.__inputDB_modified['busInfoTable'].loc[bus_mask, 'LoadProfile_{}'.format(year)],
                'DemandShare': self.__inputDB_modified['busInfoTable'].loc[bus_mask, 'DemandShare_{}'.format(year)],
                'idProfile_eMobility': self.__inputDB_modified['busInfoTable'].loc[bus_mask, 'LoadProfile_eMobility_{}'.format(year)],
                'idProfile_eHeatPump': self.__inputDB_modified['busInfoTable'].loc[bus_mask, 'LoadProfile_eHeatPump_{}'.format(year)],
                'idProfile_eHydrogen': self.__inputDB_modified['busInfoTable'].loc[bus_mask, 'LoadProfile_eHydrogen_{}'.format(year)]
            })

            
            # save to inputDB_modified
            if 'loadconfiguration' in self.__inputDB_modified:
                self.__inputDB_modified['loadconfiguration'] = pd.concat([self.__inputDB_modified['loadconfiguration'], datatableLoadConfig], ignore_index=True)
            else:
                self.__inputDB_modified['loadconfiguration'] = datatableLoadConfig
        
        # remove column DemandShare_20XX from busInfoTable
        self.__inputDB_modified['busInfoTable'] = self.__inputDB_modified['busInfoTable'].drop(columns=['DemandShare_2030'])
        self.__inputDB_modified['busInfoTable'] = self.__inputDB_modified['busInfoTable'].drop(columns=['DemandShare_2040'])
        self.__inputDB_modified['busInfoTable'] = self.__inputDB_modified['busInfoTable'].drop(columns=['DemandShare_2050'])
    

    def __prepare_all_tables(self):
        """Prepare data for all tables"""
        # get data from excel and modify it to fit into MySQL
        self.__inputDB_modified['dbinfo'] = self.__prepareData_description(
            self.__filename,
            self.__database_author,
            self.__schema_version,
        )
        
        # prepare branch data
        dfs = self.__prepareData_branch(self.__inputDB['branch'], self.__use_new_line_types)
        self.__inputDB_modified['branch'] = dfs['branch']
        self.__inputDB_modified['branchInfoTable'] = dfs['branchInfoTable']
        self.__inputDB_modified['linedata'] = dfs['linedata']
        self.__inputDB_modified['transformerdata'] = dfs['transformerdata']

        # prepare bus data
        dfs = self.__prepareData_nodes(self.__inputDB['bus'])
        self.__inputDB_modified['bus'] = dfs['bus']
        self.__inputDB_modified['busInfoTable'] = dfs['busInfoTable']
        self.__inputDB_modified['busdata'] = dfs['busdata']
        self.__inputDB_modified['loaddata'] = dfs['loaddata']

        # prepare gen data
        dfs = self.__prepareData_gens(self.__inputDB['gens'])
        self.__inputDB_modified['gen'] = dfs['gen']
        self.__inputDB_modified['genInfoTable'] = dfs['genInfoTable']
        self.__inputDB_modified['gendata'] = dfs['gendata']

        # prepare gen cost data
        dfs = self.__prepareData_gencost(self.__inputDB['fuelprices'], self.__inputDB['fuelprices_idProfiles'], self.__inputDB_modified['genInfoTable'])
        self.__inputDB_modified['gencostInfoTable'] = dfs['gencostInfoTable']
        self.__inputDB_modified['pricesInfoTable'] = dfs['pricesInfoTable']
        self.__inputDB_modified['fuelprices'] = dfs['fuelprices']

        # prepare security ref data
        security_refs_df = self.__prepareData_securityRef(self.__inputDB['SecurityRef'])
        self.__inputDB_modified['security_refs'] = security_refs_df
        self.__inputDB_modified['securityref'] = security_refs_df

        # prepare centiv data
        df_cent_flex_potential, df_centFlexPotential = self.__prepareData_centIv(self.__inputDB['CentFlexPotential'])
        self.__inputDB_modified['cent_flex_potential'] = df_cent_flex_potential
        self.__inputDB_modified['centflexpotential'] = df_centFlexPotential

        # prepare distab data
        df_distab, df_distabgencosts = self.__prepareData_distAB(self.__inputDB['DistABGenCosts'])
        self.__inputDB_modified['data_DistAB_gen_costs '] = df_distab
        self.__inputDB_modified['distabgencosts'] = df_distabgencosts

        # prepare distiv data
        dfs = self.__prepareData_distIv(self.__inputDB['DistGens'], self.__inputDB['DistGenCosts'], self.__inputDB['DistRegionData'], self.__inputDB['DistFlexPotential'], self.__config_years)
        self.__inputDB_modified.update(dfs)

        # prepare distprofiles data
        dfs = self.__prepareData_distProfiles(self.__inputDB['DistProfiles'], self.__inputDB_modified['busInfoTable'])
        self.__inputDB_modified['distprofileInfoTable'] = dfs['distprofileInfoTable']
        self.__inputDB_modified['distprofiles'] = dfs['distprofiles']

        # prepare gemel data
        df_projection, df_workforce = self.__prepareData_gemel(self.__inputDB['projections'], self.__inputDB['workforce'])
        self.__inputDB_modified['projections'] = df_projection
        self.__inputDB_modified['workforce'] = df_workforce

        # prepare gen extra data
        self.__inputDB_modified['gen_extra_InfoTable'] = self.__prepareData_gensExtra(self.__inputDB['gens_extra'])

        # prepare gen type data
        df_GenTypeData, df_gentypedata = self.__prepareData_genTypeData(self.__inputDB['gentypedata'])
        self.__inputDB_modified['GenTypeData'] = df_GenTypeData
        self.__inputDB_modified['gentypedata'] = df_gentypedata

        # prepare profiles data
        df_profileInfoTable, df_profiledata = self.__prepareData_profiles(self.__inputDB['profiles'])
        self.__inputDB_modified['profileInfoTable'] = df_profileInfoTable
        self.__inputDB_modified['profiledata'] = df_profiledata

        # prepare swiss annual target data
        dfs = self.__prepareData_swissAnnualTarget(self.__inputDB['SwissAnnualTargets'])
        self.__inputDB_modified['SwissAnnualTargetsInfoTable'] = dfs['SwissAnnualTargetsInfoTable']
        self.__inputDB_modified['swiss_annual_targets_configinfo'] = dfs['swiss_annual_targets_configinfo']
        self.__inputDB_modified['swiss_annual_targets_configuration'] = dfs['swiss_annual_targets_configuration']

        # prepare market data
        dfs = self.__prepareData_market(self.__inputDB['MarketCoupl'])
        self.__inputDB_modified['scenarioInfoTable'] = dfs['scenarioInfoTable']
        self.__inputDB_modified['marketsconfiguration'] = dfs['marketsconfiguration']

        # prepare scenario config data
        self.__inputDB_modified['scenarioconfiguration'] = self.__prepareData_scenarioConfig(self.__inputDB_modified['scenarioInfoTable'])

        logging.debug("Data has been modified to fit into MySQL.")
    
    def __prepare_configuration(self):
        logging.debug("Creating Configurations...")

        # create network config info for each year
        self.__prepareData_networkConfig( self.__config_years_int)

        # create gen config info for each year
        self.__prepareData_genConfig(self.__config_years_int)

        # create load config info for each year
        self.__prepareData_loadConfig(self.__config_years_int)
        
    def __prepare_flex_params(self):
        # prepare flex params for EV
        self.__inputDB_modified['flex_profiles_ev'] = self.__prepareData_flexParamsEV(self.__config_years, self.__demand_profiles_path)

        # prepare flex params for HP
        self.__inputDB_modified['flex_profiles_hp'], self.__inputDB_modified['flex_params_hp'] = self.__prepareData_flexParamsHP(self.__config_years, self.__demand_profiles_path)

        # create load profiles for nodal load
        self.__inputDB_modified['load_profiles'] = self.__prepareData_nodalLoad(self.__config_years, self.__demand_profiles_path)

    # function to combine all
    def prepare_data(self):
        self.__prepare_all_tables()
        self.__prepare_configuration()
        if self.__include_flex_params:
            self.__prepare_flex_params()
        
        return self.__inputDB_modified


class MysqlConnector:
    def __init__(
        self,
        push_to_mysql: bool,
        inputDB: dict, 
        host: str,
        user: str,
        password: str,
        database_name: str,
        dump_file_path: str,
        include_flex_params: bool,
    ):
        self.__push_to_mysql = push_to_mysql
        self.__inputDB = inputDB
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database_name = database_name
        self.__dump_file_path = dump_file_path
        self.__include_flex_params = include_flex_params

    def push_DB_to_mysql(self):
        if self.__push_to_mysql:
            logging.debug("----------------------------------------------------------------")
            logging.debug("Pushing data to MySQL...")
            
            # list of tables to push to MySQL (based on Matlab script)
            list_of_tables_to_push = ['dbinfo','distabgencosts','centflexpotential','projections','workforce','fuelprices','distgendata','distgenconfiginfo','distgenconfiguration',
                                    'distregiondata','distregionbygentypedata','distregionbyirradleveldata','distflexpotential','securityref','busdata','linedata','transformerdata',
                                    'distprofiles','profiledata','gendata','loaddata','marketsconfiguration','swiss_annual_targets_configinfo','swiss_annual_targets_configuration', 
                                    'networkconfiginfo','busconfiguration','lineconfiguration', 'transformerconfiguration', 'loadconfiginfo','loadconfiguration',
                                    'genconfiguration_extra','genconfiginfo', 'genconfiguration', 'scenarioconfiguration']
                                
            
            # check if flex profiles for EV and HP are included
            if self.__include_flex_params:
                list_of_tables_to_push.extend(['load_profiles', 'flex_profiles_ev', 'flex_profiles_hp', 'flex_params_hp'])

            # Check if DataFrames exist for all required keys
            missing_keys = [key for key in list_of_tables_to_push if key not in self.__inputDB]

            if missing_keys:
                logging.debug(f"Missing DataFrames for keys: {missing_keys}")
            else:
                logging.debug("DataFrames exist for all required keys.")
                logging.debug("Pushing data to MySQL...")
                
                # create database
                self.__create_database()

                # create engine
                engine = create_engine(f"mysql+mysqlconnector://{self.__user}:{self.__password}@{self.__host}/{self.__database_name}")

                # connect to MySQL
                conn, _ = self.__connect_to_mysql()

                for table in list_of_tables_to_push:
                    self.__push_table_to_sql(self.__inputDB, table, conn, engine, 'append')

                logging.debug("Finished pushing data to MySQL.")
                logging.debug("----------------------------------------------------------------")
                # close connection
                conn.close()
                logging.debug("Connection to MySQL closed.")
        

    # Check if table exists, drop it if it does, create it if it doesn't
    def __create_database(self):
        # connect to MySQL
        conn, cursor = self.__connect_to_mysql()
        # check if database already exists
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        db_exists = self.__database_name.lower() in databases
        
        # if database exists, drop it
        if db_exists:
            cursor.execute("DROP DATABASE " + self.__database_name)
            logging.debug("Database " + self.__database_name + " already exists and has been dropped.")
        else:
            logging.debug("Database " + self.__database_name + " does not exist yet.")

        # create database
        cursor.execute("CREATE DATABASE " + self.__database_name)
        logging.debug("Database " + self.__database_name + " has been created.")

        # close connection
        cursor.close()
        conn.close()

        # construct command to restore the dump file
        # command = f"mysql -u {conn_info['username']} -p{conn_info['password']} {conn_info['databaseName']} < {conn_info['dumpFile']}"

        command = f"mysql -h {self.__host} -u {self.__user} --password={self.__password} {self.__database_name} < {self.__dump_file_path}"
    
        try:
            # restore the dump file
            process = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Output any messages for debugging
            logging.debug(f"Output: {process.stdout.decode()}")
            logging.debug("Database has been restored successfully.")
        except Exception as e:
            logging.error(f"Database could not be restored: {e}")

        

    # Connection to MySQL
    def __connect_to_mysql(self):
        try:
            # Create a connection
            conn = mysql.connector.connect(
                host = self.__host,
                user = self.__user,
                password = self.__password
            )
            
            if conn.is_connected():
                cursor = conn.cursor()
                logging.debug("Connection to MySQL established")
                return conn, cursor

        except Exception as e:
            logging.error(f"Connection to MySQL could not be established: {e}")
            return None, None


    def __push_table_to_sql(
        self,
        inputDB_modified: pd.DataFrame,
        sheet_name: str,
        conn: mysql.connector.connection.MySQLConnection,
        engine: sqlalchemy.engine.base.Engine,
        if_exists_option: str
    ):
        df = inputDB_modified[sheet_name]
        try:
            df.to_sql(name=sheet_name, con=engine, if_exists=if_exists_option, index=False, chunksize=100)
            logging.debug(f"Table {sheet_name} has been loaded successfully.")

        except Exception as e:
            if conn:
                conn.rollback()  # Rollback if there's an unexpected error
            logging.error(f"Table {sheet_name} could not be loaded: {e}")