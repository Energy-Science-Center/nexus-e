import os
import pandas as pd
import pyomo.environ as pyo
import logging


class SaveResults:
    def __init__(self, results_folder: str):
        self.__results_folder = results_folder
    def saveLoadGenerationTimeseriesNames_Excel(self, ts, filename, sheet_name, row1, row2):
        df = pd.DataFrame(ts)
        df_row1 = pd.DataFrame(row1)
        df_row2 = pd.DataFrame(row2)
        df_concat = pd.concat([df_row1,df_row2,df])
        writer = pd.ExcelWriter(
            os.path.join(
                self.__results_folder,
                filename
            ),
            engine='xlsxwriter',
            #use zip64 writer method for large excel files
            engine_kwargs={'options': {'use_zip64': True}}
        )
        df_concat.to_excel(writer, sheet_name=sheet_name)
        writer.close()
    
    def saveLoadGenerationTimeseriesNamesPlus_Excel(self, ts, filename, sheet_name, row1, row2, row3):
        df = pd.DataFrame(ts)
        df_row1 = pd.DataFrame(row1)
        df_row2 = pd.DataFrame(row2)
        df_row3 = pd.DataFrame(row3)
        df_concat = pd.concat([df_row1,df_row2,df_row3,df])
        writer = pd.ExcelWriter(
            os.path.join(
                self.__results_folder,
                filename
            ),
            engine='xlsxwriter',
            #use zip64 writer method for large excel files
            engine_kwargs={'options': {'use_zip64': True}}
        )
        df_concat.to_excel(writer, sheet_name=sheet_name)
        writer.close()
        
    def saveLoadGenerationTimeseries_Excel(self, ts, filename, sheet_name):
        df = pd.DataFrame(ts)
        writer = pd.ExcelWriter(
            os.path.join(
                self.__results_folder,
                filename
            ),
            engine='xlsxwriter',
            #use zip64 writer method for large excel files
            engine_kwargs={'options': {'use_zip64': True}}
        )
        df.to_excel(writer, sheet_name=sheet_name)
        writer.close()

    def saveLoadGenerationTimeseriesNames_CSV(self, ts, filename, row1, row2):
        df = pd.DataFrame(ts)
        df_row1 = pd.DataFrame(row1)
        df_row2 = pd.DataFrame(row2)
        df_concat = pd.concat([df_row1,df_row2,df])
        df_concat.to_csv(
            os.path.join(
                self.__results_folder,
                filename
            ),
            header=True
        )
            
    def saveLoadGenerationTimeseries_CSV(self, ts, filename):
        df = pd.DataFrame(ts)
        df.to_csv(
            os.path.join(
                self.__results_folder,
                filename
            ),
            header=True
        )  
        
    def savePriceTimeseries(self, ts, filename, column = None, sheet_name = None):
        if column and sheet_name:
            df = pd.DataFrame(ts, columns = [column])
            writer = pd.ExcelWriter(
                os.path.join(
                    self.__results_folder,
                    filename
                ),
                engine='xlsxwriter'
            )
            df.to_excel(writer, sheet_name=sheet_name)
            writer.close()
        else:
            df = pd.DataFrame(ts)
            writer = pd.ExcelWriter(
                os.path.join(
                    self.__results_folder,
                    filename
                ),
                engine='xlsxwriter'
            )
            df.to_excel(writer, sheet_name='CHF_per_MWh')
            writer.close()    
            
    def saveScalars_Excel(self, ts, filename, column, sheet_name):
        df = pd.DataFrame(ts, columns = [column])
        writer = pd.ExcelWriter(
            os.path.join(
                self.__results_folder,
                filename
            ),
            engine='xlsxwriter'
        )
        df.to_excel(writer, sheet_name=sheet_name)
        writer.close()  
    
    def saveExportsImports_Excel(self, ts, filename, column, sheet_name):
        df = pd.DataFrame(ts, columns=column)
        writer = pd.ExcelWriter(
            os.path.join(
                self.__results_folder,
                filename
            ),
            engine='xlsxwriter'
        )
        df.to_excel(writer, sheet_name=sheet_name)
        writer.close()  
        
    def saveExportsImportsMultiple_Excel(self, df_list, sheet_list, file_name):
        writer = pd.ExcelWriter(
            os.path.join(
                self.__results_folder,
                file_name
            ),
            engine='xlsxwriter'
        )
        for dataframe, sheet in zip(df_list, sheet_list):
            dataframe.to_excel(writer, sheet_name=sheet, startrow=0 , startcol=0)   
        writer.close()

def saveVarParDualsCsv(model: pyo.ConcreteModel, results_folder: str):
    """
    Save all variables and parameters, and duals of selected constraints of a Pyomo model to CSV files.
    Parameters
    ----------
    model : pyo.ConcreteModel
        The Pyomo model containing variables and parameters to be saved.
    results_folder : str
        The folder where the CSV files will be saved.
    """
    # Parameters ----------------------------------------------------
    constraint_names_to_export = [  # List of constraints to export duals for
        "NodalConstraint", 
        "NodalConstraint_one_CH",
        "SoCHydroCon1",     # hydro *
        "SoCHydroDamCon",   # hydro *
        "SoCBattCandCon",   # battery ?
        "SoCCon",           # battery x
        "SoCBattCon",       # battery ?
        "SoCDSMCon",        # DSM x
        "SoCHydrogenCandCon",  # hydrogen x
        "SoCH2CandCon",        # hydrogen x
        "SoCHydroDayCon1",  # hydro *

        # "storage_soc_limit",
        # "lineATClimit",
    ]   

    

    # Main code --------------------------------------------------------
    # create results folder if it does not exist
    if not os.path.exists(results_folder):
        os.makedirs(results_folder)
    # Extract variables---------------
    # create a list of all variables in the model
    var_list = [
        v
        for v in model.component_objects(
            ctype=pyo.Var, active=True, descend_into=True
        )
    ]

    # Extract Parameters---------------
    # create a list of all parameters in the model
    par_list = [
        v
        for v in model.component_objects(
            ctype=pyo.Param, active=True, descend_into=True
        )
    ]
    
    # merge var_list and par_list
    var_par_list = var_list + par_list
    
    # Export variables to CSV files ---------
    for variable in var_par_list:
        # extract data
        extracted_info = variable.extract_values()

        # if not empty, export file with data
        if extracted_info != {}:
            # convert to DataFrame
            result_values = pd.DataFrame(
                index=extracted_info.keys(), data=extracted_info.values()
            )

            result_values.columns = ["value"] + result_values.columns.tolist()[1:]

            # extract names of the domain sets
            if variable.index_set()._implicit_subsets is None:
                header_list = [
                    variable.index_set().name,
                ]
            else:
                header_list = [
                    variable.__dict__["_implicit_subsets"][domain_counter].name
                    for domain_counter in range(
                        len(variable.__dict__["_implicit_subsets"])
                    )
                ]

            # Convert DataFrame to include the set names as the first row
            result_values.index.names = header_list

            result_values.to_csv(
                os.path.join(results_folder, f"{variable.name}.csv")
            )

        # export empty file
        else:
            result_values = pd.DataFrame()
            result_values.to_csv(
                os.path.join(results_folder, f"{variable.name}.csv")
            )

    # Export duals of selected constraints to CSV files -----------
    # Keep only the constraints that are really active
    constraint_list = [ 
        v
        for v in model.component_objects(
            ctype=pyo.Constraint, active=True, descend_into=True
        )
        if v.name in constraint_names_to_export
    ]
    result_duals_dict = {}
    
    for constraint in constraint_list:
        # export dual values in an ScalarConstraint object
        data = {}
        counter = 0
        # logging.debug(constraint)
        try: 
            for index in constraint:
                if constraint.dim() == 0:
                    data[counter] = [constraint.name] + [
                        model.dual[constraint[index]]
                    ]  # NOTE: there will be so may constraints for limited energy plants, because every duration has its own constraint
                elif constraint.dim() != 1:
                    data[counter] = [i for i in index] + [model.dual[constraint[index]]]
                else:
                    data[counter] = [index] + [model.dual[constraint[index]]]
                counter = counter + 1
                # data
                # result_duals.loc[len(result_duals)] = [[i for i in index], model.dual[constraint[index]]]
            result_duals = pd.DataFrame.from_dict(data, orient="index")

            # extract names of the domain sets
            if constraint._implicit_subsets is None:
                header_list = [
                    constraint.name,
                ]
            else:
                header_list = [
                    constraint.__dict__["_implicit_subsets"][domain_counter].name
                    for domain_counter in range(
                        len(constraint.__dict__["_implicit_subsets"])
                    )
                ]

            # Convert DataFrame to include the set names as the first row
            result_duals.columns = header_list + ["value"]
            result_duals.to_csv(
                os.path.join(results_folder, f"{constraint.name}_dual.csv"),
                index=False,
            )

            result_duals_dict[constraint.name] = result_duals
        except:
            logging.debug(f"Error in exporting duals for {constraint.name}")

def saveMappingFiles(data_dict, results_folder: str):
    """
    Create mapping files for all model elements to help with post-processing analysis.
    
    Args:
        data_dict: Dictionary containing all data structures to create mappings for
        results_folder: Base folder where mappings subfolder will be created
    """
    
    # Create mappings subfolder
    mappings_folder = os.path.join(results_folder, "mappings")
    os.makedirs(mappings_folder, exist_ok=True)
    
    # Define key attributes for each element type
    key_attributes = {
        'generators': ['Country', 'Technology', 'BusName', 'GenType', 'UnitType', 'GenName', 'FuelType', 'SubRegion'],
        'buses': ['Country', 'BusName', 'SubRegion'],
        'lines': ['FromCountry', 'ToCountry', 'LineName', 'FromBusName', 'ToBusName'],
        'transformers': ['FromCountry', 'ToCountry', 'TrafoName', 'FromBusName', 'ToBusName'],
        'loads_busnodes': ['Country', 'BusName', 'LoadType'],
        'emobilityloads_busnodes': ['Country', 'BusName', 'LoadType'],
        'heatpumploads_busnodes': ['Country', 'BusName', 'LoadType'],
        'H2loads_busnodes': ['Country', 'BusName', 'LoadType'],
        'gens_busnodes': ['Country', 'BusName', 'Technology', 'GenName'],
        'distivinj_busnodes': ['Country', 'BusName']
    }
    
    logging.debug("Creating mapping files...")
    
    for element_name, element_data in data_dict.items():
        if element_name in key_attributes and element_data:
            logging.debug(f"  Processing {element_name}...")
            _createElementMappings(element_data, element_name, key_attributes[element_name], mappings_folder)
    
    logging.debug(f"Mapping files created in: {mappings_folder}")


def _createElementMappings(element_data, element_name, key_attributes, mappings_folder):
    """
    Create mapping files for a specific element type.
    
    Args:
        element_data: List of dictionaries containing element data
        element_name: Name of the element type (e.g., 'generators', 'buses')
        key_attributes: List of attribute names to create mappings for
        mappings_folder: Folder to save mapping files
    """
    
    # Create DataFrame from element data
    df = pd.DataFrame(element_data)
    # if some of the values look like integer but are float (like 140.0), turn them to integer
    df = df.applymap(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x)
    
    if df.empty:
        logging.debug(f"    Warning: No data for {element_name}")
        return
    
    # Save complete data
    complete_file = os.path.join(mappings_folder, f"Data_{element_name}.csv")
    df.to_csv(complete_file, index=True)
    logging.debug(f"    Saved complete data: Data_{element_name}.csv ({len(df)} rows)")
    
    # Determine the ID column (usually the index or first meaningful ID column)
    id_col = 'ID'
    df_with_id = df.reset_index().rename(columns={'index': id_col})
    
    # Create forward and reverse mappings for each key attribute
    available_attrs = []
    for attr in key_attributes:
        if attr in df_with_id.columns:
            available_attrs.append(attr)
            # Forward mapping: ID -> Attribute
            forward_mapping = df_with_id[[id_col, attr]].dropna()
            if not forward_mapping.empty:
                forward_file = os.path.join(mappings_folder, f"Map_{element_name}_{attr.lower()}.csv")
                forward_mapping.to_csv(forward_file, index=False)
                
                # Reverse mapping: Attribute -> IDs
                reverse_mapping = _createReverseMapping(forward_mapping, id_col, attr)
                reverse_file = os.path.join(mappings_folder, f"Map_{attr.lower()}_{element_name}.csv")
                reverse_mapping.to_csv(reverse_file, index=False)
                
                logging.debug(f"    Created mappings for {attr}: Map_{element_name}_{attr.lower()}.csv and Map_{attr.lower()}_{element_name}.csv")
            else:
                logging.debug(f"    Warning: No valid data for attribute {attr} in {element_name}")
        else:
            logging.debug(f"    Warning: Attribute {attr} not found in {element_name}")
    
    if available_attrs:
        logging.debug(f"    Available attributes in {element_name}: {available_attrs}")
    else:
        logging.debug(f"    Warning: No requested attributes found in {element_name}")
        logging.debug(f"    Actual columns in {element_name}: {list(df.columns)[:10]}...")  # Show first 10 columns

def _createReverseMapping(mapping_df, id_col, value_col):
    """
    Create reverse mapping from attribute values to IDs.
    Each attribute value gets one row with multiple columns for each ID.
    
    Args:
        mapping_df: DataFrame with ID and value columns
        id_col: Name of the ID column
        value_col: Name of the value column
    
    Returns:
        DataFrame with one row per attribute value and separate columns for each ID
    """
    
    # Group by value and get list of IDs for each
    grouped = mapping_df.groupby(value_col)[id_col].apply(list).reset_index()
    
    # Find the maximum number of IDs for any attribute value
    max_ids = grouped[id_col].apply(len).max()
    
    # Create result DataFrame
    result_data = []
    for _, row in grouped.iterrows():
        attribute_value = row[value_col]
        ids = row[id_col]
        
        # Create a row with the attribute value and all its IDs in separate columns
        row_data = {value_col: attribute_value}
        for i, id_val in enumerate(ids):
            row_data[f"{id_col}_{i+1}"] = id_val
        
        # Fill remaining columns with empty values if this attribute has fewer IDs
        for i in range(len(ids), max_ids):
            row_data[f"{id_col}_{i+1}"] = ""
        
        result_data.append(row_data)
    
    result_df = pd.DataFrame(result_data)
    
    return result_df

def saveSelectedStats(opt, duration_log_dict: dict, results_folder: str):
    """
    Save selected statistics of the model, plus the duration log.
    
    Parameters
    ----------
    system_state : SystemState
        The SystemState instance containing the model and solver results.
    duration_log_dict : dict
        Dictionary containing the duration log of the model.
    results_folder : str
        The folder where the CSV files will be saved.
    """
    model = opt.model
    results = opt.results

    selected_stats = {
        "Termination_Condition": str(results.solver.termination_condition) if results else None,
        "Solver_Status": str(results.solver.status) if results else None,
        "Objective_Value": pyo.value(model.obj) if hasattr(model, 'obj') else None,
        "solve_time": getattr(results.solver, 'time', None) if results else None,
        "NO_Variables": len([v for v in model.component_data_objects(pyo.Var)]),
        "NO_Constraints": len([c for c in model.component_data_objects(pyo.Constraint)]),
    }

    # Merge the duration log with the selected statistics
    selected_stats.update(duration_log_dict)

    # Convert the statistics to a DataFrame
    stats_df = pd.DataFrame.from_dict(selected_stats, orient='index', columns=['Value'])

    # Save the DataFrame to a CSV file
    stats_df.to_csv(
        os.path.join(results_folder, "selected_stats.csv"),
        header=True
    )