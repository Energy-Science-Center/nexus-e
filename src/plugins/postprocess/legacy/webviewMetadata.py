#####################
# goal: we retrieve and write the following information into a csv file: 
#   - simulation_submission_time (Temporarily removed because this
#       info is unknown if Nexus-e is executed locally)
#   - reference_year (placeholder)
#   - simulated_years
#   - scenario_name
#   - scenario_short_name
# Some fields are just placeholders, where we need to fill it up manually.
#####################

import os
import pandas as pd
import pathlib

def main(
        simulation_name : str = "",
        scenario_name : str = "",
        scenario_description : str = "",
        simulated_years : str = ""
):
    this_file_folder = pathlib.Path(__file__).parent.resolve()
    nexus_e_framework_root_folder = f"{this_file_folder}/../.."

    # If no simulation years are given, they are retrieved from the results
    # folders names.
    if not simulated_years:
        yearly_results_path = os.path.join(
            nexus_e_framework_root_folder, "Results", simulation_name
        )
        yearly_results = [
            element 
            for element in os.listdir(yearly_results_path)
            if os.path.isdir(os.path.join(yearly_results_path, element))
        ]
        simulated_years = [result_folder.strip("/").split("_")[-1] 
            for result_folder in yearly_results
        ]
    elif isinstance(simulated_years, str):
        # The input simulated_years from MATLAB looks like 
        # "[2020 2030 2040 2050]". In order to interprete it as a list
        # in Python, we first need to remove the brackets and split by spaces.
        simulated_years = simulated_years.replace(
            "[", ""
        ).replace(
            "]", ""
        ).split()

    if not scenario_name:
        scenario_name = simulation_name
    if not scenario_description:
        scenario_description = simulation_name

    result = {
        # "simulation_submission_time": submission_time,
        "reference_year": "2015", # could be left empty
        "simulated_years": ",".join(simulated_years),
        "scenario_description": scenario_description,
        "scenario_short_name": scenario_name
    }

    simulation_postprocess_folder = os.path.join(
        nexus_e_framework_root_folder,
        "Results",
        simulation_name,
        "postprocess",
    )
    if not os.path.exists(simulation_postprocess_folder):
        os.makedirs(simulation_postprocess_folder, exist_ok=True)
    pd.DataFrame.from_dict(
        data=result,
        orient='index'
    ).to_csv(
        os.path.join(simulation_postprocess_folder, "metadata.csv"),
        index_label="name",
        header=["info"]
    )