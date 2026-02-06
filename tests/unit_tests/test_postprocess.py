import json
import os
import pandas as pd
import pytest
import shutil

import src.plugins.postprocess.nexus_e_plugin as postprocess

TMP_FOLDER: str = os.path.join("tests", "tmp")

class TestSimulationResults():

    @pytest.mark.parametrize(("results_path"),
        [
            os.path.join(TMP_FOLDER, "any_results", "any_simulation"),
            os.path.join(TMP_FOLDER, "another_results", "another_simulation")
        ]
    )
    def test_get_simulation_postprocess_folder_path(
        self,
        results_path: str,
    ):
        # Arrange
        sut = postprocess.SimulationResults(
            results_path = results_path,
        )
        expected_postprocess_folder = os.path.join(
            results_path,
            "postprocess"
        )

        # Act
        results = sut.postprocess_path

        # Assert
        assert (
            os.path.realpath(results) 
            == os.path.realpath(expected_postprocess_folder)
        )


    @pytest.mark.parametrize(("results_path"),
        [
            os.path.join(TMP_FOLDER, "any_results", "any_simulation"),
            os.path.join(TMP_FOLDER, "another_results", "another_simulation")
        ]
    )
    def test_create_postprocess_folder_for_simulation(
        self,
        results_path: str,
    ):
        # Arrange
        sut = postprocess.SimulationResults(
            results_path = results_path,
        )
        expected_simulation_folder = sut.path
        expected_postprocess_folder = sut.postprocess_path
        if (
            os.path.exists(expected_simulation_folder) 
            and os.path.isdir(expected_simulation_folder)
        ):
            shutil.rmtree(expected_simulation_folder)

        try:
            # Act
            sut.create_postprocess_folder()

            # Assert
            assert os.path.exists(expected_postprocess_folder)
        finally:
            shutil.rmtree(TMP_FOLDER)


    @pytest.mark.parametrize("results_file_name",
        [
            "any_results.csv",
            "another_results.csv",
        ]
    )
    def test_write_a_file_in_postprocess_folder(self, results_file_name: str):
        # Arrange
        results_path = TMP_FOLDER
        sut = postprocess.SimulationResults(
            results_path=results_path,
        )
        results_file_path = os.path.join(
            sut.postprocess_path,
            results_file_name
        )
        if os.path.exists(results_file_path):
            os.remove(results_file_path)
        any_results = pd.DataFrame({"any_column": ["any_value"]})

        try:
            # Act
            sut.write_csv_in_postprocess(any_results, results_file_name)

            # Assert
            assert os.path.exists(results_file_path)
        finally:
            shutil.rmtree(TMP_FOLDER)


    @pytest.mark.parametrize("results",
        [
            pd.DataFrame(
                {
                    "A": [1, 2, 3], "B": [4, 5, 6]
                }
            ),
            pd.DataFrame(
                {
                    "A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]
                }
            )
        ]
    )
    def test_retrieve_data_from_saved_postprocess_results(
        self,
        results: pd.DataFrame
    ):
        # Arrange
        results_path = TMP_FOLDER
        sut = postprocess.SimulationResults(
            results_path=results_path,
        )
        results_file_name = "any_results.csv"
        path_to_csv_file = os.path.join(
            sut.postprocess_path,
            results_file_name
        )

        try:
            # Act
            sut.write_csv_in_postprocess(results, results_file_name)
            expected_result = pd.read_csv(path_to_csv_file, index_col=0)

            # Assert
            pd.testing.assert_frame_equal(results, expected_result)
        finally:
            shutil.rmtree(TMP_FOLDER)


    @pytest.mark.parametrize(("modules_folders", "expected_simulated_years"),
        [
            (
                ["any_module_2020", "another_module_2030"],
                [2020, 2030]
            ),
            (
                ["any_module_2020", "any_module_2030", "any_module_2040"],
                [2020, 2030, 2040]
            ),
        ]
    )
    def test_retrieve_simulated_years_from_results_folders(
        self,
        modules_folders: list[str],
        expected_simulated_years: list[int]
    ):
        try:
            # Arrange
            results_path = TMP_FOLDER
            sut = postprocess.SimulationResults(
                results_path=results_path,
            )
            sut.create_postprocess_folder()
            modules_folders_path = [
                os.path.join(results_path, folder)
                for folder in modules_folders
            ]
            for folder_path in modules_folders_path:
                os.makedirs(folder_path, exist_ok=True)

            # Act
            result = sut.get_simulated_years()

            # Assert
            assert result == expected_simulated_years

        finally:
            shutil.rmtree(TMP_FOLDER)


    def test_retrieve_empty_list_of_years_when_no_results_folders(self):
        # Arrange
        results_path = TMP_FOLDER
        sut = postprocess.SimulationResults(
            results_path=results_path,
        )

        # Act
        result = sut.get_simulated_years()

        # Assert
        assert result == []


    @pytest.mark.parametrize(
        (
            "simulation_name",
            "simulated_years",
            "scenario_description",
        ),
        [
            (
                "any_simulation",
                [2020, 2030],
                "any_description",
            ),
            (
                "another_simulation", 
                [2020, 2030, 2040], 
                "another_description",
            ),
        ]
    )
    def test_retrieve_metadata_from_simulation_results(
        self,
        simulation_name,
        simulated_years,
        scenario_description,
    ):
        try:
            # Arrange
            results_path = os.path.join(TMP_FOLDER, "any_results")
            for year in simulated_years:
                os.makedirs(
                    os.path.join(
                        results_path,
                        simulation_name,
                        f"module_{year}")
                )
            sut = postprocess.SimulationResults(
                results_path=os.path.join(results_path, simulation_name)
            )
            formatted_simulated_years = ",".join(map(str, simulated_years))
            expected_results = pd.DataFrame(
                {
                    "info": [
                        "2015",
                        formatted_simulated_years,
                        scenario_description,
                        ""
                    ]
                },
                index=[
                    "reference_year",
                    "simulated_years",
                    "scenario_description",
                    "scenario_short_name"
                ]
            )
            expected_results.index.name = "name"

            # Act
            result = sut.get_metadata(scenario_description)

            # Assert
            pd.testing.assert_frame_equal(result, expected_results)

        finally:
            shutil.rmtree(TMP_FOLDER)

    @pytest.mark.parametrize(("plot_config_test_file"),
        [
            "any_plot_config.json",
            "another_plot_config.json",
        ]
    )
    def test_copy_plot_config_in_simulation_postprocess_folder(
        self,
        plot_config_test_file
    ):
        try:
            # Arrange
            results_folder = TMP_FOLDER
            if os.path.exists(plot_config_test_file):
                os.remove(plot_config_test_file)
            any_config = {
                "any_key": "any_value",
                "another_key": "another_value"
            }
            with open(plot_config_test_file, "w") as file:
                json.dump(any_config, file)
            sut = postprocess.SimulationResults(
                results_path=results_folder,
            )

            # Act
            sut.copy_plot_config(plot_config_test_file)

            # Assert
            assert os.path.exists(
                os.path.join(
                    results_folder,
                    "postprocess",
                    plot_config_test_file
                )
            )

        finally:
            os.remove(plot_config_test_file)
            shutil.rmtree(TMP_FOLDER)