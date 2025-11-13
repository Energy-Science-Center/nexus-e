from pathlib import Path
import pytest
import shutil

from src.plugins.postprocess import results_context

class TestResultsContext():

    @pytest.mark.parametrize(("simulated_years", "expected_results"),
        [
            ([], []),
            (["2020"], [2020]),
            (["2020", "2030", "2050"], [2020, 2030, 2050]),
            (["2050", "2020", "2040"], [2020, 2040, 2050]),
        ]
    )
    def test_retrieve_years_simulated_by_centiv(
        self,
        simulated_years: list[str],
        expected_results: list[int]
    ):
        results_path = Path() / "tests" / "temp"

        try:
            # Arrange
            results_path.mkdir(exist_ok=True)
            for year in simulated_years:
                Path(results_path / f"CentIv_{year}").mkdir(exist_ok=True)
            
            # Act
            results = results_context.get_years_simulated_by_centiv(results_path)

            # Assert
            assert results == expected_results

        finally:
            shutil.rmtree(results_path, ignore_errors=True)
