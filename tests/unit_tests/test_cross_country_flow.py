import pytest
import pandas as pd
import numpy as np

from src.plugins.postprocess.legacy.cross_country_flow import CrossCountryFlows, DataframeManager

@pytest.fixture
def setup_cross_country_flow():
    # Create a mock DataframeManager
    dataframe_manager = DataframeManager(output_directory="test_output", filename="cross_country_flows", model="c")
    
    # Create an instance of CrossCountryFlows
    cross_country_flows = CrossCountryFlows(simulated_year=[2020], dataframe_manager=dataframe_manager)
    
    return cross_country_flows, dataframe_manager

class MockBorderFlows:
    def load(self, year):
        pass

    def get_hourly_flow_from_to_country(self, from_country, to_country):
        if from_country == "DE" and to_country == "FR":
            return pd.Series([100, 200, 300]), pd.Series([50, 60, 70])
        elif from_country == "FR" and to_country == "DE":
            return pd.Series([80, 90, 100]), pd.Series([40, 50, 60])
        return pd.Series([0, 0, 0]), pd.Series([0, 0, 0])

def test_load_flows_for_all_years(setup_cross_country_flow):
    cross_country_flows, dataframe_manager = setup_cross_country_flow
    
    # Arrange
    border_flows = MockBorderFlows()
    
    # Act
    cross_country_flows.load_flows_for_all_years(border_flows)
    
    # Assert
    result_df = dataframe_manager.result_dataframes['DE'][2020]
    assert 'Imports (Net)' in result_df.columns
    assert 'Transit_Flow' in result_df.columns
    assert 'FR Exports_Transit' in result_df.columns
    assert 'FR Exports_Non_Transit' in result_df.columns
    assert 'FR Imports_Transit' in result_df.columns
    assert 'FR Imports_Non_Transit' in result_df.columns
