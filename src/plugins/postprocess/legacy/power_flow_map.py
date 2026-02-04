from dataclasses import dataclass
import os
from pathlib import Path
import pandas as pd

from .cross_country_flow import BorderFlows, LoginCredentials
from ..results_context import get_years_simulated_by_centiv

@dataclass
class Border:
    name: str
    from_country: str
    to_country: str


class PowerFlows:
    def __init__(self, year):
        self.__year = year
        self.__country_borders = [
            Border(name="CH-FR", from_country="CH", to_country="FR"),
            Border(name="CH-IT", from_country="CH", to_country="IT"),
            Border(name="FR-IT", from_country="FR", to_country="IT"),
            Border(name="CH-AT", from_country="CH", to_country="AT"),
            Border(name="AT-IT", from_country="AT", to_country="IT"),
            Border(name="AT-DE", from_country="AT", to_country="DE"),
            Border(name="CH-DE", from_country="CH", to_country="DE"),
            Border(name="FR-DE", from_country="FR", to_country="DE"),
        ]

    def collect_power_flows_for_all_borders(self, border_flows: BorderFlows):
        # Exports: from country -> to country
        # Imports: from country <- to country
        self.__border_flows = pd.DataFrame(index=["From", "To", "Exports", "Imports"])

        border_flows.load(self.__year)

        for border in self.__country_borders:
            exports, imports = border_flows.get_annual_flow_from_to_country(
                border.from_country, border.to_country
            )
            self.__border_flows[border.name] = [
                border.from_country,
                border.to_country,
                exports,
                imports,
            ]

    def export_border_flow_file(self, output_directory: str):
        self.__border_flows.to_csv(
            os.path.join(output_directory, f"border_flows_annual_{self.__year}.csv")
        )


def main(database: str, host: str, user: str, password: str):
    simulated_years = get_years_simulated_by_centiv(Path())

    for year in simulated_years:
        border_flows = BorderFlows(
            database=database,
            database_credentials=LoginCredentials(
                host=host,
                user=user,
                password=password
            )
        )
        power_flows = PowerFlows(year)
        power_flows.collect_power_flows_for_all_borders(border_flows=border_flows)
        output_directory = os.path.join(
            "postprocess",
            "national_generation_and_capacity"
        )
        power_flows.export_border_flow_file(output_directory)