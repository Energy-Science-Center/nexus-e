
from dataclasses import asdict, dataclass, field
import logging
from typing import Any

from nexus_e_interface.plugin import Plugin
from nexus_e_interface.scenario import Scenario

from .database import MySQLDatabaseContext

@dataclass
class Parameters:
    input_data_host: str = ""
    """MySQL server hostname."""
    input_data_port: str = "3306"
    """MySQL server port."""
    input_data_user: str = ""
    """MySQL server username."""
    input_data_password: str = ""
    """MySQL server password."""
    database_copies: list[dict[str, str]] = field(default_factory=list)
    """
    List of copied databases names to delete alongside their source database.
    """
    delete_all_copies: bool = True
    """Allow to delete all copies given by database_copies."""
    reset_original_input_data_name: bool = True
    """Allow to update input_data_name back to the original database name."""

class NexusePlugin(Plugin):
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Parameters())
    
    def __init__(self, parameters: dict, scenario: Scenario | None = None):
        parameters = {
            key: value
            for key, value in parameters.items()
            if key in NexusePlugin.get_default_parameters()
        }
        self.__parameters = Parameters(**parameters)

    def run(self) -> dict[str, Any]:
        if len(self.__parameters.database_copies) == 0:
            logging.info("Nothing to do, database_copies is empty.")
            return {}
        output = {
            "database_copies": self.__parameters.database_copies.copy()
        }
        if self.__parameters.delete_all_copies:
            database_server = MySQLDatabaseContext(
                host=self.__parameters.input_data_host,
                port=self.__parameters.input_data_port,
                username=self.__parameters.input_data_user,
                password=self.__parameters.input_data_password,
            )
            for database_copy in self.__parameters.database_copies:
                logging.info(
                    f"Deleting database: {database_copy['copy_name']}..."
                )
                database_server.drop_database(
                    database_name=database_copy["copy_name"]
                )
                output["database_copies"].remove(database_copy)
                logging.info("DONE")
        if self.__parameters.reset_original_input_data_name:
            copy_names = [
                copy["copy_name"] for copy in self.__parameters.database_copies
            ]
            original_names = [
                copy["original_name"] for copy in self.__parameters.database_copies
            ]
            first_original_name = [
                name for name in original_names if name not in copy_names
            ]
            if len(first_original_name) == 0:
                logging.warning(
                    (
                        "Cannot determine original database name, "
                        "either that name is missing or all copies are "
                        "referencing each other"
                    )
                )
            elif len(first_original_name) > 1:
                logging.warning(
                    (
                        "Cannot determine original database name, "
                        "several first original names detected: "
                        f"{first_original_name}"
                    )
                )
            else:
                output["input_data_name"] = first_original_name[0]
        return output
