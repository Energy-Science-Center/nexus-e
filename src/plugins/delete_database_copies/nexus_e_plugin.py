
from dataclasses import asdict, dataclass, field
import logging
from typing import Any

from nexus_e_interface.plugin import Plugin
from nexus_e_interface.scenario import Scenario

from .database import MySQLDatabaseContext

@dataclass
class Parameters:
    database_copies: list[dict[str, str]] = field(default_factory=list)
    """
    List of copied databases names to delete alongside their source database.
    """
    delete_all_copies: bool = True
    """Allow to delete all copies given by database_copies."""
    reset_original_data_context_name: bool = True
    """Allow to update data_context name back to the original database name."""

class NexusePlugin(Plugin):
    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Parameters())
    
    def __init__(self, parameters: dict, scenario: Scenario):
        parameters = {
            key: value
            for key, value in parameters.items()
            if key in NexusePlugin.get_default_parameters()
        }
        self.__parameters = Parameters(**parameters)
        self.__data_context = scenario.get_data_context()
        if self.__data_context.type != "mysql":
            raise ValueError("delete_database_copies only works with a MySQL database")

    def run(self) -> dict[str, Any]:
        if len(self.__parameters.database_copies) == 0:
            logging.info("Nothing to do, database_copies is empty.")
            return {}
        output = {
            "database_copies": self.__parameters.database_copies.copy()
        }
        if self.__parameters.delete_all_copies:
            database_server = MySQLDatabaseContext(
                host=self.__data_context.host,
                port=self.__data_context.port,
                username=self.__data_context.user,
                password=self.__data_context.password,
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
        if self.__parameters.reset_original_data_context_name:
            copy_names = [
                copy["copy_name"] for copy in self.__parameters.database_copies
            ]
            original_names = [
                copy["original_name"] for copy in self.__parameters.database_copies
            ]
            first_original_name = set(
                name for name in original_names if name not in copy_names
            )
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
                output["data_context"] = {"name": first_original_name.pop()}
        return output
