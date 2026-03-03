from dataclasses import asdict, dataclass, field
import logging
from typing import Any

from .database import DatabaseCopyNamer, MySQLDatabaseContext

from nexus_e_interface.plugin import Plugin
from nexus_e_interface.scenario import Scenario

@dataclass
class Parameters:
    database_copies: list[dict[str, str]] = field(default_factory=list)
    """List of copied and original databases names to append the new copy to."""
    forced_copy_name: str = ""
    """Force the name given to the database copy."""
    user_initials: str = ""
    """
    User initials are saved in metadata (legacy behavior). Currently the only
    way we use to track databases authorship.
    """

class NexusePlugin(Plugin):
    """
    This plugin creates a copy of the MySQL database whose name is given by
    the data_context "name" parameter.
    """

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
            raise ValueError("copy_database only works with a MySQL database")

    def run(self) -> dict[str, Any]:
        output = {}
        if self.__parameters.forced_copy_name:
            database_copy_name = self.__parameters.forced_copy_name
        else:
            name_generator = DatabaseCopyNamer(
                self.__parameters.user_initials
            )
            database_copy_name = name_generator.create_copy_name(
                self.__data_context.name
            )
        mysql_server = MySQLDatabaseContext(
            host=self.__data_context.host,
            port=self.__data_context.port,
            username=self.__data_context.user,
            password=self.__data_context.password,
        )
        logging.info(f"Copy database: {self.__data_context.name}")
        mysql_server.copy_database(
            self.__data_context.name, database_copy_name
        )
        logging.info(f"New database created: {database_copy_name}")
        output["data_context"] = {"name": database_copy_name}
        output["database_copies"] = (
            self.__parameters.database_copies.copy()
        )
        output["database_copies"].append(
            {
                "copy_name": database_copy_name,
                "original_name": self.__data_context.name
            }
        )
        return output
        