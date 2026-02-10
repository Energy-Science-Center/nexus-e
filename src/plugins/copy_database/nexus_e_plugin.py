from dataclasses import asdict, dataclass, field
import logging
from typing import Any

from .database import DatabaseCopyNamer, MySQLDatabaseContext

from nexus_e_interface.plugin import Plugin
from nexus_e_interface.scenario import Scenario

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
    input_data_name: str = ""
    """Name of the database to be copied."""
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
    the input_data_name parameter.
    """

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
        output = {}
        if self.__parameters.forced_copy_name:
            database_copy_name = self.__parameters.forced_copy_name
        else:
            name_generator = DatabaseCopyNamer(
                self.__parameters.user_initials
            )
            database_copy_name = name_generator.create_copy_name(
                self.__parameters.input_data_name
            )
        mysql_server = MySQLDatabaseContext(
            host=self.__parameters.input_data_host,
            port=self.__parameters.input_data_port,
            username=self.__parameters.input_data_user,
            password=self.__parameters.input_data_password,
        )
        logging.info(f"Copy database: {self.__parameters.input_data_name}")
        mysql_server.copy_database(
            self.__parameters.input_data_name, database_copy_name
        )
        logging.info(f"New database created: {database_copy_name}")
        output["input_data_name"] = database_copy_name
        output["database_copies"] = (
            self.__parameters.database_copies.copy()
        )
        output["database_copies"].append(
            {
                "copy_name": database_copy_name,
                "original_name": self.__parameters.input_data_name
            }
        )
        return output
        