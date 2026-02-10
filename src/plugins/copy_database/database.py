"""Handle Nexus-e MySQL databases.

This module can create a copy of a scenario database such that the simulation
can work with the copy and leave the original scenario untouched.
"""

from datetime import datetime
import random
import string
import subprocess
from abc import ABC, abstractmethod


class DatabaseContext(ABC):
    @abstractmethod
    def __init__(self, host: str, port: str, username: str, password: str): ...

    @abstractmethod
    def copy_database(self, original_name: str, copy_name: str): ...


MYSQL_DATABASE_NAME_MAX_LENGTH = 64
"""MySQL maximum length for database names."""
RANDOM_SIGNATURE_LENGTH = 4
"""Use to suffix a database name with a random unique string."""
USER_INITIALS_MAX_LENGTH = 4


class MySQLDatabaseContext:
    def __init__(self, host: str, port: str, username: str, password: str):
        """A MySQLDatabaseCopier is constructed with a database server
        connection's credentials.

        Args:
            host: server's URL
            port: server's port to MySQL server
            username: user of MySQL server
            password: user's password

        TODO:
            - We shouldn't use user's credentials directly in the command line
            (Maybe it's then kept in the commands' history on the MySQL
            server?). The good practice seems to be using a .cnf file.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def copy_database(self, original_name: str, copy_name: str):
        """ "Duplicate a database on the MySQL server.

        TODO:
            - Check that the original database exists.
        """
        create_database_command = "mysql"
        create_database_command += f" --host {self.host}"
        create_database_command += f" --port {self.port}"
        create_database_command += f" --user {self.username}"
        create_database_command += f" --password={self.password}"
        create_database_command += f' --execute="create database {copy_name}"'
        subprocess.run(create_database_command, shell=True)
        copy_database_command = "mysqldump"
        copy_database_command += " --routines=true"
        copy_database_command += f" --host {self.host}"
        copy_database_command += f" --port {self.port}"
        copy_database_command += f" --user {self.username}"
        copy_database_command += f" --password={self.password}"
        copy_database_command += f" {original_name}"
        copy_database_command += " | mysql"
        copy_database_command += f" --host {self.host}"
        copy_database_command += f" --port {self.port}"
        copy_database_command += f" --user {self.username}"
        copy_database_command += f" --password={self.password}"
        copy_database_command += f" {copy_name}"
        subprocess.run(copy_database_command, shell=True)


class DatabaseCopyNamer:
    """Generate databases names."""

    def __init__(self, user_initials: str):
        """Use the user's initials to mark the database names."""
        self.user_initials = user_initials[:USER_INITIALS_MAX_LENGTH]

    def create_copy_name(self, original_name: str) -> str:
        """The name of the database copy is the original name with an added
        timestamp, the user's initials and a randomly generated signature.

        The name of the copy is assured to be short enough not to exceed the
        maximum length of a MySQL database name.
        """
        copy_suffix = f"_{datetime.now().strftime('%y%m%dT%H%M')}"
        copy_suffix += f"_{self.user_initials}"
        copy_suffix += f"_{self.__randomstring(RANDOM_SIGNATURE_LENGTH)}"
        copy_name = (
            original_name[: MYSQL_DATABASE_NAME_MAX_LENGTH - len(copy_suffix)]
            + copy_suffix
        )
        return copy_name

    def __randomstring(self, length: int) -> str:
        characters = string.ascii_lowercase + "1234567890"
        return "".join(random.choice(characters) for i in range(length))
