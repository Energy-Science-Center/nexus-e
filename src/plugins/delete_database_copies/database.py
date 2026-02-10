"""Handle Nexus-e MySQL databases."""

import subprocess

class MySQLDatabaseContext:
    def __init__(self, host: str, port: str, username: str, password: str):
        """Used to connect to a MySQL server and delete databases there.

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

    def drop_database(self, database_name: str):
        """Drop a database on the MySQL server."""
        drop_database_command = "mysql"
        drop_database_command += f" --host {self.host}"
        drop_database_command += f" --port {self.port}"
        drop_database_command += f" --user {self.username}"
        drop_database_command += f" --password={self.password}"
        drop_database_command += f' --execute="drop database {database_name}"'
        subprocess.run(drop_database_command, shell=True)