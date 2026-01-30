import os
import pandas as pd
from sqlalchemy import create_engine, DDL
import glob
import base64
import json
import re
import argparse
from datetime import datetime
import logging

from nexus_e import config
from nexus_e.database import MYSQL_DATABASE_NAME_MAX_LENGTH

def main(
    simulation_postprocess_path: str,
    simulation_execution_date: str,
    scenario: str,
    webviewer_version: str,
    host: str,
    port: str,
    user: str,
    password: str
):
    # Connect with database
    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}", echo=False
    )

    #########################
    # Create a new schema for this simulation
    ########################
    # Note: the schema name shouldn't be too long, otherwise it will give an "Identifier name... is too long" error.

    # If the scenario name is provided, we name the schema with the submission data and the scenario name.
    if scenario:
        scenario = scenario.replace(" ", "-")  # remove space
        schema_name = f"{simulation_execution_date}_{scenario}".lower()
    # If the scenario name is not provided, we name the schema with the submission data and time.
    else:
        schema_name = simulation_execution_date + '_unknown'
    schema_name = schema_name[:MYSQL_DATABASE_NAME_MAX_LENGTH]

    print(f"Results will be saved to the schema: {schema_name}")
    with engine.connect() as connection:
        connection.execute(DDL(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`"))
    # Print all schemas to check whether the current one gets successfully added
    # engine.execute('SHOW DATABASES').fetchall()

    #########################
    # Store all csv files
    #########################
    print("Move csv files...")

    all_csv_files = glob.glob(
        f"{simulation_postprocess_path}/**/*.csv",
        recursive=True
    )

    # Use regular expression to extract the file name. "(\\|\/)" means either "\" or "/" - different on unix and Windows.
    regex = r".*(\\|\/)(?P<tablename>.*)\.csv$"
    for filename in all_csv_files:
        # Import CSV to pandas
        data = pd.read_csv(filename)
        tablename = re.match(regex, filename).group("tablename")
        # Table names have to be lowercase:
        # https://stackoverflow.com/questions/59389911/sqlalchemy-exc-invalidrequesterror-could-not-reflect-requested
        # -tables-not-av
        data.to_sql(name=tablename.lower(), con=engine, schema=schema_name, if_exists='replace', index=False)
        print(f"Added to output DB: {tablename.lower()}")

    #########################
    # Store all json files
    #########################
    print("Move json files...")

    all_json_files = glob.glob(
        f"{simulation_postprocess_path}/**/*.json",
        recursive=True
    )
    regex = r".*(\\|\/)(?P<json_filename>.*)\.json$"

    # if there is any json files
    if all_json_files:
        with engine.connect() as connection:
            # First drop the existing table then create a new one - similar to the "if_exists='replace'" option in the
            # data.to_sql() commands
            connection.execute(f"DROP TABLE IF EXISTS `{schema_name}`.json_files")
            connection.execute(
                f"""CREATE TABLE `{schema_name}`.json_files (
                id VARCHAR(40) NOT NULL,
                json_file JSON NOT NULL,
                PRIMARY KEY (id));"""
            )
            for filename in all_json_files:
                json_filename = re.match(regex, filename).group("json_filename")
                with open(filename, "rb") as json_file:
                    json_data = json.load(json_file)
                connection.execute(
                    f"INSERT INTO `{schema_name}`.json_files(id, json_file) VALUES ('{json_filename}', '"
                    f"{json.dumps(json_data)}')"
                )
                print(f"Added to output DB: {json_filename}")

    #########################
    # Store all jpg files
    #########################
    print("Move jpg files...")

    all_jpg_files = glob.glob(
        f"{simulation_postprocess_path}/**/*.jpg",
        recursive=True
    )
    regex = r".*(\\|\/)(?P<imagename>.*)\.jpg$"

    # if there is any jpg files
    if all_jpg_files:
        # create a table for images
        with engine.connect() as connection:
            # First drop the existing table then create a new one - similar to the "if_exists='replace'" option in the
            # data.to_sql() commands
            connection.execute(f"DROP TABLE IF EXISTS `{schema_name}`.images")
            connection.execute(
                f"""CREATE TABLE IF NOT EXISTS `{schema_name}`.images (
                id VARCHAR(40) NOT NULL,
                image_file LONGBLOB NOT NULL,
                PRIMARY KEY (id));"""
            )
            for filename in all_jpg_files:
                imagename = re.match(regex, filename).group("imagename")
                with open(filename, "rb") as image_file:
                    encoded_string = base64.b64encode(image_file.read()).decode()
                connection.execute(
                    f"INSERT INTO `{schema_name}`.images(id, image_file) VALUES ('{imagename}', FROM_BASE64('"
                    f"{encoded_string}'))"
                )
                print(f"Added to output DB: {imagename}")

    print('\n')
    print('---------------------------------------------------------------')
    print('Link for the webviewer:')
    print(f'https://nexus-e.org/{webviewer_version}/{schema_name}')
    print('---------------------------------------------------------------')
    print('\n')

    logging.info('---------------------------------------------------------------')
    logging.info('Link for the webviewer:')
    logging.info(f'https://nexus-e.org/{webviewer_version}/{schema_name}')
    logging.info('---------------------------------------------------------------')
    # retrieve image: https://pynative.com/python-mysql-blob-insert-retrieve-file-image-as-a-blob-in-mysql/

    # def write_file(data, filename):
    #     # Convert binary data to proper format and write it on Hard Disk
    #     with open(filename, 'wb') as file:
    #         file.write(data)
    #
    # with engine.connect() as connection:
    #     result = connection.execute(
    #         f"SELECT * FROM `{rName}`.images"
    #     )
    #     for row in result:
    #         print("id = ", row[0])
    #         write_file(row[1], "test.jpg")

if __name__ == '__main__':
    config_file_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "config.toml"
    )
    settings = config.load(config.TomlFile(config_file_path))
    parser = argparse.ArgumentParser(description='Move csv/json/jpg data to MySQL')
    # TODO: allow to select what data to move to MySQL (e.g. sometimes we don't want to process the image data because it
    #  takes a long time.)# Source file: By default, it is left empty, then the script will look for and read from the
    #  lsf.o* file.
    parser.add_argument("--simu-name", type=str, help="Simulation name (i.e., result folder's name)",
                        default='nexuse_s6_vzug_wp2_ntc_100_Nov12_T0117_MB_12-Nov-2023_01-17')
    parser.add_argument("--scen-name", type=str, help="A descriptive scenario name (e.g. Baseline)",
                        default='nexuse_s6_vzug_wp2_ntc_100')
    parser.add_argument("--version-wv", type=str, help="Prefix for webviewer",
                        default='')
    args = parser.parse_args()
    main(
        simulation_postprocess_path=os.path.join(
            settings.results.simulation_folder,
            "postprocess"
        ),
        simulation_execution_date=datetime.now().strftime("%Y-%m-%dT%H-%M-%S"),
        scenario=args.scen_name,
        webviewer_version=args.version_wv,
        host=settings.output_database_server.host,
        port=settings.output_database_server.port,
        user=settings.output_database_server.user,
        password=settings.output_database_server.password
    )
