""" TODO: write the docstring."""

import os
import sys
from datetime import datetime

import pandas as pd
from loguru import logger

from DBConnection import DBConnection
from utils import get_folder_name, split_large_file


class Lfi(object):
    """TODO: write the docstring for this class."""

    def __init__(
        self,
        path: str,
        connection: DBConnection,
        split_size: int,
        chunk_size: int,
        schema="public",
        monitoring_table_name="monitoring",
        sep=",",
        overwrite=False
    ) -> None:
        self.path = path
        self.connection = connection
        self.schema = schema
        self.split_size = split_size
        self.chunk_size = chunk_size
        self.sep = sep
        self.monitoring_table_name = monitoring_table_name
        self.overwrite = overwrite

    def start(self):
        """TODO: write the docstring for this method."""
        # Check the schema if it exists else we create it
        if self.connection.check_schema_exists(self.schema):
            logger.info(f"Schema \"{self.schema}\" already exists!")
        elif self.connection.create_schema(self.schema):
            logger.info(f"Schema \"{self.schema}\" Created successfully!")
        else:
            logger.error(f"Error while Creating the Schema \"{self.schema}\"!")
            sys.exit(-1)

        # Check if the monitoring table exists else we create it
        if self.connection.check_monitoring_table(schema_name=self.schema, monitoring_table_name=self.monitoring_table_name):
            logger.info(
                f"Monitoring table \"{self.monitoring_table_name}\" already exists in the \"{self.schema}\" schema!")
        elif self.connection.create_monitoring_table(schema_name=self.schema, monitoring_table_name=self.monitoring_table_name):
            logger.info(
                f"Monitoring table \"{self.monitoring_table_name}\" has been created in the \"{self.schema}\" schema!")
        else:
            logger.error(
                f"Error while Creating the monitoring table \"{self.monitoring_table_name}\" in \"{self.schema}\"!")
            sys.exit(-1)

        # check id the path is a folder or a file
        if os.path.isdir(self.path):
            logger.info(f"Working on Directory of files: {self.path}!")
            for file in os.listdir(self.path):
                file_path = os.path.join(self.path, file)
                if os.path.isfile(file_path):
                    logger.info(f"Working on the file: {file_path}!")
                    self.process(file_path)
        elif os.path.isfile(self.path):
            logger.info(f"Working on the file: {self.path}!")
            self.process(self.path)
        else:
            logger.error(f"The provided path {self.path} is empty!")
            sys.exit(-1)

    def process(self, file: str):
        """TODO: write the docstring for this method."""
        # Get the table name and the path to the directory that contains the splits
        table_name, dir_full_path = get_folder_name(file)
        # Get columns and split the large file into splits of split_size lins
        cols = split_large_file(file, self.split_size,
                                table_name, dir_full_path)
        # Split the cols line to a list of culumns
        columns = cols.split(self.sep)
        columns.append("split_id")
        # TODO: get this from the monitoring table
        last_split_injected_results = self.connection.get_the_last_split_injected(
            params={
                "schema_name": {
                    "type": 'schema',
                    "value": self.schema
                },
                "monitoring_table_name": {
                    "type": 'table',
                    "value": self.monitoring_table_name
                },
                "table_name": {
                    "type": 'value',
                    "value": table_name
                }
            },)

        if len(last_split_injected_results) == 0:
            is_split_finished = True
            split_id = 0
        else:
            split_id = last_split_injected_results[0][2]
            is_split_finished = last_split_injected_results[0][4] is not None
        if self.overwrite:
            split_id = 0
            self.connection.delete_table_monitoring(
                params={
                    "schema_name": {
                        "type": 'schema',
                        "value": self.schema
                    },
                    "monitoring_table_name": {
                        "type": 'table',
                        "value": self.monitoring_table_name
                    },
                    "table_name": {
                        "type": 'value',
                        "value": table_name
                    }
                }
            )
        # if we are not gonna recreate the table and the last split has not finished injecting
        # we gonna remove all lines of this split and continue injecting from it
        if not self.overwrite and not is_split_finished:
            self.connection.delete_rows_split_id(
                params={
                    "schema_name": {
                        "type": 'schema',
                        "value": self.schema
                    },
                    "monitoring_table_name": {
                        "type": 'table',
                        "value": self.monitoring_table_name
                    },
                    "table_name": {
                        "type": 'value',
                        "value": table_name
                    },
                    "chunk_number": {
                        "type": 'value',
                        "value": split_id
                    }
                },)
        for i, file_part in enumerate(
            sorted(os.listdir(dir_full_path),
                   key=lambda x: int(x.split(".")[-1]))
        ):
            if i < split_id:
                continue
            self.connection.insert_into_monitoring_table(
                params={
                    "schema_name": {
                        "type": 'schema',
                        "value": self.schema
                    },
                    "monitoring_table_name": {
                        "type": 'table',
                        "value": self.monitoring_table_name
                    },
                    "table_name": {
                        "type": 'value',
                        "value": table_name
                    },
                    "chunk_number": {
                        "type": 'value',
                        "value": i
                    },
                    "chunk_start_date": {
                        "type": 'value',
                        "value": datetime.now()
                    },
                    "chunk_end_date": {
                        "type": 'value',
                        "value": None
                    },
                },
            )
            df_iter = pd.read_csv(
                os.path.join(dir_full_path, file_part),
                sep=self.sep,
                chunksize=self.chunk_size,
                iterator=True,
                dtype=str,
            )
            while True:
                try:
                    df = next(df_iter)
                    df["split_id"] = str(i)
                    df.columns = columns
                    df.set_index(columns[0])
                    if self.overwrite:
                        df.head(n=0).to_sql(
                            schema=self.schema,
                            name=table_name,
                            con=self.connection.engine,
                            if_exists="replace",
                        )
                        self.overwrite = False
                    df.to_sql(
                        schema=self.schema,
                        name=table_name,
                        con=self.connection.engine,
                        if_exists="append",
                    )
                except StopIteration:
                    break
            self.connection.update_monitoring_table(
                params={
                    "schema_name": {
                        "type": 'schema',
                        "value": self.schema
                    },
                    "monitoring_table_name": {
                        "type": 'table',
                        "value": self.monitoring_table_name
                    },
                    "table_name": {
                        "type": 'value',
                        "value": table_name
                    },
                    "chunk_number": {
                        "type": 'value',
                        "value": i
                    },
                    "chunk_end_date": {
                        "type": 'value',
                        "value": datetime.now()
                    },
                },
            )
