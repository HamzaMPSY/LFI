""" TODO: write the docstring."""

import os
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
        sep=",",
    ) -> None:
        self.path = path
        self.connection = connection
        self.schema = schema
        self.split_size = split_size
        self.chunk_size = chunk_size
        self.sep = sep

    def start(self):
        """TODO: write the docstring for this method."""
        # Check the schema if it exists else we create it
        rs = self.connection.check_schema_exists(self.schema).fetchone()
        if rs is not None and rs[0] == 1:
            logger.info(f"Schema {self.schema} exists!")
        elif self.connection.create_schema(self.schema):
            self.connection.set_schema(self.schema)
            logger.info(f"Schema {self.schema} Created successfully!")
        else:
            logger.error(f"Error while Creating the Schema {self.schema}!")

        # Check if the monitoring table exists else we create it
        self.connection.check_monitoring_table(self.schema)

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

    def process(self, file: str):
        """TODO: write the docstring for this method."""
        # Get the table name and the path to the directory that contains the splits
        table_name, dir_full_path = get_folder_name(file)
        # Get columns and split the large file into splits of split_size lins
        cols = split_large_file(file, self.split_size, table_name, dir_full_path)
        # Split the cols line to a list of culumns
        columns = cols.split(self.sep)
        columns.append("split_id")
        # TODO: get this from the monitoring table
        create = True
        for i, file_part in enumerate(
            sorted(os.listdir(dir_full_path), key=lambda x: int(x.split(".")[-1]))
        ):
            self.connection.insert_into_monitoring_table(
                schema=self.schema,
                params={
                    "table_name": table_name,
                    "chunk_number": i,
                    "chunk_start_date": datetime.now(),
                    "chunk_end_date": None,
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
                    if create:
                        df.head(n=0).to_sql(
                            schema=self.schema,
                            name=table_name,
                            con=self.connection.db,
                            if_exists="replace",
                        )
                        create = False
                    df.to_sql(
                        schema=self.schema,
                        name=table_name,
                        con=self.connection.db,
                        if_exists="append",
                    )
                except StopIteration:
                    break
            self.connection.update_monitoring_table(
                schema=self.schema,
                params={
                    "table_name": table_name,
                    "chunk_number": i,
                    "chunk_end_date": datetime.now(),
                },
            )
