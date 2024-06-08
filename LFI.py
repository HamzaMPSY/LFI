import os

import pandas as pd
from loguru import logger

from DBConnection import DBConnection
from utils import get_folder_name, split_large_file


class LFI(object):
    def __init__(self, path: str, connection: DBConnection, split_size: int, chunk_size: int, schema='public', sep = ',') -> None:
        self.path = path
        self.connection = connection
        self.schema = schema
        self.split_size = split_size
        self.chunk_size = chunk_size
        self.sep= sep

    def start(self):
        # Check the schema if it exists else we create it
        rs = self.connection.check_schema_exists(self.schema).fetchone()
        if rs is not None and rs[0] == 1:
            logger.info("Schema %s exists!" % (self.schema))
        elif self.connection.create_schema(self.schema):
            self.connection.set_schema(self.schema)
            logger.info("Schema %s Created successfully!" % (self.schema))
        else:
            logger.error("Error while Creating the Schema %s!" % (self.schema))

        # Check if the monitoring table exists else we create it
        self.connection.check_monitoring_table(self.schema)

        # check id the path is a folder or a file
        if os.path.isdir(self.path):
            logger.info("Working on Directory of files: %s!" % (self.path))
            for file in os.listdir(self.path):
                file_path = os.path.join(self.path, file)
                if os.path.isfile(file_path):
                    logger.info("Working on the file: %s!" % (file_path))
                    self.process(file_path)
        elif os.path.isfile(self.path):
            logger.info("Working on the file: %s!" % (self.path))
            self.process(self.path)
        else:
            logger.error("The provided path %s is empty!" % (self.path))

    def process(self, file: str):
        dir_name, dir_full_path = get_folder_name(file)
        split_large_file(file, self.split_size, dir_name, dir_full_path)

        for file_part in os.listdir(dir_full_path):
            df = pd.read_csv(os.path.join(dir_full_path,file_part), sep = self.sep, chunksize=self.chunk_size)