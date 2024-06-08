from loguru import logger

from LFI import LFI, DBConnection
from utils import *

if __name__ == "__main__":
    host, port, dbname, user, password = get_db_credentials(".config")
    connection = DBConnection(host, port, dbname, user, password)
    lfi = LFI(path="files", connection=connection, split_size=10000, chunk_size=1024, schema="mpsy")
    lfi.start()
