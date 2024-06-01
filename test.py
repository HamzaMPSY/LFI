from LFI import DBConnection
from loguru import logger
from utils import *

if __name__ == "__main__":
    host, port, dbname, user, password = get_db_credintials(".config")
    connection = DBConnection(host, port, dbname, user, password)
    connection.close()
