from sqlalchemy import create_engine
from loguru import logger


class DBConnection(object):
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connect()

    def connect(self):
        conn_str = 'postgresql+psycopg2://%s:%s@%s:%s/%s' % (
            self.user, self.password, self.host, self.port, self.dbname)
        try:
            self.db = create_engine(conn_str)
            self.db.connect()
            logger.info("Connection engine created!")
        except Exception as e:
            logger.exception(e)

    def close(self):
        self.db.dispose()
        logger.info("Connection engine closed successfully!")
