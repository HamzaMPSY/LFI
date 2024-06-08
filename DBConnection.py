from loguru import logger
from sqlalchemy import create_engine, text


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
            logger.info("Connection engine created!")
        except Exception as e:
            logger.exception(e)

    def execute(self, sql: str, params: dict):

        with self.db.connect() as conn:
            statement = text(sql)
            try:
                rs = conn.execute(statement=statement, parameters=params)
                conn.commit()
                return rs
            except Exception as e:
                logger.exception(e)
        conn.close()

    def check_schema_exists(self, schema):
        sql = """select 1 from information_schema.schemata where schema_name = :schema_name"""

        params = {
            "schema_name": schema,
        }

        return self.execute(sql, params)

    def create_schema(self, schema):
        sql = """CREATE SCHEMA IF NOT EXISTS """ + schema + ";"
        params = {}
        return self.execute(sql, params)

    def set_schema(self, schema):
        sql = """SET search_path TO """ + schema + ";"
        params = {}
        return self.execute(sql, params)

    def check_monitoring_table(self, schema, table="monitoring"):
        checking_sql = """select 1 from information_schema.tables a where a.table_schema = :schema_name and a.table_name = :table_name"""
        checking_params = {
            "schema_name": schema,
            "table_name": table,
        }
        res = self.execute(checking_sql, checking_params).fetchone()
        if res is not None and res[0] == 1:
            logger.info(
                "Monitoring table already exists in the %s schema!" % (schema))
        else:
            creating_sql = """
                CREATE TABLE monitoring (
                    id  SERIAL PRIMARY key,
                    table_name VARCHAR(255),
                    chunk_number INT,
                    chunk_start_date timestamp,
                    chunk_end_date timestamp
                    );
            """
            creating_params = {}
            self.execute(creating_sql, creating_params)
            logger.info(
                "Monitoring table has been created in the %s schema!" % (schema))

    def close(self):
        self.db.dispose()
        logger.info("Connection engine closed successfully!")

