from loguru import logger
from sqlalchemy import create_engine, text


class DBConnection:
    """TODO: add docstring to this class"""

    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connect()

    def connect(self):
        """TODO: add docstring to this method"""
        conn_str = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            self.user,
            self.password,
            self.host,
            self.port,
            self.dbname,
        )
        try:
            self.db = create_engine(conn_str)
            logger.info("Connection engine created!")
        except Exception as e:
            logger.exception(e)

    def execute(self, sql: str, params: dict):
        """TODO: add docstring to this method"""
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
        """TODO: add docstring to this method"""
        sql = """SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :schema_name"""

        params = {
            "schema_name": schema,
        }

        return self.execute(sql, params)

    def create_schema(self, schema):
        """TODO: add docstring to this method"""
        sql = """CREATE SCHEMA IF NOT EXISTS """ + schema + ";"
        params = {}
        return self.execute(sql, params)

    def set_schema(self, schema):
        """TODO: add docstring to this method"""

        sql = """SET search_path TO """ + schema + ";"
        params = {}
        return self.execute(sql, params)

    def check_monitoring_table(self, schema, table="monitoring"):
        """TODO: add docstring to this method"""

        checking_sql = """SELECT 1 FROM INFORMATION_SCHEMA.TABLES A WHERE A.TABLE_SCHEMA = :schema_name AND a.table_name = :table_name"""
        checking_params = {
            "schema_name": schema,
            "table_name": table,
        }
        res = self.execute(checking_sql, checking_params).fetchone()
        if res is not None and res[0] == 1:
            logger.info(f"Monitoring table already exists in the {schema} schema!")
        else:
            creating_sql = (
                """
                CREATE TABLE """
                + schema
                + "."
                + table
                + """ (
                    id  SERIAL PRIMARY key,
                    table_name VARCHAR(255),
                    chunk_number INT,
                    chunk_start_date timestamp,
                    chunk_end_date timestamp
                    );
            """
            )
            creating_params = {}
            self.execute(creating_sql, creating_params)
            logger.info(f"Monitoring table has been created in the {schema} schema!")

    def insert_into_monitoring_table(self, params, schema="public", table="monitoring"):
        """This Method will allow us to insert a new line in the monitoring table

        Args:
            params (dict): dict of args that we will replace in the sql query
            table (str, optional): monitoring table name. Defaults to "monitoring".
        """
        insert_sql = (
            """INSERT INTO """
            + schema
            + "."
            + table
            + """ (table_name, chunk_number, chunk_start_date, chunk_end_date) VALUES(:table_name, :chunk_number, :chunk_start_date, :chunk_end_date);"""
        )

        self.execute(sql=insert_sql, params=params)

    def update_monitoring_table(self, params, schema="public", table="monitoring"):
        """This Method will allow us to insert a new line in the monitoring table
        Args:
            params (dict): dict of args that we will replace in the sql query
            table (str, optional): monitoring table name. Defaults to "monitoring".
        """
        update_sql = (
            """UPDATE """
            + schema
            + "."
            + table
            + """ SET chunk_end_date= :chunk_end_date
            WHERE table_name=:table_name AND chunk_number = :chunk_number;"""
        )
        self.execute(sql=update_sql, params=params)

    # def get_the_last_split_injected(self, params, schema="public", table="monitoring"):

    #      update_sql = """SELECT M"""
    #     self.execute(sql=update_sql, params=params)

    def close(self):
        """TODO: add docstring to this method"""

        self.db.dispose()
        logger.info("Connection engine closed successfully!")
