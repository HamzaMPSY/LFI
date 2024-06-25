import psycopg2
from loguru import logger
from sqlalchemy import create_engine


class DBConnection:
    """TODO: add docstring to this class"""

    def __init__(self, host, port, dbname, user, password):
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

        self.con = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port)

    def execute(self, query: str, params: dict):
        """Methode that take query and dict of parameters and parse the query then execute

        Args:
            query (str): query query with parameters
            params (dict): dict of paramters to parse

        Returns:
            List: List of all records or None if it's a DDL query
        """
        parsed_query = self.parse(query=query, params=params)
        # logger.info("Executing Query: " + parsed_query, level=2)
        # open the cursor
        cur = self.con.cursor()
        try:
            cur.execute(parsed_query)
            if cur.description is not None:
                # It's a SELECT query (or any other query that returns rows)
                data = cur.fetchall()
            else:
                # It's a DDL or DML query that does not return rows
                data = None
            self.con.commit()
        except (psycopg2.ProgrammingError, psycopg2.DatabaseError) as e:
            logger.exception("Error executing query")
            self.con.rollback()
            data = None
        finally:
            cur.close()

        return data

    def parse(self, query: str, params: dict) -> str:
        """Method that will take a sql query and replace all parameters 
        with the corresponding value from the dictionary

        Args:
            query (str): sql query
            params (dict): dict of parameters

        Returns:
            str: parsed query
        """
        for key, value in params.items():
            if value['type'] in ('schema', 'table', 'column'):
                query = query.replace(':' + key, value['value'])
            elif value['type'] == 'value':
                if value['value'] is None:
                    query = query.replace(':' + key, "NULL")
                elif isinstance(value['value'], int):
                    query = query.replace(':' + key, str(value['value']))
                else:
                    query = query.replace(':' + key, f"'{value['value']}'")
            else:
                raise ValueError(f"Unknown parameter type: {value['type']}")
        return query

    def check_schema_exists(self, schema_name: str) -> bool:
        """Method that will check if a certain schema is existing in the database that we are connected to

        Args:
            schema (str): schema_name

        Returns:
            bool: True if exists, False otherwise
        """        """"""
        sql = """SELECT 1 FROM :table WHERE :column = :schema_name"""

        params = {
            "schema_name": {
                "type": 'value',
                "value": schema_name
            },
            "table": {
                "type": 'table',
                "value": "INFORMATION_SCHEMA.SCHEMATA"
            },
            "column": {
                "type": 'column',
                "value": "SCHEMA_NAME"
            }
        }

        results = self.execute(sql, params)
        return len(results) > 0 and results[0][0] == 1

    def create_schema(self, schema_name: str) -> bool:
        """Method that create a certain schema given its name

        Args:
            schema_name (str): schema's name

        Returns:
            bool : whether the schema is created or not
        """
        sql = """CREATE SCHEMA IF NOT EXISTS :schema_name"""
        params = {
            "schema_name": {
                "type": 'schema',
                "value": schema_name
            },
        }
        return self.execute(sql, params) is None

    def check_monitoring_table(self, schema_name: str, monitoring_table_name: str = "monitoring") -> bool:
        """Methode to check if the monitoring table exist in a particular schema

        Args:
            schema_name (str): schema's name
            monitoring_table_name (str, optional): monitoring table name. Defaults to "monitoring".

        Returns:
            bool: True it exists already, False otherwise
        """

        checking_sql = """SELECT 1 FROM INFORMATION_SCHEMA.TABLES A WHERE A.TABLE_SCHEMA = :schema_name AND a.table_name = :table_name"""
        checking_params = {
            "schema_name": {
                "type": 'value',
                "value": schema_name
            },
            "table_name": {
                "type": 'value',
                "value": monitoring_table_name
            }
        }
        results = self.execute(checking_sql, checking_params)
        return len(results) > 0 and results[0][0] == 1

    def create_monitoring_table(self, schema_name: str, monitoring_table_name: str = "monitoring") -> bool:
        creating_sql = """
                CREATE TABLE :schema_name.:table_name (
                    id  SERIAL PRIMARY key,
                    table_name VARCHAR(255),
                    chunk_number INT,
                    chunk_start_date timestamp,
                    chunk_end_date timestamp
                    )
            """
        creating_params = {
            "schema_name": {
                "type": 'schema',
                "value": schema_name
            },
            "table_name": {
                "type": 'table',
                "value": monitoring_table_name
            }
        }
        return self.execute(creating_sql, creating_params) is None

    def delete_table_monitoring(self, params):
        delete_query = """
            delete from :schema_name.:monitoring_table_name where
                table_name = :table_name"""
        self.execute(query=delete_query, params=params)
        logger.info(
            f"Rows of {params['table_name']['value']} has been deleted!")

    def insert_into_monitoring_table(self, params: dict):
        """This Method will allow us to insert a new line in the monitoring table

        Args:
            params (dict): dict of args that we will replace in the sql query
            table (str, optional): monitoring table name. Defaults to "monitoring".
        """
        insert_query = """INSERT INTO :schema_name.:monitoring_table_name  (table_name, chunk_number, chunk_start_date, chunk_end_date) VALUES(:table_name, :chunk_number, :chunk_start_date, :chunk_end_date)"""
        self.execute(query=insert_query, params=params)

    def update_monitoring_table(self, params: dict):
        """This Method will allow us to insert a new line in the monitoring table
        Args:
            params (dict): dict of args that we will replace in the sql query
            table (str, optional): monitoring table name. Defaults to "monitoring".
        """
        update_query = """UPDATE :schema_name.:monitoring_table_name SET chunk_end_date= :chunk_end_date WHERE table_name=:table_name AND chunk_number = :chunk_number"""
        self.execute(query=update_query, params=params)

    def get_the_last_split_injected(self, params: dict) -> list:

        select_query = """ select
                *
            from :schema_name.:monitoring_table_name where
                chunk_number = (
                    select
                        max(chunk_number)
                    from :schema_name.:monitoring_table_name where
                        table_name = :table_name)
                and table_name = :table_name"""

        return self.execute(query=select_query, params=params)

    def delete_rows_split_id(self, params: dict) -> None:
        delete_query = """
            delete from :schema_name.:monitoring_table_name where
                table_name = :table_name and chunk_number = :chunk_number"""
        self.execute(query=delete_query, params=params)
        logger.info(
            f"Rows from {params['chunk_number']['value']} of {params['table_name']['value']} has been deleted")

    def close(self):
        """TODO: add docstring to this method"""

        self.db.dispose()
        logger.info("Connection engine closed successfully!")
