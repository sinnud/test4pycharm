"""
| program file: postgresql.py
| Python class: PostgreSQL
| Purpose: connect to database
"""
import psycopg2
import time

import logzero
import traceback # Python error trace
from logzero import logger

class PostgreSQL(object):
    def __init__(self, host, user, dbname, password, port='5432'):
        self.module_name = 'PostgreSQL'
        self.host = host
        self.user = user
        self.dbname = dbname
        self.port = port
        self.password = password

        self.conn_string = f"host='{self.host}' port='{self.port}' dbname='{self.dbname}' user='{self.user}' password='{self.password}'"
        self.conn = None
        # self.logger = AL().get_logger()
        logger.debug(f'Connect to {self.host}:{self.port}.{self.dbname} using user {self.user} in module {self.module_name}')

    def __repr__(self):
      return f"PostgreSQL: host='{self.host}' port='{self.port}' dbname='{self.dbname}'"

    def __del__(self):
        if self.conn:
            self.conn.close()
            logger.debug(f"Connection to database {self.host}:{self.port}.{self.dbname} closed in module {self.module_name}")

    def cursor(self):
        if not self.conn or self.conn.closed or self.conn.cursor().closed:
            self.connect()
        return self.conn.cursor()

    def tables(self, schema):
        schema_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = {schema} AND table_type = 'BASE TABLE'"
        return [x[0] for x in self.execute(schema_query)]

    def connect(self):
        self.conn = psycopg2.connect(self.conn_string)
        self.conn.set_session(autocommit=True)
        self.conn.set_client_encoding('UTF8')

    def execute(self, query, lower_logging_level=False):
        if not self.conn or self.conn.closed:
            self.connect()
        start = time.time()
        cursor = self.conn.cursor()
        cursor.execute(query)
        log_message = f'Executed query on {self.host}:{self.port}.{self.dbname}:\n\n {query}  \n\n' \
            f'Rows affected : {cursor.rowcount}, took {time.time() - start} seconds.'

        if lower_logging_level:
            logger.info(f"{log_message} in module {self.module_name}")
        else:
            logger.debug(f"{log_message} in module {self.module_name}")

        try:
            rst = cursor.fetchall()
        except Exception as e:
            return cursor
        if len(rst) < 1000:
            logger.debug(f'# Query  result on {self.host}:{self.port}.{self.dbname}: {rst}. in module {self.module_name}')
        return rst

    # Used for importing data from file
    def import_from_file(self, query, file):
        if not self.conn or self.conn.closed:
            self.connect()
        logger.debug(f'Import from file {file} using query: \n\n {query} in module {self.module_name} on {self.host}:{self.port}.{self.dbname}')
        start = time.time()
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(query, file)
            logger.debug(f'Rows affected on {self.host}:{self.port}.{self.dbname}: {cursor.rowcount}, took {time.time() - start} seconds. in module {self.module_name}')
        except Exception as e:
            self.conn.rollback()
            logger.exception(e)
            logger.error(f"Copy expert failed on {self.host}:{self.port}.{self.dbname}. File: {file}. Query: {query}. Please see previous logs. in module={self.module_name}")
            return False

        return True

    # Used for exporting data to a file
    def export_to_file(self, query, file):
        if not self.conn or self.conn.closed:
            self.connect()
        logger.debug(f'Export to file {file} using query: \n\n {query} \nin module {self.module_name} on {self.host}:{self.port}.{self.dbname}')
        start = time.time()
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(query, file)
            logger.debug(f'Rows exported on {self.host}:{self.port}.{self.dbname}: {cursor.rowcount}, took {time.time() - start} seconds. in module {self.module_name}')
        except Exception as e:
            self.conn.rollback()
            logger.exception(f"Copy expert failed on {self.host}:{self.port}.{self.dbname}. File: {file}. Query: {query}. Please see previous logs.")
            return False

        return True
