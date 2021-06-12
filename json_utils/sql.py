"""
| program file: sql.py
| Python class: SQL
| Purpose: utilities for database, calls postgresql.py
| 
| extra methods like table_exist, schema_exist, table_list_in_schema, func_list_in_schema
|
| Updates: 25MAR2021-LD allow called directly, also from installed package
"""
import glob
import hashlib
import os
import subprocess
import sys

import sqlparse
from dateutil import parser

import logzero
import traceback # Python error trace
from logzero import logger

try:
    # call directly
    from postgresql import PostgreSQL
except:
    # after install json_utils, call this way
    from json_utils.postgresql import PostgreSQL

class Sql(object):
    def __repr__(self):
        return 'Sql'

    def __init__(self, host, dbname, user='analytics'):
        self.connection_fun = PostgreSQL
        self.host=host
        self.dbname=dbname
        self.user=user
        self.connection = None

    def greenplum_password(self, user_name):
        passfile = f'/tmp/{user_name.lower()}.dat'
        return open(passfile).read().strip()

    def __connect(self):
        if not self.connection:
            self.connection = self.connection_fun( self.host, self.user, self.dbname,
                                      self.greenplum_password(self.user))
        return self.connection

    def clean_connection(self):
        self.connection = None

    def sql_execute_with_replace(self, query, replace_dict=None, lower_logging_level=False):
        if replace_dict is not None:
            for key, val in replace_dict.items():
                query = query.replace(key, val)

        return self.__connect().execute(query=query, lower_logging_level=lower_logging_level)

    def sql_run_file(self, sql_file, replace_dict=None, ignore_sql_errors=False, lower_logging_level=False):
        with open(sql_file, encoding='utf-8') as f:
            for query in sqlparse.split(f.read()):
                real_query = sqlparse.format(query, strip_comments=True).strip()
                if len(real_query) == 0 or real_query == ';':
                    logger.debug(f"One of queries in {sql_file} is empty. Empty query content: {real_query} in module SQL")
                    continue
                try:
                    if query.strip():
                        self.sql_execute_with_replace(query.strip(), replace_dict, lower_logging_level)
                except Exception as e:
                    if not ignore_sql_errors:
                        raise ProcessException(f'Error occurred while running query: '
                                               f'\n{query.strip()[:500]}... (concatenated). Please see previous logs.')
                    else:
                        logger.error(f'Process continues. Error occurred while running query: '
                                     f'\n{query.strip()[:500]}... (concatenated). Please see previous logs.')

    def sql_execute_folder(self
                          , folder_path=None
                          , file_pattern='ARDM_*.sql'
                          , file_name_to_skip=None  # recover from this SQL file
                          , replace_dict=None  # replace string in query before submit
                          , ignore_sql_errors=False
                          , lower_logging_level=False
                          ):

        file_list = glob.glob(f"{folder_path}{os.sep}{file_pattern}")
        for idx, sql_file in enumerate(sorted(file_list), start=1):
            file_name = os.path.basename(sql_file)
            logger.info(f"Executing SQL  file: {file_name} in module SQL")
            if file_name_to_skip and file_name in file_name_to_skip:
                continue
            self.sql_run_file(sql_file, replace_dict, ignore_sql_errors, lower_logging_level)

    # extra member for further development
    def table_exist(self, gpschema, gptable):
        '''
        | check if the table exists in given schema
        '''
        qry=f"select exists (select 1 from pg_tables where schemaname='{gpschema}' and tablename='{gptable}')"
        rst=self.sql_execute_with_replace(qry)
        logger.debug(f"Query: {qry} with result: {rst[0][0]}")
        return rst[0][0]
        
    def schema_exist(self, gpschema):
        '''
        | check if the schema exists
        '''
        qry=f"select exists (select 1 from information_schema.schemata where schema_name='{gpschema}')"
        rst=self.sql_execute_with_replace(qry)
        logger.debug(f"Query: {qry} with result: {rst[0][0]}")
        return rst[0][0]
        
    def table_list_in_schema(self, gpschema, pattern=None):
        '''
        | obtain whole table list in given schema
        '''
        qry=f"select table_name from information_schema.tables where table_schema='{gpschema}'"
        if pattern is not None:
            qry=f"{qry} and table_name like '%{pattern}%'"
        rst_=self.sql_execute_with_replace(qry)
        rst=[x[0] for x in rst_]
        logger.debug(f"Query: {qry} with result: {rst}")
        return rst
        
    def func_list_in_schema(self, gpschema):
        '''
        | obtain whole function list in given schema
        '''
        qry=f"select quote_ident(n.nspname) || '.' || quote_ident(p.proname) || func.getFunctionParameter(p.oid)"
        qry=f"{qry} from pg_language AS l JOIN pg_proc AS p ON p.prolang = l.oid"
        qry=f"{qry} JOIN pg_namespace AS n ON p.pronamespace = n.oid"
        qry=f"{qry} WHERE n.nspname = '{gpschema}'"
        rst_=self.sql_execute_with_replace(qry)
        rst=[x[0] for x in rst_]
        logger.debug(f"Query: {qry} with result: {rst}")
        return rst

    def import_from_file(self, query, file):
        return self.__connect().import_from_file(query, file)
