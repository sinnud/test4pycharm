""" Connect to PostgreSQL database
"""
import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import psycopg2

import time

class PostgresqlConnect(object):
    """ class PostgresqlConnect to connect to postgresql database

        :param host: postgresql host name (ip address)
        :param user: user name in postgresql database
        :param password: user password in postgresql database
        :param database: the database name you like to connect
    """
    def __init__(self
        , host='localhost'
        , user='sinnud'
        , password='Jeffery45!@'
        , database='dbhuge'
        ):
        self.host=host
        self.user=user
        self.password=password
        self.dbname=database
        self.conn_string=f"host='{host}' dbname='{database}' user='{user}' password='{password}'"
        self.conn=None

    def __del__(self):
        """ destructor method -- not show in doc
        """
        if self.conn:
            self.conn.close()
            logger.debug(f"=== Disconnect to {self.host} with account {self.user} ===")

    def connect(self):
        """ connect to database: method for :class:`PostgresqlConnect`
        """
        self.conn = psycopg2.connect(self.conn_string)
        #self.conn.autocommit(True) # mssql
        self.conn.set_session(autocommit=True)
        #self.conn.set_client_encoding('UTF8')
        logger.debug(f"=== Connect to {self.host} using account {self.user} ===")

    def cursor(self):
        """ get cursor of connection to database
        """
        if not self.conn: # or self.conn.closed:
            self.connect()
        return self.conn.cursor()

    """ submit query to database -- not show in doc
    """
    def execute(self, query):
        """ ubmit query to database

        :param query: string of query you like to submit
        """
        if not self.conn or self.conn.closed:
            self.connect()
        logger.debug(f'RUNNING QUERY: {query}')
        start = time.time()
        cursor = self.conn.cursor()
        cursor.execute(query)
        logger.debug(f'# ROWS AFFECTED : {cursor.rowcount}, took {time.time() - start} seconds.')
        try:
            rst = cursor.fetchall()
        except Exception as e:
            return cursor
        if len(rst)<1000:
            logger.debug(f'# RESULT : {rst}.')
        return rst        

    def close(self):
        #if not self.conn.closed:
        logger.debug(f'====== Close {self.host} with account {self.user} ======')
        self.conn.close()
        
""" test function -- not show in doc
"""
def main(arg=None):
    """ test function
    """
    psc = PostgresqlConnect()
    """
    test submitting queries -- not show in doc
    """
    rst = psc.execute('set search_path=wdinfo')
    rst = psc.execute('select count(*) from sinnud')
    rst = psc.execute('drop table sinnud')
    return True
if __name__ == '__main__':
    mylog=os.path.realpath(__file__).replace('.py','.log')
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    if len(sys.argv)>1:
        logger.info(f"Argument: {sys.argv}")
        myarg=sys.argv
        pgmname=myarg.pop(0)
        logger.info(f"Program name:{pgmname}.")
        logger.info(f"Arguments:{myarg}.")
        rst=main(arg=' '.join(myarg))
    else:
        logger.info(f"No arguments")
        rst=main()
    if not rst:
        # record error in log such that process.py will capture it
        logger.error(f"ERROREXIT: Please check")
    logger.info(f'end python code {__file__}.\n')