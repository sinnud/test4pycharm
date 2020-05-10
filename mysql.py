import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import mysql.connector # test mysql connection
import time

class MySqlConnect(object):
    def __init__(self, host="localhost", user="sinnud", passwd="Jeffery45!@"
                    , database="test", allow_local_infile=True):
        self.host=host
        self.user=user
        self.database=database
        self.conn = None
        self.conn_string = f"host='{host}', database='{database}', user='{user}', password='{passwd}'"
        logger.debug(f'======Connect to {host}.{database} using account {user}======')

    def connect(self):
        self.conn = mysql.connector.connect(self.conn_string)
        self.conn.set_session(autocommit = True)
        cursor = self.conn.cursor()
        cursor.execute(f'USE {self.database}')

    def cursor(self):
        if not self.conn or self.conn.closed:
            self.connect()
        return self.conn.cursor()

    def execute(self, query):
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
        if not self.conn.closed:
            self.conn.close()

def main():
    logger.debug("Start main... (connect to MySql)")
    mydb=MySqlConnect()
    logger.debug("Start query...")
    result=mydb.execute("select count(*) from photo_info")
    logger.debug("Log result...")
    logger.info(result)
    logger.debug("Close...")
    mydb.close()
    return True

if __name__ == '__main__':
    mylog=os.path.realpath(__file__).replace('.py','.log')
    #mylog=f"{config.WORKDIR}/{mylog}"
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
