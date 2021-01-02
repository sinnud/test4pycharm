import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import time

from .conn import PostgresqlConnect

class PostgresqlUtils(object):
    def __init__(self
        , host='localhost'
        , user='sinnud'
        , password='Jeffery45!@'
        , database='dbhuge'
        ):
        self.conn=PostgresqlConnect(host, user, password, database)

    def __del__(self):
        if self.conn:
            self.conn.close()
            logger.debug(f"=== Disconnect to {self.host} with account {self.user} ===")

    ''' load data into PostgreSql on localhost '''
    def local_import(self, query, file):
        logger.debug(f"Import {file} using query\n\n{query}")
        start = time.time()
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(query, file)
            logger.debug(f"Rows affected: {cursor.rowcount}, took {time.time() - start} seconds")
        except:
            logger.error(f"{traceback.format_exc()}")
            self.conn.rollback()
            return False
        return True

    def table_exist(self, tbl=None):
        qry=f"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        qry=f"{qry}   AND TABLE_NAME='{tbl}'"
        rst = self.execute(qry)
        if rst[0][0]==0:
            return False
        return True

    def truncate_table(self, tbl=None):
        qry=f"truncate table {tbl}"
        try:
            self.execute(qry)
            return True
        except:
            logger.error(f"{traceback.format_exc()}")
        return False

    def create_table(self, tbl=None, tbl_str=None, distby=None):
        if distby is None:
            qry=f"create table {tbl} ({tbl_str})"
        else:
            qry=f"create table {tbl} ({tbl_str}) distributed by ({distby})"
        try:
            self.execute(qry)
            return True
        except:
            logger.error(f"{traceback.format_exc()}")
        return False
       
    def create_truncate_table(self, tbl=None, tbl_str=None, distby=None):
        if self.table_exist(tbl=tbl):
            return self.truncate_table(tbl=tbl)
        return self.create_table(tbl=tbl, tbl_str=tbl_str)

    def import_datalist(self, datalist=None, tbl=None, tbl_str=None, distby=None, headerline=False):
        if not self.create_truncate_table(tbl=tbl, tbl_str=tbl_str, distby=distby):
            return False
        if headerline:
            query=f"copy {tbl} from stdin header"
        else:
            query=f"copy {tbl} from stdin"
        try:
            self.local_import(query, StringIO('\n'.join(datalist)))
            return True
        except:
            logger.error(f"{traceback.format_exc()}")
        return False

        
def main(arg=None):
    psc = PostgresqlUtils()
    rst = psc.conn.execute('set search_path=wdinfo')
    rst = psc.conn.execute('select count(*) from sinnud')
    rst = psc.conn.execute('drop table sinnud')
    if not psc.create_truncate_table(tbl='sinnud', tbl_str='name text', distby='name'):
        return False
    with open('requirement.txt', 'r') as f:
        f_str=f.read()
    flist_=f_str.split('\n')
    flist=list(filter(None, flist_))
    logger.info(flist)
    if not psc.import_datalist(datalist=flist, tbl='sinnud', tbl_str='name text', distby='name', headerline=False):
        return False
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