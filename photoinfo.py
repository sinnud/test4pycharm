'''
Scan net drive photo, store photo info into mysql database
'''
import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import mysql.connector # test mysql connection
import glob

from datetime import datetime

import pandas as pd # import data through pandas to mysql

# local module
from mysql_con import MySqlConnect
from mysql_con import Pandas2MySql

def show_photo(file=None):
    img=mpimg.imread(file)
    imgplot = plt.imshow(img)
    plt.show()

def get_photo_info_pd(folder=None):
    datalist=list()
    stack = list()  # FIFO data-type
    stack.append(folder)
    while(len(stack)>0):
        thisdir=stack.pop(0)
        logger.info(f"Go through folder {thisdir}...")
        filelist=glob.glob(f"{thisdir}/*")
        for idx, file in enumerate(sorted(filelist), start=1):
            if os.path.isdir(file):
                logger.info(f"Folder {file} need to go through...({idx})")
                stack.append(file)
            elif os.path.isfile(file):
                path, fn=os.path.split(file)
                filename, file_extension = os.path.splitext(fn)
                st=os.stat(file)
                dt_fmt=datetime.fromtimestamp(st.st_ctime)
                datalist.append([fn, path, file_extension[1:], file, st.st_size, dt_fmt])
            else:
                logger.info(f"Not file or folder: {file}...({idx})")
    return pd.DataFrame(datalist)

def folder2table(folder=None, database=None, tbl=None):
    photo_dir=f"/mnt/photos/{folder}"
    mypd=get_photo_info_pd(folder=photo_dir)
    mypd.columns=['filename', 'folder', 'file_type', 'fullpath', 'filesize', 'createtime']
    #logger.info(mypd)
    logger.info("Try to connect mysql...")
    p2d=Pandas2MySql(pd=mypd, database=database)
    logger.info("Try to import to mysql...")
    p2d.pdimport(tbl)
    p2d.close()
    return True

def dropifexist(database=None, tbl=None):
    mydb=MySqlConnect(database=database)
    mycursor=mydb.cursor()
    mycursor.execute(f"drop table if exists {tbl}")
    mydb.close()

def main(arg=None):
    thisfolder='.'
    database='photoinfo'
    thistbl='phtall'
    dropifexist(database=database, tbl=thistbl)
    folder2table(folder=thisfolder, database=database, tbl=thistbl)
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
