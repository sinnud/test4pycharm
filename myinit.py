import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import mysql.connector # test mysql connection
import glob

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

import csv

from datetime import datetime

def show_photo(file=None):
    img=mpimg.imread(file)
    imgplot = plt.imshow(img)
    plt.show()

def get_photo_info(folder=None, csvfile=None):
    stack = list()  # FIFO data-type
    stack.append(folder)

    with open(csvfile, 'w') as f:
        csvw=csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator='\n')
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
                    #logger.debug(f"The {idx}-th file {fn} under {path} with extension {file_extension[1:]} size: {st.st_size} created at {st.st_ctime} or {dt_fmt}.")
                    csvw.writerow([fn, path, file_extension[1:], file, st.st_size, dt_fmt])
                else:
                    logger.info(f"Not file or folder: {file}...({idx})")
    return True

def mysql_showdb(mydb=None):
    mycursor=mydb.cursor()
    mycursor.execute("show databases")
    for x in mycursor:
        logger.info(x)

def mysql_csv2db(mydb=None, csvfile=None, tblnm=None):
    mycursor=mydb.cursor()
    mycursor.execute(f"drop table if exists {tblnm}")
    for x in mycursor:
        logger.info(x)
    logger.debug(f"Finish dropping {tblnm}")
    query=f"create table {tblnm} (filename varchar(100), folder varchar(100)"
    query=f"{query}, file_type varchar(20), fullpath varchar(400)"
    query=f"{query}, filesize bigint, createtime timestamp)"
    mycursor.execute(query)
    for x in mycursor:
        logger.info(x)
    logger.debug(f"Finish creating {tblnm}")
    thisquery=f"LOAD DATA LOCAL INFILE '{csvfile}'"
    thisquery=f"{thisquery}\n INTO TABLE {tblnm}"
    thisquery=f"{thisquery}\n FIELDS TERMINATED BY ','"
    qt='"'
    thisquery=f"{thisquery}\n ENCLOSED BY '{qt}'"
    nl='\\n'
    thisquery=f"{thisquery}\n LINES TERMINATED BY '{nl}'"
    logger.info(f"Query:\n {thisquery}")
    mydb.get_warnings=True
    mycursor.execute(thisquery)
    for x in mycursor:
        logger.info(x)
    wrn=mycursor.fetchwarnings()
    logger.info(f"Warning: {wrn}")
    mydb.commit()
    logger.debug(f"Finish load csv data to {tblnm}")

def main(arg=None):
    photo_dir="/mnt/photos/art"
    csvfile='/tmp/photo.csv'
    get_photo_info(folder=photo_dir, csvfile=csvfile)
    #show_photo(file="/mnt/photos/chico/2015-11-08 15.15.57.jpg")

    try:
        mydb=mysql.connector.connect(host="localhost", user="sinnud", passwd="Jeffery45!@"
                                     , database="test", allow_local_infile=True
                                     )
        #mysql_showdb(mydb=mydb)
        mysql_csv2db(mydb=mydb, csvfile='/tmp/photo.csv', tblnm='photo_info')
        mydb.close()
    except:
        logger.debug(traceback.format_exc())
    os.remove(csvfile)
    logger.info(f"Deleted file {csvfile}.")
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
