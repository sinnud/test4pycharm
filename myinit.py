import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import mysql.connector # test mysql connection
import glob

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

def show_photo(file=None):
    img=mpimg.imread(file)
    imgplot = plt.imshow(img)
    plt.show()

def get_photo_info(folder=None):
    filelist=glob.glob(f"{folder}/*.*")
    datalist=list()
    for idx, file in enumerate(sorted(filelist), start=1):
        logger.debug(f"The {idx}-th file {file}.")
    return True

def main(arg=None):
    get_photo_info(folder="/mnt/data/chico")
    show_photo(file="/mnt/data/chico/2015-11-08 15.15.57.jpg")
    try:
        mydb=mysql.connector.connect(host="localhost", user="sinnud", passwd="ld                                     u.zsh"
                                     , database="test"
                                     )
        logger.info(mydb)
        mycursor=mydb.cursor()
        mycursor.execute("show databases")
        for x in mycursor:
            logger.info(x)
        mydb.close()
    except:
        logger.debug(traceback.format_exc())
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
