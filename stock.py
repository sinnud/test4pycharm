import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import yfinance as yf # stock data

# local module
from mysql_con import MySqlConnect, Pandas2MySql

def main():
    # define the ticker symbol
    tickerSymbol = 'MSFT'

    # get data on this ticker
    tickerData = yf.Ticker(tickerSymbol)

    # get the historical prices for this ticker
    tickerDf = tickerData.history(period='1d', start='2010-1-1', end='2020-1-25')

    logger.info(tickerDf.shape)
    tblnm='ttt'
    try:
        mydb=MySqlConnect(database='stock')
        mycursor = mydb.cursor()
        mycursor.execute(f"drop table if exists {tblnm}")
        mydb.close()
        p2d=Pandas2MySql(pd=tickerDf, database='stock')
        p2d.pdimport(tblnm)
        p2d.close()
    except:
        logger.debug(traceback.format_exc())
        return False
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
