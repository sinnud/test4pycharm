"""
Demo standalone
===============

* **Program file**: gap_test.py
* **Client**      : demo using sample JSON data file
* **Updates**     :

The purpose of this file is to demostrate importing data files to database.

Run this test under upper folder of `tests`

`python -B -m unittest test.test.test_map_gen`

 or

`python -B -m unittest test.test.test_parse`

 etc.

* The postgresql.sql is used to connect to database

Common tests
------------
* **test_map_gen** will do the following:

  * load into **json_data**
  * Compute all paths in data
  * Generate map (table_plan_json)
  * export to JSON format map file
  * generate postgres DDL SQL query to file

* **test_map2csv** will do the following:

  * Import JSON format map file
  * Export to CSV format map file
  * Then user can modify CSV format map file as needed

* **test_csv2map** will do the following:

  * Import CSV format map file
  * generate postgres DDL SQL query to file (based on user changedd map)
  * Export to JSON format map file
  * Later user can parse data using this new map and import to database

* **test_parse** will do the following:

  * Decrypt data file
  * load into **json_data**
  * Import JSON format map file
  * Parse JSON data based on map
  * Import parsed data into database (assume tables in database has been created using DDL)

The python functions
--------------------
"""
import os
import csv
from datetime import datetime
import traceback # Python error trace
import logzero
from logzero import logger

from json_utils.json_utils import JsonUtils
from json_utils.sql import Sql

def get_db_conn(box=1, db='dev'):
    """ define greenplum connection using Sql under appriss_sas
    """
    return Sql(f"gp{box}", db)

def flush_to_db(ju=None, db_conn=None, schema=None, truncate_before_flush=True):
    """ flush data from memory to greenplum tables
    """
    for tbl_map in ju.map["tableList"]:
        tbl = tbl_map["tableName"]
        if truncate_before_flush:
            qry = f"truncate table {schema}.{tbl}"
            db_conn.sql_execute_with_replace(qry)
        qry = f"copy {schema}.{tbl} FROM STDIN with DELIMITER '|'"
        from io import StringIO
        db_conn.import_from_file(qry, StringIO('\n'.join(ju.parsed_tables[tbl])))
    pass

def test_map_gen():
    """ test function: generate map based on data
    """
    crt_dir = os.path.dirname(__file__)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_fn = os.path.basename(__file__).replace('.py', '')
    # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
    mylog = f"{crt_dir}/{script_fn}.log"
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    # data_prepare()
    json_df = f"{crt_dir}/data/datafile.json"
    with open(json_df, 'r') as f:
        json_data = f.read()
    logger.debug(f"raw data string length: {len(json_data)}")
    ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')
    ju.load_from_string(jstr = json_data)

    ju.compute_all_paths()
    ju.table_plan_json()
    ju.json_map_export(map_file = f"{crt_dir}/map/ex_init.map")
    ju.postgres_ddl(sql_file = f"{crt_dir}/map/ex_init.sql", schema_name = 'work_ld')

    logger.info(f'end python code {__file__}.\n')


def test_parse():
    """ test function: parse JSON data based on map; import into database
    """
    crt_dir = os.path.dirname(__file__)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_fn = os.path.basename(__file__).replace('.py', '')
    # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
    mylog = f"{crt_dir}/{script_fn}.log"
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    # data_prepare()
    json_df = f"{crt_dir}/data/datafile.json"
    with open(json_df, 'r') as f:
        json_data = f.read()
    logger.debug(f"raw data string length: {len(json_data)}")
    ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')
    ju.load_from_string(jstr = json_data)

    ju.json_map_import(map_file = f"{crt_dir}/map/ex_init.map")
    ju.parse_to_csv()
    # flush to db?
    flush_to_db(ju=ju, gp_conn=get_db_conn(box=1,db='dev'), schema='work_ld')

    logger.info(f'end python code {__file__}.\n')

def test_map2csv():
    """ test function: import map to CSV file for user revising
    """
    crt_dir = os.path.dirname(__file__)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_fn = os.path.basename(__file__).replace('.py', '')
    # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
    mylog = f"{crt_dir}/{script_fn}.log"
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')

    ju.json_map_import(map_file = f"{crt_dir}/map/ex_init.map")
    ju.map_export_csv(map_csv = f"{crt_dir}/map/ex_init.csv")
    logger.info(f'end python code {__file__}.\n')


def test_csv2map():
    """ test function: import CSV format map
    """
    crt_dir = os.path.dirname(__file__)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_fn = os.path.basename(__file__).replace('.py', '')
    # mylog = f"{crt_dir}/{script_fn}_{timestamp}.log"
    mylog = f"{crt_dir}/{script_fn}.log"
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    ju = JsonUtils(csv_delim='|', table_name_prefix='ex_')

    ju.map_import_csv(map_csv = f"{crt_dir}/map/ex_v001.csv")
    ju.postgres_ddl(sql_file = f"{crt_dir}/map/ex_v001.sql", schema_name = 'work_ld')
    ju.json_map_export(map_file = f"{crt_dir}/map/ex_v001.map")
    logger.info(f'end python code {__file__}.\n')
