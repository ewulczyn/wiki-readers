from db_utils import execute_hive_expression,get_hive_timespan
import os

def create_hive_trace_table(db_name, table_name):
    query = """
    DROP TABLE IF EXISTS %(db_name)s.%(table_name)s;
    CREATE TABLE %(db_name)s.%(table_name)s (
        ip STRING,
        ua STRING,
        xff STRING,
        requests STRING
    )
    PARTITIONED BY (year INT, month INT, day INT, host STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    """


    params = {'db_name': db_name, 'table_name': table_name}
    execute_hive_expression(query % params)



def exec_hive(query, priority = False):
    if priority:
        query = "SET mapreduce.job.queuename=priority;" + query
    cmd = """hive -e \" """ +query+ """ \" """ 

    print(cmd)
    os.system(cmd)