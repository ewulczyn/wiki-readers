from db_utils import execute_hive_expression,get_hive_timespan
import os
import dateutil
from IPython.display import clear_output
import hmac
import hashlib
import pymysql
from db_utils import mysql_to_pandas
from pyspark.sql import Row


##########  HIVE Traces ###################

def exec_hive(query, priority = False):
    if priority:
        query = "SET mapreduce.job.queuename=priority;" + query
    cmd = """hive -e \" """ +query+ """ \" """ 
    os.system(cmd)


def create_hive_trace_table(db_name, table_name, local = True):
    """
    Create a Table partitioned by day and host
    """
    query = """
    CREATE TABLE IF NOT EXISTS %(db_name)s.%(table_name)s (
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

    if local:
        execute_hive_expression(query % params)
    else: 
        exec_hive(query % params)


def add_day_to_hive_trace_table(db_name, table_name, day, local = True, priority = True):

    query = """
    INSERT OVERWRITE TABLE %(db_name)s.%(table_name)s
    PARTITION(year=%(year)d, month=%(month)d, day =%(day)d, host)
    SELECT
        client_ip,
        user_agent,
        x_forwarded_for,
        CONCAT_WS('||', COLLECT_LIST(request)) as requests,
        uri_host as host
    FROM
        (SELECT
            client_ip,
            user_agent,
            x_forwarded_for,
            CONCAT(ts, '|', referer, '|', uri_path ) request,
            uri_host
        FROM
            wmf.webrequest
        WHERE 
            is_pageview
            AND agent_type = 'user'
            AND %(time_conditions)s
            AND uri_host in ('en.wikipedia.org', 'en.m.wikipedia.org')
        ) a
    GROUP BY
        client_ip,
        user_agent,
        x_forwarded_for,
        uri_host
    """

    day_dt = dateutil.parser.parse(day)

    params = {  'time_conditions': get_hive_timespan(day, day, hour = False),
                'db_name': db_name,
                'table_name': table_name,
                'year' : day_dt.year,
                'month': day_dt.month,
                'day': day_dt.day
                }

    if local:
        execute_hive_expression(query % params, priority = priority)
    else: 
        exec_hive(query % params, priority = priority)


############ Hash IPs in Spark #############################

"""
HIVE SCHEMA:
IP, UA, XFF, requests

"""

########## Join Traces and Clicks ##########################
def query_db(host,query, params):
    conn = pymysql.connect(host =host, read_default_file="/etc/mysql/conf.d/analytics-research-client.cnf")
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return mysql_to_pandas(rows)


def get_all_clicks():
    """
    Returns df of "Yes" clicks on the survey
    """
    query = """
    SELECT *
    FROM
        log.QuickSurveysResponses_15266417 r,
        log.QuickSurveyInitiation_15278946 i
    WHERE
        r.event_surveyInstanceToken = i.event_surveyInstanceToken
        AND i.event_eventName ='impression'
        AND event_surveyResponseValue ='ext-quicksurveys-external-survey-yes-button'
    """ 

    return query_db('analytics-store.eqiad.wmnet', query, {})    


def get_clicks(day, host):
    start_mw = dateutil.parser.parse(day).strftime('%Y%m%d') + '000000'
    stop_mw = dateutil.parser.parse(day).strftime('%Y%m%d') + '235959'

    d_click = get_all_clicks()
    d_click_reduced = d_click[d_click['timestamp'] < stop_mw]
    d_click_reduced = d_click_reduced[d_click_reduced['timestamp'] > start_mw]
    d_click_reduced = d_click_reduced[d_click_reduced['webHost'] == host]
    return d_click_reduced

def get_click_rdd(sc, day, host):
    fields = ['event_pageId', 'event_pageTitle', 'event_surveyInstanceToken', 'timestamp', 'i.timestamp']
    d_click = get_clicks(day, host)
    d_click_list = []
    for i, r in d_click.iterrows():
        key = r['clientIp'] + r['userAgent'][1:-1]
        data = dict(r[fields])
        d_click_list.append((key, data))
    return sc.parallelize(d_click_list)

def get_click_df(sc, sqlContext, day, host, name):
    click_rdd = get_click_rdd(sc, day, host)
    click_row_rdd = click_rdd.map(lambda x: Row(key=x[0], click_data=x[1])) 
    clickDF = sqlContext.createDataFrame(click_row_rdd)
    clickDF.registerTempTable(name)
    return clickDF

def get_file_name(day, host, table):
    day_dt = dateutil.parser.parse(day)

    params = {
        'year' : day_dt.year,
        'month' : day_dt.month,
        'day' : day_dt.day,
        'host' : host,
        'table' : table,
    }
    fname = '%(table)s/year=%(year)d/month=%(month)d/day=%(day)d/host=%(host)s' % params
    return fname

