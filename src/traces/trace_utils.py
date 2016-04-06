from db_utils import execute_hive_expression,get_hive_timespan
import os
import dateutil
from IPython.display import clear_output
import hmac
import hashlib
import pymysql
from db_utils import mysql_to_pandas
import pandas as pd
import ast
from db_utils import exec_hive_stat2 as exec_hive

##########  HIVE Traces ###################




def create_hive_trace_table(db_name, table_name, local = True):
    """
    Create a Table partitioned by day and host
    """
    query = """
    CREATE TABLE IF NOT EXISTS %(db_name)s.%(table_name)s (
        ip STRING,
        ua STRING,
        xff STRING,
        requests STRING,
        geocoded_data MAP<STRING,STRING>

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
        exec_hive_stat2(query % params)


def add_day_to_hive_trace_table(db_name, table_name, day, local = True, priority = True):

    query = """
    INSERT OVERWRITE TABLE %(db_name)s.%(table_name)s
    PARTITION(year=%(year)d, month=%(month)d, day =%(day)d, host)
    SELECT
        client_ip,
        user_agent,
        x_forwarded_for,
        CONCAT_WS('||', COLLECT_LIST(request)) AS requests,
        geocoded_data,
        uri_host AS host
    FROM
        (SELECT
            client_ip,
            user_agent,
            x_forwarded_for,
            CONCAT(ts, '|', referer, '|', title ) AS request,
            geocoded_data,
            uri_host
            FROM
                (SELECT
                    client_ip,
                    user_agent,
                    x_forwarded_for,
                    ts, 
                    referer,
                    geocoded_data,
                    uri_host,
                    CASE
                        WHEN rd_to IS NULL THEN raw_title
                        ELSE rd_to
                    END AS title
                FROM
                    (SELECT
                        client_ip,
                        user_agent,
                        x_forwarded_for,
                        ts, 
                        referer,
                        REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1) as raw_title,
                        geocoded_data,
                        uri_host
                    FROM
                        wmf.webrequest
                    WHERE 
                        is_pageview
                        AND webrequest_source = 'text'
                        AND agent_type = 'user'
                        AND %(time_conditions)s
                        AND uri_host in ('en.wikipedia.org', 'en.m.wikipedia.org')
                        AND LENGTH(REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1)) > 0
                    ) c
                LEFT JOIN
                    traces.en_redirect r
                ON c.raw_title = r.rd_from
                ) b
            ) a
    GROUP BY
        client_ip,
        user_agent,
        x_forwarded_for,
        geocoded_data,
        uri_host
    HAVING 
        COUNT(*) < 500
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

def get_hash_function_via_input():
    key = input("Please Provide the Hash Key:")
    clear_output()
    def hash_function(s):
        code = hmac.new(key.encode('utf-8'), s.encode('utf-8'), hashlib.sha1)
        s = code.hexdigest()
        return s
    return hash_function


def parse_row(line):
    row = line.strip().split('\t')
    if len(row) !=4:
        return None
    
    d = {'ip': row[0],
         'ua': row[1],
         'requests' : parse_requests(row[3])
        }
    return d

def parse_requests(requests):
    ret = []
    for r in requests.split('||'):
        t = r.split('|')
        if len(t) != 3:
            continue
        ret.append({'t': t[0], 'r': t[1], 'p': t[2]})
    ret.sort(key = lambda x: x['t']) # sort by time
    return ret

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

    query = """
    SELECT
        webHost,
        page_title as title,
        event_surveyInstanceToken as survey_token,
        clientIp,
        userAgent,
        CAST(DATE_FORMAT(STR_TO_DATE(timestamp, '%%Y%%m%%d%%H%%i%%S'), '%%Y-%%m-%%d %%H:%%i:%%S') AS CHAR(22) CHARSET utf8) AS timestamp
    FROM
        log.QuickSurveysResponses_15266417 s,
        enwiki.page p
    WHERE
        p.page_id = s.event_pageId 
        AND s.event_surveyResponseValue ='ext-quicksurveys-external-survey-yes-button'
    """

    df =  query_db('analytics-store.eqiad.wmnet', query, {})    
    df.sort_values(by='timestamp', inplace = True)
    df.drop_duplicates(subset = 'survey_token', inplace = True)
    return df


def get_clicks(d_all_clicks, day, host):
    start = dateutil.parser.parse(day).strftime('%Y-%m-%d') + ' 00:00:00'
    stop = dateutil.parser.parse(day).strftime('%Y-%m-%d') + ' 23:59:59'
    d_click_reduced = d_all_clicks[d_all_clicks['timestamp'] < stop]
    d_click_reduced = d_click_reduced[d_click_reduced['timestamp'] > start]
    d_click_reduced = d_click_reduced[d_click_reduced['webHost'] == host]
    return d_click_reduced

def get_click_rdd(sc, d_all_clicks, day, host):
    fields = ['title', 'survey_token', 'timestamp'] #, 'i.timestamp']
    d_click = get_clicks(d_all_clicks, day, host)
    d_click_list = []
    for i, r in d_click.iterrows():
        try:
            key = r['clientIp'] + r['userAgent'][1:-1]
            data = dict(r[fields])
            d_click_list.append((key, data))
        except:
            print('ERROR')
            print(r)
            continue
    return sc.parallelize(d_click_list)

def get_click_df(sc, d_all_clicks, sqlContext, day, host, name):
    from pyspark.sql import Row

    click_rdd = get_click_rdd(sc, d_all_clicks, day, host)
    click_row_rdd = click_rdd.map(lambda x: Row(key=x[0], click_data=x[1])) 
    clickDF = sqlContext.createDataFrame(click_row_rdd)
    clickDF.registerTempTable(name)
    return clickDF

def get_partition_name(day, host):
    day_dt = dateutil.parser.parse(day)

    params = {
        'year' : day_dt.year,
        'month' : day_dt.month,
        'day' : day_dt.day,
        'host' : host,
    }
    fname = 'year=%(year)d/month=%(month)d/day=%(day)d/host=%(host)s' % params
    return fname

def parse_geo(g):
    return dict( [ e.split('\x03') for e in g.split('\x02')])

def article_in_trace(r):
    page = r['click_data']['title']
    return page in [e['p'] for e in r['requests']]

# generate random sequence of times
def get_random_time_list():
    dt = parse_date('2016-03-01')
    times = [dt]
    for i in range(10):
        n = random.randint(0,60)
        times.append(times[-1] + datetime.timedelta(minutes=n))
    return times

def load_click_trace_data(version, directory = '/Users/ellerywulczyn/readers/data', start = '2016-03-01', stop = '2016-03-08'):
    dfs = []
    
    days = [str(day) for day in pd.date_range(start,stop)] 
    
    for host in ('en.wikipedia.org', 'en.m.wikipedia.org'):
        for day in days:
            partition = get_partition_name(day, host)
            dfs.append(pd.read_csv(os.path.join(directory, version, partition, 'join_data.tsv'), sep = '\t'))
    
    
    df =  pd.concat(dfs)
    df['geo_data'] = df['geo_data'].apply(lambda x: ast.literal_eval(x))
    df['requests'] = df['requests'].apply(lambda x: ast.literal_eval(x))
    df['click_data'] = df['click_data'].apply(lambda x: ast.literal_eval(x))
    return df



### a2v preprocess


