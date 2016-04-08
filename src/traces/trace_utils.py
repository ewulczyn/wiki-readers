from db_utils import execute_hive_expression,get_hive_timespan,query_analytics_store,query_db_from_stat
from db_utils import exec_hive_stat2 as exec_hive
import os
import dateutil
from IPython.display import clear_output
import hmac
import hashlib
import pymysql
import pandas as pd
import ast
import datetime
import pytz, datetime, dateutil

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

    df =  query_db_from_stat('analytics-store.eqiad.wmnet', query, {})    
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



# generate random sequence of times
def get_random_time_list():
    dt = parse_date('2016-03-01')
    times = [dt]
    for i in range(10):
        n = random.randint(0,60)
        times.append(times[-1] + datetime.timedelta(minutes=n))
    return times


###### Join Click Traces and Survey Responses #####

def load_click_trace_data(version, directory = '/Users/ellerywulczyn/readers/data/click_traces', start = '2016-03-01', stop = '2016-03-08'):
    """
    Loads all join_data.tsvs for the given timespan into a single df.
    """
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




def recode_host(h):
    if h == 'en.m.wikipedia.org':
        return 'mobile'
    if h == 'en.wikipedia.org':
        return 'desktop'
    else:
        return None


def load_survey_dfs(survey_file = '../../data/responses.tsv'):

    """
    Returns complete survey data as well as survey data joinable to a yes click in EL
    """
    d_survey = pd.read_csv(survey_file, sep = '\t')

    query = """
    SELECT
        webHost AS host,
        event_surveyInstanceToken as token
    FROM 
        log.QuickSurveysResponses_15266417
    WHERE
        event_surveyResponseValue ='ext-quicksurveys-external-survey-yes-button'
        AND webHost in ('en.m.wikipedia.org', 'en.wikipedia.org')
    """

    d_click = query_analytics_store( query, {})
    d_click['host'] = d_click['host'].apply(recode_host)
    d_survey_in_el = d_survey.merge(d_click, how = 'inner', on = 'token')

    return d_survey, d_survey_in_el
    
def utc_to_local(dt, timezone):
    utc_dt = pytz.utc.localize(dt, is_dst=None)
    try:
        return  utc_dt.astimezone (pytz.timezone (timezone) )
    except:
        return None

def get_joined_survey_and_traces(d_traces, d_survey_in_el):
    """
    Join click traces and survey data in EL
    """
    dt = d_survey_in_el.merge(d_traces, how = 'inner', right_on = 'survey_token', left_on = 'token')
    dt['utc_click_dt'] = dt['click_data'].apply(lambda x: datetime.datetime.strptime (x['timestamp'], "%Y-%m-%d %H:%M:%S"))
    dt['local_click_dt'] = dt.apply(lambda x: utc_to_local(x['utc_click_dt'], x['geo_data']['timezone']), axis = 1)
    del dt['token']
    dt.rename(columns={  'submit_timestamp': 'survey_submit_dt',
                         'key': 'client_token',
                         'requests': 'trace_data',
                        },
              inplace=True)

    dt['utc_click_dt'] = dt['utc_click_dt'].apply(lambda x: datetime.datetime.strftime (x, "%Y-%m-%d %H:%M:%S"))
    dt['local_click_dt'] = dt['local_click_dt'].apply(lambda x:  datetime.datetime.strftime (x, "%Y-%m-%d %H:%M:%S") if x else x)


    return dt
