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


def create_hive_trace_table(db_name, table_name, start, stop, local = True, priority = True):

    query = """

    SET mapreduce.task.timeout=6000000;
    DROP TABLE IF EXISTS %(db_name)s.%(table_name)s_requests;
    CREATE TABLE %(db_name)s.%(table_name)s_requests
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    AS SELECT
        client_ip,
        user_agent,
        geocoded_data,
        user_agent_map,
        CONCAT( 'ts|', ts,
                '|referer|', referer,
                '|title|', title,
                '|uri_host|', uri_host,
                '|uri_path|', reflect('java.net.URLDecoder', 'decode', uri_path),
                '|uri_query|', reflect('java.net.URLDecoder', 'decode', uri_query),
                '|is_pageview|', is_pageview,
                '|access_method|', access_method,
                '|referer_class|', referer_class,
                '|project|', normalized_host.project_class,
                '|lang|', normalized_host.project
            ) AS request
    FROM
        (SELECT
            c.*,
            CASE
                WHEN NOT is_pageview THEN NULL
                WHEN rd_to IS NULL THEN raw_title
                ELSE rd_to
            END AS title
        FROM
            (SELECT
                w.*,
                CASE
                    WHEN is_pageview THEN pageview_info['page_title']
                    ELSE round(RAND(), 5) 
                END AS raw_title
            FROM
                wmf.webrequest w
            WHERE 
                webrequest_source = 'text'
                AND agent_type = 'user'
                AND %(time_conditions)s
                AND access_method != 'mobile app'
                AND uri_host in ('en.wikipedia.org', 'en.m.wikipedia.org')
            ) c
        LEFT JOIN
            traces.en_redirect r
        ON c.raw_title = r.rd_from
        ) b;
    
    SET mapreduce.task.timeout=6000000;
    DROP TABLE IF EXISTS %(db_name)s.%(table_name)s;
    CREATE TABLE %(db_name)s.%(table_name)s
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    AS SELECT
        client_ip,
        user_agent,
        geocoded_data,
        user_agent_map,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(request)) AS requests
    FROM
        %(db_name)s.%(table_name)s_requests
    GROUP BY
        client_ip,
        user_agent,
        geocoded_data,
        user_agent_map
    HAVING 
        COUNT(*) < 1000;
    """

    params = {  'time_conditions': get_hive_timespan(start, stop, hour = False),
                'db_name': db_name,
                'table_name': table_name,
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


def get_clicks():
    """
    Returns df of "Yes" clicks on the survey
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
    df.drop_duplicates(subset = ['clientIp', 'userAgent'], inplace = True)
    return df


def get_click_rdd(sc, d_click):
    fields = ['title', 'survey_token', 'timestamp'] 
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

def get_click_df(sc, d_clicks, sqlContext, name):
    from pyspark.sql import Row

    click_rdd = get_click_rdd(sc, d_clicks)
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

def load_click_trace_data(version, directory = '/Users/ellerywulczyn/readers/data/click_traces'):
    """
    Loads all join_data.tsvs for the given timespan into a single df.
    """
    df =  pd.read_csv(os.path.join(directory, version, 'join_data.tsv'), sep = '\t')
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
