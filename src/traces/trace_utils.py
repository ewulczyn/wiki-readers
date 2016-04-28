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
import json

##########  HIVE Traces ###################


def create_hive_trace_table(db_name, table_name, local = True):
    """
    Create a Table partitioned by day and host
    """
    query = """
    CREATE TABLE IF NOT EXISTS %(db_name)s.%(table_name)s (
        ip STRING,
        ua STRING,
        geocoded_data MAP<STRING,STRING>,
        user_agent_map MAP<STRING,STRING>,
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
        geocoded_data,
        user_agent_map,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(request)) AS requests,
        uri_host AS host
    FROM
        (SELECT
            client_ip,
            user_agent,
            geocoded_data,
            user_agent_map,
            CONCAT( 'ts|', ts,
                    '|referer|', referer,
                    '|title|', title,
                    '|uri_path|', reflect('java.net.URLDecoder', 'decode', uri_path),
                    '|uri_query|', reflect('java.net.URLDecoder', 'decode', uri_query),
                    '|is_pageview|', is_pageview,
                    '|access_method|', access_method,
                    '|referer_class|', referer_class,
                    '|project|', normalized_host.project_class,
                    '|lang|', normalized_host.project
                ) AS request,
            uri_host
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
            ) b
        ) a

    GROUP BY
        client_ip,
        user_agent,
        geocoded_data,
        user_agent_map,
        uri_host
    HAVING 
        COUNT(*) < 500;
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


### Join Survey and Traces

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


def load_click_trace_data(version, directory = '/Users/ellerywulczyn/readers/data/click_traces', start = '2016-03-01', stop = '2016-03-08'):
    """
    Loads all join_data.tsvs for the given timespan into a single df.
    """
    dfs = []
    
    days = [str(day) for day in pd.date_range(start,stop)] 

    for host in ('en.wikipedia.org', 'en.m.wikipedia.org'):
        for day in days:
            partition = get_partition_name(day, host)
            path = os.path.join(directory, version, partition, 'join_data.json')
            df = pd.DataFrame(json.load(open(path)))
            dfs.append(df)

    df =  pd.concat(dfs)

    df_clean = df[df.apply(article_in_trace, axis=1)].copy()
    df_clean['survey_token'] = df_clean['click_data'].apply(lambda x: x['survey_token'])
    # drop any rows with the same token.
    df_clean.drop_duplicates(inplace = True, subset = 'survey_token',  keep = False)
    df_clean.drop_duplicates(inplace = True, subset = 'key',  keep = False)
    return df_clean


def article_in_trace(r):
    page = r['click_data']['title']
    return page in [e['title'] for e in r['requests']]


def parse_trace_ts(trace):
    clean = []
    for r in trace:
        try:
            r['ts'] = datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')
            clean.append(r)
        except:
            pass
    return clean


def trace_ts_to_str(trace):
    clean = []
    for r in trace:
        r['ts'] = datetime.datetime.strftime(r['ts'], '%Y-%m-%d %H:%M:%S')
        clean.append(r)
    return clean


def get_random_time_list():
    dt = parse_date('2016-03-01')
    times = [dt]
    for i in range(10):
        n = random.randint(0,60)
        times.append(times[-1] + datetime.timedelta(minutes=n))
    return times


def sessionize(trace, interval = 60):
    """
    Break trace whenever
    there is interval minute gap between requests
    """
    sessions = []
    session = [trace[0]]
    for r in trace[1:]:
        d = r['ts'] -  session[-1]['ts']
        if d > datetime.timedelta(minutes=interval):
            sessions.append(session)
            session = [r,]
        else:
            session.append(r)

    sessions.append(session)
    return sessions 

def get_click_session(r):
    
    title = r['click_title']
    sessions = [s for s in r['sessions'] if (title in [e['title'] for e in s])]
    click_ts = r['click_dt_utc']
    times = [get_candidate_time(click_ts, title, s) for s in sessions]
    min_index = times.index(min(times))
    r['click_session'] = sessions[min_index]
    
    return r

def get_candidate_time(click_ts, title, session):
    requests = [r for r in session if r['title'] == title]
    times = [abs((click_ts - r['ts']).total_seconds()) for r in requests]
    return min(times)


def load_responses_with_traces(path = '../../data/responses_with_traces.tsv'):
    df = pd.read_csv(path, sep = '\t', parse_dates = False)
    df['geo_data'] = df['geo_data'].apply(eval)
    df['trace_data'] = df['trace_data'].apply(eval)
    df['sessions'] = df['sessions'].apply(eval)
    df['click_session'] = df['click_session'].apply(eval)
    df['ua_data'] = df['ua_data'].apply(eval)
    df['click_dt_utc'] = df['click_dt_utc'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['survey_submit_dt'] = df['survey_submit_dt'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['trace_data'] = df['trace_data'].apply(parse_trace_ts)
    df['click_session'] = df['click_session'].apply(parse_trace_ts)
    return df