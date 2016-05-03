from misc_utils import get_partition_name
import pandas as pd
from db_utils import query_analytics_store
import os
import json
import datetime

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


def find_click_request(row):
   
    trace = row.trace_data
    title = row.click_title
    click_dt = row.click_dt_utc
    click_request = None

    # number requests in trace
    for i, r in enumerate(trace):
        r['id'] = i
    
    eligible_requests = [r for r in trace if r['title'] == title and r['ts'] <= click_dt]

    if len(eligible_requests) == 0:
        row['click_request'] = None
        row['time_to_click'] = None
        return row

    times = [datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')  for r in eligible_requests]
    click_dt = datetime.datetime.strptime(click_dt, '%Y-%m-%d %H:%M:%S')
    deltas = [(click_dt-t).total_seconds() for t in times]

    min_index = deltas.index(min(deltas))
    
    row['click_request'] = eligible_requests[min_index]
    row['time_to_click'] = deltas[min_index]
    return row


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

    return pd.concat(dfs)

        


def join_survey_and_traces(d_survey_in_el, d_click_traces):

    #pull fields out of click data
    d_click_traces = d_click_traces.copy()
    d_click_traces['survey_token'] = d_click_traces['click_data'].apply(lambda x: x['survey_token'])
    d_click_traces['click_dt_utc'] = d_click_traces['click_data'].apply(lambda x: x['timestamp'])
    d_click_traces['click_title'] = d_click_traces['click_data'].apply(lambda x: x['title'])
    del d_click_traces['click_data']

    # join and clean
    df = d_survey_in_el.merge(d_click_traces, how = 'inner', right_on = 'survey_token', left_on = 'token')
    del df['token'] # duplicate data
    df.rename(columns={  'submit_timestamp': 'survey_submit_dt',
                         'key': 'client_token',
                         'requests': 'trace_data',
                        }, inplace=True)

    print('Join Size: ', df.shape[0])

    
    
    df = df.apply(find_click_request, axis=1)
    print('Has click_request: ', df.shape[0])

    df.dropna( inplace = True, subset = ['click_request'])
    df.sort_values(inplace = True, by ='time_to_click')
    df.drop_duplicates(inplace = True, subset = 'survey_token',  keep = False)
    df.sort_values(inplace = True, by ='time_to_click')
    df.drop_duplicates(inplace = True, subset = 'client_token',  keep = False)
    # only consider traces where survey was answered within an hour of request
    df = df.query('time_to_click < 3600')

    return df