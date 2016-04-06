import pandas as pd
from db_utils import query_analytics_store,query_hive_ssh
import pytz, datetime


def recode_host(h):
    if h == 'en.m.wikipedia.org':
        return 'mobile'
    if h == 'en.wikipedia.org':
        return 'desktop'
    else:
        return None


def load_survey_dfs():
    d_survey = pd.read_csv('../../data/clean_responses.tsv', sep = '\t')

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
    d_joined = d_survey.merge(d_click, how = 'inner', on = 'token')

    return d_survey, d_joined
    


def utc_to_local(dt, timezone):
    utc_dt = pytz.utc.localize(dt, is_dst=None)
    try:
        return  utc_dt.astimezone (pytz.timezone (timezone) )
    except:
        return None


def get_response_traces(d_traces, d_joined):
    dt = d_joined.merge(d_traces, how = 'inner', right_on = 'survey_token', left_on = 'token')
    dt['dt'] = dt['click_data'].apply(lambda x: datetime.datetime.strptime (x['timestamp'], "%Y-%m-%d %H:%M:%S"))
    dt['local_dt'] = dt.apply(lambda x: utc_to_local(x['dt'], x['geo_data']['timezone']), axis = 1)
    return dt