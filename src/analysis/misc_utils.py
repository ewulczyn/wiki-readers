import datetime
import pandas as pd

def parse_trace_ts(trace):
    clean = []
    for r in trace:
        try:
            r['ts'] = datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')
            clean.append(r)
        except:
            print(r)
            pass
    return clean


def load_raw_responses_with_traces(path = '../../data/responses_with_traces.tsv'):
    df = pd.read_csv(path, sep = '\t')
    df['geo_data'] = df['geo_data'].apply(eval)
    df['trace_data'] = df['trace_data'].apply(eval)
    df['ua_data'] = df['ua_data'].apply(eval)
    df['click_request'] = df['click_request'].apply(eval)
    df['click_dt_utc'] = df['click_dt_utc'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['survey_submit_dt'] = df['survey_submit_dt'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['trace_data'] = df['trace_data'].apply(parse_trace_ts)
    return df