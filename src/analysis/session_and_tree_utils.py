import pandas as pd
import ast
import datetime
import json
import copy
from urllib.parse import urlparse
import os




# Utils for turning a trace into sessions and sessions into trees




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


def get_click_session(row):
    click_request_id = row['click_request']['id']
    sessions = row['sessions']
    for session in sessions:
        for r in session:
            if r['id'] == click_request_id:
                row['click_session'] = session
                return row


def get_path_and_query(r):
    return os.path.join(r['uri_path'], r['uri_query'])
    
    
def get_referer_path_and_query(r):
    ref = urlparse(r['referer'])
    return os.path.join(ref.path, ref.query)



def getMinimumSpanningForest(trace):
    trace = copy.deepcopy(trace)
    roots = []
    pageCounts = {}
    pageToLastEvent = {}
    for i, event in enumerate(trace):
        ref = get_referer_path_and_query(event)
        
        parent = pageToLastEvent.get(ref, None)
        
        if parent is None:
            roots.append(event)
        else:
            children = parent.get('children', [])
            children.append(event)
            parent['children'] = children
            refererCount = pageCounts.get(ref, 0)
            
            if refererCount > 1:
                event['parent_ambiguous'] = True
                
        pageToLastEvent[get_path_and_query(event)] =  event
    return roots




def get_click_tree(row):
    forest = row['click_forest']
    click_request_id = row['click_request']['id']
    
    for tree in forest:
        stack = [tree,]
        while len(stack) > 0:
            node = stack.pop()
            if node['id'] == click_request_id:
                row['click_tree'] = tree
                return row
            elif 'children' in node:
                stack += node['children']



def get_sessions_and_trees(df):
    df['sessions'] = df['trace_data'].apply(sessionize)
    df = df.apply(get_click_session, axis=1)
    df['click_forest'] = df['click_session'].apply(getMinimumSpanningForest)
    df = df.apply(get_click_tree, axis=1)
    return df
