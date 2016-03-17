import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from stats_utils import *
import copy

def get_std_err(num_events, num_trials):
    dist =  get_beta_dist(num_events, num_trials, num_samples = 5000)
    ci = bayesian_ci(dist, 95)
    return (ci[1]-ci[0]) / 2
def get_std_err_series(s):
    n = s.sum()
    return s.apply(lambda x: get_std_err(x, n))

def recode_host(h):
    if h == 'en.m.wikipedia.org':
        return 'mobile'
    if h == 'en.wikipedia.org':
        return 'desktop'
    else:
        return None
    

def plot_proportion(d, x, hue, title,  xorder = None, dropna_x = True, dropna_hue = True, rotate = False, normx = True):
    
    if dropna_x:
        d = d[d[x] != 'no response']
    else:
        xorder.append('no response')
        
    if dropna_hue:
        d = d[d[hue] != 'no response']
    
    if not normx:
        temp = x
        x = hue
        hue = temp
        
    d_in = pd.DataFrame({'count' : d.groupby( [x, hue] ).size()}).reset_index()
    d_in['err'] = d_in.groupby(hue).transform(get_std_err_series)
    d_in['proportion'] = d_in.groupby(hue).transform(lambda x: x/x.sum())['count']
    
    d_exp = pd.DataFrame()
    counts = d_in.groupby(hue)['count'].sum()
    for i, r in d_in.iterrows():
    
        n = counts[r[hue]]
        n1 = r['count']
        n0  =n - n1
        r_new = r[[x, hue]]
        r_new['in_data'] = 1
        d_exp = d_exp.append([r_new]*n1,ignore_index=True)
        r_new['in_data'] = 0
        d_exp = d_exp.append([r_new]*n0,ignore_index=True)
        
    if not normx:
        temp = x
        x = hue
        hue = temp
        
    fig = sns.barplot(
                x = x,
                y = 'in_data',
                data=d_exp,
                hue = hue,
                order = xorder,
                color = (0.54308344686732579, 0.73391773700714114, 0.85931565621319939)
                )
    plt.ylabel('proportion')
    plt.title(title)
    
    if rotate:
        plt.xticks(rotation=45) 

