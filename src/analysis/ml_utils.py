import json, sys, errno, codecs, datetime
import numpy as np
from heapq import heappop, heappush
from pprint import pprint
from datetime import datetime
import time

def num_pageviews(session):
  return len([r for r in session if 'is_pageview' in r and r['is_pageview'] == 'true'])


def get_session_length(session):
  if len(session) < 2:
      return 0
  else:
      d1 = session[0]['ts']
      d2 = session[-1]['ts']
      
      # convert to unix timestamp
      d1_ts = time.mktime(d1.timetuple())
      d2_ts = time.mktime(d2.timetuple())

      # they are now in seconds, subtract and then divide by 60 to get minutes.
      return (d2_ts-d1_ts) / 60


def external_searches(session):
  return len([r for r in session if 'referer_class' in r and r['referer_class'] == 'external (search engine)'])



# This function measures the "DFS-likeness" of a tree:
# If a tree has a structure for which all valid traversals are identical, it returns NaN.a
# If a tree allows for several distinct traversals, the function returns a value between 0 and 1
# capturing from where in the open-list (a.k.a. fringe) of nodes the navigator picked the next node
# to be visited. A BFS will have a value of 0, a DFS, a value of 1. Intermediate values arise when
# the navigator behaves sometimes in a BFS, and sometimes in a DFS, way.
def dfsness(root):
  heap = [(root['ts'], 0, 0, root)]
  pos = []
  while len(heap) > 0:
    unique_phases = sorted(set([x[1] for x in heap]))
    phase_dict = dict(zip(unique_phases, range(len(unique_phases))))
    heap = [(x[0], phase_dict[x[1]], x[2], x[3]) for x in heap]
    #print [(x[1],x[3]['uri_path']) for x in heap]
    (time_next, phase_next, tb_next, node_next) = heappop(heap)
    phase = len(unique_phases)
    pos += [phase_next / (phase - 1.0) if phase > 1 else np.nan]
    # tb stands for 'tie-breaker'.
    tb = 0
    if 'children' in node_next:
      for ch in node_next['children']:
        heappush(heap, (ch['ts'], phase, tb, ch))
        tb += 1
  return np.nansum(pos) / (len(pos) - sum(np.isnan(pos)))

def get_tree_metrics_helper(tree):
  metrics = dict()
  if 'children' not in tree:
    metrics['size'] = 1
    metrics['size_sum'] = 1
    metrics['size_sq_sum'] = 1
    metrics['num_leafs'] = 1
    metrics['depth_max'] = 0
    metrics['depth_sum'] = 0
    metrics['degree_max'] = 0
    metrics['degree_sum'] = 0
    metrics['timediff_sum'] = 0
    metrics['timediff_min'] = np.nan
    metrics['timediff_max'] = np.nan
    metrics['ambiguous_max'] = 1 if 'parent_ambiguous' in tree.keys() else 0
    metrics['ambiguous_sum'] = 1 if 'parent_ambiguous' in tree.keys() else 0
  else:
    k = len(tree['children'])
    dt = tree['ts']
    metrics['size'] = 0
    metrics['size_sum'] = 0
    metrics['size_sq_sum'] = 0
    metrics['num_leafs'] = 0
    metrics['depth_max'] = 0
    metrics['depth_sum'] = 0
    metrics['degree_max'] = 0
    metrics['degree_sum'] = 0
    metrics['timediff_sum'] = 0
    metrics['timediff_min'] = np.inf
    metrics['timediff_max'] = -np.inf
    metrics['ambiguous_max'] = 0
    metrics['ambiguous_sum'] = 0
    for ch in tree['children']:
      child_metrics = get_tree_metrics_helper(ch)
      dt_child = ch['ts']
      timediff = (dt_child - dt).total_seconds()
      metrics['size'] += child_metrics['size']
      metrics['size_sum'] += child_metrics['size_sum']
      metrics['size_sq_sum'] += child_metrics['size_sq_sum']
      metrics['num_leafs'] += child_metrics['num_leafs']
      metrics['depth_max'] = max(metrics['depth_max'], child_metrics['depth_max'])
      metrics['depth_sum'] += child_metrics['depth_sum']
      metrics['degree_max'] = max(metrics['degree_max'], child_metrics['degree_max'])
      metrics['degree_sum'] += child_metrics['degree_sum']
      metrics['timediff_sum'] += child_metrics['timediff_sum'] + timediff
      metrics['timediff_min'] = min(metrics['timediff_min'], timediff)
      metrics['timediff_max'] = max(metrics['timediff_min'], timediff)
      metrics['ambiguous_max'] = max(metrics['ambiguous_max'], child_metrics['ambiguous_max'])
      metrics['ambiguous_sum'] += child_metrics['ambiguous_sum']
    metrics['size'] += 1
    metrics['size_sum'] += metrics['size']
    metrics['size_sq_sum'] += metrics['size'] * metrics['size']
    metrics['depth_max'] += 1
    metrics['depth_sum'] += metrics['depth_max']
    metrics['degree_sum'] += k
    metrics['degree_max'] = max(metrics['degree_max'], k)
    metrics['ambiguous_max'] = max(metrics['ambiguous_max'], 1 if 'parent_ambiguous' in tree.keys() else 0)
    metrics['ambiguous_sum'] += 1 if 'parent_ambiguous' in tree.keys() else 0

  return metrics

def get_tree_metrics(tree):
  metrics = get_tree_metrics_helper(tree)
  metrics['mean_time_diff'] = metrics['timediff_sum'] / max(1, metrics['degree_sum'])
  metrics['dfsness'] = dfsness(tree)
  return metrics

