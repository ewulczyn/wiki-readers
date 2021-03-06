from pyspark import SparkConf, SparkContext
import json
import pandas as pd
import argparse
import hmac
import hashlib
import os


"""
Usage: 

spark-submit \
    --driver-memory 1g \
    --master yarn \
    --deploy-mode client \
    --num-executors 20 \
    --executor-memory 5g \
    --executor-cores 4 \
    --queue priority \
/home/ellery/readers/src/data_generation/hash_trace_ips.py \
    --name rs3v3 \
    --key 

"""

def parse_hive_struct(s):
    d = {}
    for e in s.split('\x02'):
        if '\x03' in e:
            k,v = e.split('\x03')
            d[k] = v
    return d

def parse_row(line):
    row = line.strip().split('\t')
    if len(row) !=5:
        return None
    
    d = {'ip': row[0],
         'ua': row[1],
         'geo_data' : parse_hive_struct(row[2]),
         'ua_data' : parse_hive_struct(row[3]),
         'requests' : parse_requests(row[4])
        }
    return d


def parse_requests(requests):
    ret = []
    for r in requests.split('REQUEST_DELIM'):
        t = r.split('|')
        if (len(t) % 2) != 0: # should be list of (name, value) pairs and contain at least id,ts,title
            continue
        data_dict = {t[i]:t[i+1] for i in range(0, len(t), 2) }
        ret.append(data_dict)
    ret.sort(key = lambda x: x['ts']) # sort by time
    return ret



if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--name', required=True, 
        help='where to put hashed traces'
    )

    parser.add_argument(
        '--key', required=True, 
        help='hash key'
    )


    args = parser.parse_args()

    input_dir = '/user/hive/warehouse/traces.db/' + args.name
    output_dir = '/user/ellery/readers/data/hashed_traces/' + args.name
    key = args.key.strip()
    
    conf = SparkConf()
    conf.set("spark.app.name", 'Hash Trace IPs')
    sc = SparkContext(conf=conf, pyFiles=[])

    def hash_ip(x):
        ip = x['ip']
        code = hmac.new(key.encode('utf-8'), ip.encode('utf-8'), hashlib.sha1)
        hashed_ip = code.hexdigest()
        x['ip'] = hashed_ip
        return x

    
    trace_rdd = sc.textFile(input_dir) \
        .map(parse_row) \
        .filter(lambda x: x is not None) \
        .map(hash_ip) \
        .map(lambda x: json.dumps(x))         
    os.system('hadoop fs -rm -r ' + output_dir)
    trace_rdd.saveAsTextFile(output_dir)
