from pyspark import SparkConf, SparkContext
import json
from trace_utils import get_partition_name
import pandas as pd
import argparse
import hmac
import hashlib
import os


"""
Usage: 

spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
hash_trace_ips.py \
    --start 2016-03-01 \
    --stop 2016-03-08 \
    --input_dir /user/hive/warehouse/traces.db/rs3v2 \
    --output_dir /user/ellery/readers/data/hashed_traces/rs3v2 \
    --key 
"""

# WARNING THESE FUNCTIONS ARE DUPLICATED IN trace_utils.py
def parse_row(line):
    row = line.strip().split('\t')
    if len(row) !=5:
        return None
    
    d = {'ip': row[0],
         'ua': row[1],
         'requests' : parse_requests(row[3]),
         'geo_data' : row[4]
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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--start', required=True, 
        help='start day'
    )

    parser.add_argument(
        '--stop', required=True, 
        help='start day'
    )

    parser.add_argument(
        '--input_dir', required=True, 
        help='path to hive table with raw traces'
    )

    parser.add_argument(
        '--output_dir', required=True, 
        help='where to put hashed traces'
    )

    parser.add_argument(
        '--key', required=True, 
        help='hash key'
    )


    args = parser.parse_args()

    start = args.start 
    stop  = args.stop
    days = [str(day) for day in pd.date_range(start,stop)] 

    input_dir = args.input_dir
    output_dir = args.output_dir
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

    for host in ('en.wikipedia.org', 'en.m.wikipedia.org'):
        for day in days:
            print('Processing', host,  day)
            partition = get_partition_name(day, host)
            input_partition = os.path.join(input_dir, partition)
            output_partition = os.path.join(output_dir, partition )
            trace_rdd = sc.textFile(input_partition) \
                .map(parse_row) \
                .filter(lambda x: x is not None) \
                .map(hash_ip) \
                .map(lambda x: json.dumps(x))         
            os.system('hadoop fs -rm -r ' + output_partition)
            trace_rdd.saveAsTextFile(output_partition)














