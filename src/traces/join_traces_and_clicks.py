from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from trace_utils import get_partition_name, get_click_df
import pandas as pd
import argparse
import os
from pprint import pprint
import json
import traceback


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
join_traces_and_clicks.py \
    --start 2016-02-17 \
    --stop 2016-02-19 \
    --input_dir /user/ellery/readers/data/hashed_traces/test \
    --output_dir /home/ellery/readers/data/click_traces/test 
"""

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

    
    args = parser.parse_args()

    start = args.start 
    stop  = args.stop
    days = [str(day) for day in pd.date_range(start,stop)] 

    input_dir = args.input_dir
    output_dir = args.output_dir
    
    conf = SparkConf()
    conf.set("spark.app.name", 'Join Traces and Clicks')
    sc = SparkContext(conf=conf, pyFiles=[])
    sqlContext = SQLContext(sc)

    count_df = {}

    for host in ('en.wikipedia.org', 'en.m.wikipedia.org'):
        count_df[host] = {}
        for day in days:
            print('Processing', host,  day)
            partition = get_partition_name(day, host)
            input_partition = os.path.join(input_dir, partition)
            output_partition = os.path.join(output_dir, partition)

            trace_rdd = sc.textFile(input_partition) \
                .map(lambda x: json.loads(x)) \
                .filter(lambda x: len(x) == 3) \
                .map(lambda x: Row(key=x['ip'] + x['ua'], requests=x['requests']))
            

            traceDF = sqlContext.createDataFrame(trace_rdd)
            traceDF.registerTempTable("traceDF")

            clickDF = get_click_df(sc,sqlContext, day,host, 'clickDF')

            query = """
            SELECT *
            FROM traceDF JOIN clickDF
            WHERE clickDF.key = traceDF.key
            """
            res = sqlContext.sql(query).collect()

            click_traces = []
            for row in res:
                d = {'key': row.key, 'requests': row.requests, 'click_data':row.click_data}
                click_traces.append(d)

            outfile = os.path.join(output_partition, 'join_data.json')
            try:
                os.makedirs( output_partition )
            except:
                print(traceback.format_exc())

            json.dump(click_traces, open(outfile, 'w'))

            nclicks = int(sqlContext.sql("SELECT COUNT(*) as n FROM clickDF ").collect()[0].n)
            joinSize = len(click_traces)
            status = '# Clicks: %d Join Size: %d' % (nclicks, joinSize)
            print(status)
            count_df[host][day] = status
            sqlContext.dropTempTable('traceDF')
            sqlContext.dropTempTable('clickDF')


    pprint(count_df)









