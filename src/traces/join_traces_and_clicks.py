from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from trace_utils import get_click_df, get_clicks
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
    --executor-memory 20g \
    --executor-cores 4 \
    --queue priority \
join_traces_and_clicks.py \
    --input_dir /user/ellery/readers/data/hashed_traces/test \
    --output_dir /home/ellery/readers/data/click_traces/test 
"""

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_dir', required=True, 
        help='path to hive table with raw traces'
    )

    parser.add_argument(
        '--output_dir', required=True, 
        help='where to put hashed traces'
    )

    
    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir
    
    conf = SparkConf()
    conf.set("spark.app.name", 'Join Traces and Clicks')
    sc = SparkContext(conf=conf, pyFiles=[])
    sqlContext = SQLContext(sc)


    d_clicks = get_clicks()

    
    trace_rdd = sc.textFile(input_dir) \
        .map(lambda x: json.loads(x)) \
        .filter(lambda x: len(x) == 5) \
        .map(lambda x: Row(key=x['ip'] + x['ua'], requests=x['requests'], geo_data=x['geo_data'], ua_data=x['ua_data']))
    
    print(trace_rdd.take(1))

    traceDF = sqlContext.createDataFrame(trace_rdd)
    traceDF.registerTempTable("traceDF")

    clickDF = get_click_df(sc, d_clicks, sqlContext, 'clickDF')

    query = """
    SELECT *
    FROM traceDF JOIN clickDF
    WHERE clickDF.key = traceDF.key
    """
    res = sqlContext.sql(query).collect()

    click_traces = []
    for row in res:
        d = {'key': row.key, 'requests': row.requests, 'click_data':row.click_data, 'geo_data': row.geo_data, 'ua_data' : row.ua_data}
        click_traces.append(d)

    json_outfile = os.path.join(output_dir, 'join_data.json')
    tsv_outfile = os.path.join(output_dir, 'join_data.tsv')

    try:
        os.makedirs( output_dir )
    except:
        print(traceback.format_exc())


    df = pd.DataFrame(click_traces)
    # keep only (request, click_data pairs) where the article clicked on is in the trace
    def article_in_trace(r):
        page = r['click_data']['title']
        return page in [e['title'] for e in r['requests']]

    df_clean = df[df.apply(article_in_trace, axis=1)].copy()
    df_clean['survey_token'] = df_clean['click_data'].apply(lambda x: x['survey_token'])
    # drop any rows with the same token.
    df_clean.drop_duplicates(inplace = True, subset = 'survey_token',  keep = False)

    df_clean.to_csv(tsv_outfile, sep = '\t', index=False)
    json.dump(click_traces, open(json_outfile, 'w'))

    nclicks = int(sqlContext.sql("SELECT COUNT(*) as n FROM clickDF ").collect()[0].n)
    joinSize = len(click_traces)
    cleanSize = df_clean.shape[0]
    status = '# Clicks: %d Join Size: %d Clean Size %d' % (nclicks, joinSize, cleanSize)
    print(status)
    sqlContext.dropTempTable('traceDF')
    sqlContext.dropTempTable('clickDF')











