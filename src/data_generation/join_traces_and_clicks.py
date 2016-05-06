from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import pandas as pd
import argparse
import os
from pprint import pprint
import json
import traceback
from db_utils import query_db_from_stat


"""
Usage: 

spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 20 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
join_traces_and_clicks.py \
    --name rs3v3 
"""

def get_all_clicks():
    """
    Queries EL and returns a pandas df of data for "Yes" clicks
    on the survey
    """

    query = """
    SELECT
        webHost,
        page_title as title,
        event_surveyInstanceToken as survey_token,
        clientIp,
        userAgent,
        CAST(DATE_FORMAT(STR_TO_DATE(timestamp, '%%Y%%m%%d%%H%%i%%S'), '%%Y-%%m-%%d %%H:%%i:%%S') AS CHAR(22) CHARSET utf8) AS timestamp
    FROM
        log.QuickSurveysResponses_15266417 s,
        enwiki.page p
    WHERE
        p.page_id = s.event_pageId 
        AND s.event_surveyResponseValue ='ext-quicksurveys-external-survey-yes-button'
    """

    df =  query_db_from_stat('analytics-store.eqiad.wmnet', query, {})    
    df.sort_values(by='timestamp', inplace = True)
    df.drop_duplicates(subset = 'survey_token', inplace = True)
    return df


def get_click_rdd(sc, d_click):
    """
    Convert to RDD
    """
    fields = ['title', 'survey_token', 'timestamp'] #, 'i.timestamp']
    d_click_list = []
    for i, r in d_click.iterrows():
        try:
            key = r['clientIp'] + r['userAgent'][1:-1]
            data = dict(r[fields])
            d_click_list.append((key, data))
        except:
            print('ERROR')
            print(r)
            continue
    return sc.parallelize(d_click_list)


def get_click_df(sc, d_all_clicks, sqlContext, name):
    """
    Convert to Spark DataFrame
    """
    click_rdd = get_click_rdd(sc, d_all_clicks)
    click_row_rdd = click_rdd.map(lambda x: Row(key=x[0], click_data=x[1])) 
    clickDF = sqlContext.createDataFrame(click_row_rdd)
    clickDF.registerTempTable(name)
    return clickDF


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--name', required=True, 
        help='path to hive table with raw traces'
    )

    
    args = parser.parse_args()

    input_dir = '/user/ellery/readers/data/hashed_traces/' + args.name
    output_dir = '/home/ellery/readers/data/click_traces/' + args.name
    
    conf = SparkConf()
    conf.set("spark.app.name", 'Join Traces and Clicks')
    sc = SparkContext(conf=conf, pyFiles=[])
    sqlContext = SQLContext(sc)


    d_all_clicks = get_all_clicks()

    
    trace_rdd = sc.textFile(input_dir) \
        .map(lambda x: json.loads(x)) \
        .filter(lambda x: len(x) == 5) \
        .map(lambda x: Row(key=x['ip'] + x['ua'], requests=x['requests'], geo_data=x['geo_data'], ua_data=x['ua_data']))
    
    print(trace_rdd.take(1))

    traceDF = sqlContext.createDataFrame(trace_rdd)
    traceDF.registerTempTable("traceDF")

    clickDF = get_click_df(sc, d_all_clicks, sqlContext, 'clickDF')

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

    try:
        os.makedirs( output_dir )
    except:
        print(traceback.format_exc())


    json.dump(click_traces, open(json_outfile, 'w'))

    nclicks = int(sqlContext.sql("SELECT COUNT(*) as n FROM clickDF ").collect()[0].n)
    joinSize = len(click_traces)
    status = '# Clicks: %d Join Size: %d' % (nclicks, joinSize)
    print(status)
    

