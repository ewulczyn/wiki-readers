from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from misc_utils import get_partition_name
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
    --start 2016-03-01 \
    --stop 2016-03-08 \
    --input_dir /user/ellery/readers/data/hashed_traces/rs3v2 \
    --output_dir /home/ellery/readers/data/click_traces/rs3v2 
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


def reduce_clicks(d_all_clicks, day, host):
    """
    Reduce d_all_clicks, by filtering on day and host type
    """
    start = dateutil.parser.parse(day).strftime('%Y-%m-%d') + ' 00:00:00'
    stop = dateutil.parser.parse(day).strftime('%Y-%m-%d') + ' 23:59:59'
    d_click_reduced = d_all_clicks[d_all_clicks['timestamp'] < stop]
    d_click_reduced = d_click_reduced[d_click_reduced['timestamp'] > start]
    d_click_reduced = d_click_reduced[d_click_reduced['webHost'] == host]
    return d_click_reduced


def get_click_rdd(sc, d_all_clicks, day, host):
    """
    Reduce d_all_clicks by day and host and convert to RDD
    """
    fields = ['title', 'survey_token', 'timestamp'] #, 'i.timestamp']
    d_click = reduce_clicks(d_all_clicks, day, host)
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


def get_click_df(sc, d_all_clicks, sqlContext, day, host, name):
    """
    Reduce d_all_clicks by day and host and convert to Spark DataFrame
    """
    click_rdd = get_click_rdd(sc, d_all_clicks, day, host)
    click_row_rdd = click_rdd.map(lambda x: Row(key=x[0], click_data=x[1])) 
    clickDF = sqlContext.createDataFrame(click_row_rdd)
    clickDF.registerTempTable(name)
    return clickDF


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

    d_all_clicks = get_all_clicks()

    for host in ('en.wikipedia.org', 'en.m.wikipedia.org'):
        count_df[host] = {}
        for day in days:
            print('Processing', host,  day)
            partition = get_partition_name(day, host)
            input_partition = os.path.join(input_dir, partition)
            output_partition = os.path.join(output_dir, partition)

            print('Input Partition: ', input_partition)
            trace_rdd = sc.textFile(input_partition) \
                .map(lambda x: json.loads(x)) \
                .filter(lambda x: len(x) == 5) \
                .map(lambda x: Row(key=x['ip'] + x['ua'], requests=x['requests'], geo_data=x['geo_data'], ua_data=x['ua_data']))
            
            print(trace_rdd.take(1))

            traceDF = sqlContext.createDataFrame(trace_rdd)
            traceDF.registerTempTable("traceDF")

            clickDF = get_click_df(sc, d_all_clicks, sqlContext, day,host, 'clickDF')

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

            json_outfile = os.path.join(output_partition, 'join_data.json')

            try:
                os.makedirs( output_partition )
            except:
                print(traceback.format_exc())


            json.dump(click_traces, open(json_outfile, 'w'))

            nclicks = int(sqlContext.sql("SELECT COUNT(*) as n FROM clickDF ").collect()[0].n)
            joinSize = len(click_traces)
            status = '# Clicks: %d Join Size: %d Clean Size %d' % (nclicks, joinSize)
            print(status)
            count_df[host][day] = status
            

    pprint(count_df)


