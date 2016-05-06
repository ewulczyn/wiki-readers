import argparse
import pandas as pd
import os
import sys
import inspect
from db_utils import exec_hive_stat2, get_hive_timespan
import dateutil

"""
USAGE:
python create_hive_traces.py \
--start 2016-05-01 \
--stop 2016-05-01 \
--db traces \
--name test \
--priority
"""



def create_hive_trace_table(db_name, table_name, priority = True):
    """
    Create a Table partitioned by day and host
    """
    query = """
    CREATE TABLE IF NOT EXISTS %(db_name)s.%(table_name)s_by_day (
        ip STRING,
        ua STRING,
        geocoded_data MAP<STRING,STRING>,
        user_agent_map MAP<STRING,STRING>,
        requests STRING
    )
    PARTITIONED BY (year INT, month INT, day INT, host STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    """

    params = {'db_name': db_name, 'table_name': table_name}

    
    exec_hive_stat2(query % params, priority = priority)


def add_day_to_hive_trace_table(db_name, table_name, day, priority = True):

    query = """
    INSERT OVERWRITE TABLE %(db_name)s.%(table_name)s_by_day
    PARTITION(year=%(year)d, month=%(month)d, day =%(day)d, host)
    SELECT
        client_ip,
        user_agent,
        geocoded_data,
        user_agent_map,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(request)) AS requests,
        uri_host AS host
    FROM
        (SELECT
            client_ip,
            user_agent,
            geocoded_data,
            user_agent_map,
            CONCAT( 'ts|', ts,
                    '|referer|', referer,
                    '|title|', title,
                    '|uri_path|', reflect('java.net.URLDecoder', 'decode', uri_path),
                    '|uri_query|', reflect('java.net.URLDecoder', 'decode', uri_query),
                    '|is_pageview|', is_pageview,
                    '|access_method|', access_method,
                    '|referer_class|', referer_class,
                    '|project|', normalized_host.project_class,
                    '|lang|', normalized_host.project
                ) AS request,
            uri_host
        FROM
            (SELECT
                c.*,
                CASE
                    WHEN NOT is_pageview THEN NULL
                    WHEN rd_to IS NULL THEN raw_title
                    ELSE rd_to
                END AS title
            FROM
                (SELECT
                    w.*,
                    CASE
                        WHEN is_pageview THEN pageview_info['page_title']
                        ELSE round(RAND(), 5) 
                    END AS raw_title
                FROM
                    wmf.webrequest w
                WHERE 
                    webrequest_source = 'text'
                    AND agent_type = 'user'
                    AND %(time_conditions)s
                    AND hour = 1
                    AND access_method != 'mobile app'
                    AND uri_host in ('en.wikipedia.org', 'en.m.wikipedia.org')
                ) c
            LEFT JOIN
                traces.en_redirect r
            ON c.raw_title = r.rd_from
            ) b
        ) a
    GROUP BY
        client_ip,
        user_agent,
        geocoded_data,
        user_agent_map,
        uri_host
    HAVING 
        COUNT(*) < 500;
    """

    day_dt = dateutil.parser.parse(day)

    params = {  'time_conditions': get_hive_timespan(day, day, hour = False),
                'db_name': db_name,
                'table_name': table_name,
                'year' : day_dt.year,
                'month': day_dt.month,
                'day': day_dt.day
                }

    
    exec_hive_stat2(query % params, priority = priority)


def ungroup(db_name, table_name, priority = True):
    query = """
    CREATE TABLE %(db_name)s.%(table_name)s
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE AS
    SELECT
        ip,
        ua,
        geocoded_data,
        user_agent_map,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(requests)) AS requests
    FROM
        %(db_name)s.%(table_name)s_by_day
    WHERE
        year = 2016
    GROUP BY
        ip,
        ua,
        geocoded_data,
        user_agent_map
    """
    params = {'db_name': db_name, 'table_name': table_name}

    exec_hive_stat2(query % params, priority = priority)

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
        '--db', default='traces',
        help='hive db'
    )

    parser.add_argument(
        '--name', required=True, 
        help='hive table'
    )

    parser.add_argument(
        '--priority', default=False, action="store_true",
        help='hive table'
    )


    args = parser.parse_args()
    create_hive_trace_table(args.db, args.name, priority = args.priority)

    start = args.start 
    stop  = args.stop
    days = [str(day) for day in pd.date_range(start,stop)] 
    for day in days:
        print('Adding Traces From: ', day)
        add_day_to_hive_trace_table(args.db, args.name, day, priority = args.priority)
    ungroup(args.db, args.name, priority = args.priority)

