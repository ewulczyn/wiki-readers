import argparse
import pandas as pd
import os
import sys
import inspect
from trace_utils import create_hive_trace_table, add_day_to_hive_trace_table

"""
USAGE:
python create_hive_traces.py \
--start 2016-03-01 \
--stop 2016-03-08 \
--db traces \
--table rs3v2 \
--priority
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
        '--db', default='traces',
        help='hive db'
    )

    parser.add_argument(
        '--table', required=True, 
        help='hive table'
    )

    parser.add_argument(
        '--priority', default=False, action="store_true",
        help='hive table'
    )


    args = parser.parse_args()


    create_hive_trace_table(args.db, args.table, local = False)

    start = args.start 
    stop  = args.stop
    days = [str(day) for day in pd.date_range(start,stop)] 
    for day in days:
        print('Adding Traces From: ', day)
        add_day_to_hive_trace_table(args.db, args.table, day, local = False, priority = args.priority)