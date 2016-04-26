import argparse
import pandas as pd
import os
import sys
import inspect
from trace_utils import create_hive_trace_table

"""
USAGE:
python create_hive_traces.py \
--start 2016-03-01 \
--stop 2016-03-01 \
--db traces \
--table test \
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
    create_hive_trace_table(args.db, args.table, args.start, args.stop, local = False, priority = args.priority)