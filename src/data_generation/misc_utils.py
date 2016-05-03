import dateutil


"""
Functions that need to be shared across mul
"""

def get_partition_name(day, host):
    day_dt = dateutil.parser.parse(day)

    params = {
        'year' : day_dt.year,
        'month' : day_dt.month,
        'day' : day_dt.day,
        'host' : host,
    }
    fname = 'year=%(year)d/month=%(month)d/day=%(day)d/host=%(host)s' % params
    return fname