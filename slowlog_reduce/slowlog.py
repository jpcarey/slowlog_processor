# -*- coding: utf-8 -*-


"""slowlog_reduce.slowlog: provides entry point main()."""


__version__ = "0.0.1"


from slowlog_reduce.line_processor import LineProcessor
from slowlog_reduce.printer import print_table
from . import result_caching
from . import es_indexer

from itertools import zip_longest
from contextlib import ExitStack
from collections import Counter, namedtuple
from glob import glob
import logging
import sys
import json
import pytz
import hashlib
import os
import fileinput
import argparse

import gzip

# TODO:
# filter should work for md5 and dates
#   maybe impliment to work for any column
# print order is by query hash, maybe customize?
# add def to print similiar queries where only a % of charcaters changed



logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

parser = argparse.ArgumentParser(
    description='script to check the number of fields in elasticsearch'
    )

parser.add_argument(
    '-p', '--paths',
    nargs='*',
    default=[
        '/Users/jared/Downloads/smartprocure_dv3_index_search_slowlog.log-4/**/*.log'
        ],
    help='paths / glob pattern to get slowlogs',
    required=False
    )

parser.add_argument(
    '--time_align',
    action='store',
    type=str,
    default=False,
    # default='/Users/jared/Downloads/diagnostics-20170216-151640/nodes_stats.json',
    help=''
    )

parser.add_argument(
    '--nocache',
    dest='cache',
    action='store_false'
    )
parser.set_defaults(cache=True)

parser.add_argument(
    '-c', '--columns',
    default=['md5','type','took','ts_min','extra_source','query'],
    help=''
    )

# parser.add_argument(
#     '-f', '--filter_ids',
#     action='store',
#     default=(),
#     # default=(
#     #     '25247852f8a774374834e896585b893e'
#     # ),
#     help='md5 hash of the query body to filter to'
#     )

parser.add_argument(
    '-tz', '--timezone',
    default='UTC',
    help='timezone used for all date parsing and math'
    )

args = parser.parse_args()


stats = Counter({
    'query': 0,
    'fetch': 0
})


TIMEZONE = pytz.timezone(args.timezone)

def Main():
    logger.info(args.paths)
    filenames = [logfile for path in args.paths
        for logfile in glob(path, recursive=True) if not os.path.isdir(logfile)]

    logger.debug(filenames)

    R = namedtuple('R', 'failures results metadata')

    cache, cache_filename, data = result_caching.read(args.cache,filenames)

    if not cache:
        check_nodestats(filenames)

        r = R(
            failures=LineProcessor.errors,
            results=read_logs(filenames),
            metadata=stats + LineProcessor.stats
        )
        result_caching.write(cache_filename, r._asdict())

    else:
        logger.debug('reading cache file {}'.format(cache_filename))
        data['metadata']['cache'] = cache
        r = R(
            failures=data['failures'],
            results=data['results'],
            metadata=data['metadata']
        )
        # print(r.failures)

    # TODO: write to elasticsearch (needs mappings to be fixed at some point)
    # es_indexer.index(r)

    print_table(args.columns, r)



def flatten_results(d):
    flat = []
    for md5 in d:
        for i in md5:
            if i['slowlog_type'] == 'query':
                stats['query'] += 1
            else:
                stats['fetch'] += 1
            flat.append(i)
    return flat


def read_logs(filenames):
    global TIMEZONE
    with ExitStack() as stack:
        # handles = [stack.enter_context(open(file)) for file in filenames]
        # for line in (line for tuple in zip_longest(*handles) for line in tuple if line):
        handles = [stack.enter_context(fileinput.input(files=file))
                    for file in filenames]

        for f,line in ((f,line) for lines in zip_longest(*handles)
                        for f,line in enumerate(lines) if line):

            logger.debug('{} {}'.format(
                handles[f].filelineno(),
                handles[f].filename()
                ))

            try:
                l = LineProcessor(
                    line.strip(),
                    handles[f].filelineno(),
                    handles[f].filename(),
                    TIMEZONE
                    )

            # moved everything inside line_processor
            # need to double exception / error handling
            except (ValueError, AttributeError):
                continue

    results = flatten_results(LineProcessor.aggs.values())
    return results


def check_slowlog():
    pass



def check_nodestats(slowlog_files):
    """
    This tries to automatically detect if the slowlogs being read are from a
    support diagnostic. If they are, try read the node_stats.json to use for
    automatically applying time offsets based upon time difference to the master
    node.
    """
    if args.time_align and os.path.isfile(args.time_align):
        LineProcessor.time_align(args.time_align)
    else:
        for item in slowlog_files:
            if 'diagnostics-' in item:
                regex = r'^.*diagnostics-.*?\/'
                m = re.search(regex, item)
                if m and os.path.isdir(m.group(0)):
                    nodes_stats = os.path.join(m.group(0), 'nodes_stats.json')
                    if os.path.isfile(nodes_stats):
                        LineProcessor.time_align(nodes_stats)
                        logger.debug('hello')
                        break
