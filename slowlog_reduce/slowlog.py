# -*- coding: utf-8 -*-


"""slowlog_reduce.slowlog: provides entry point main()."""


__version__ = "0.0.1"


from slowlog_reduce.line_processor import LineProcessor
from . import result_cache

from datetime import datetime
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
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
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

    cache, cache_filename, data = result_cache.read(args.cache,filenames)

    if not cache:
        check_nodestats(filenames)

        r = R(
            failures=LineProcessor.errors,
            results=read_logs(filenames),
            metadata=stats + LineProcessor.stats
        )
        result_cache.write(cache_filename, r._asdict())

    else:
        logger.debug('reading cache file {}'.format(cache_filename))
        data['metadata']['cache'] = cache
        r = R(
            failures=data['failures'],
            results=data['results'],
            metadata=data['metadata']
        )
        # print(r.failures)
    # es_index()
    # printer()
    print_table(args.columns, r)



class PrettyTable():
    """Helper class to print pretty tables with proper alignment


    After initialising the class, one adds column definitions to
    the table by using add_column(). This method builds definitions
    to be used later when you start printing the table using:

      * print_header()
      * print_separator()
      * and multiple print_line()

    To customize the output of each column add formatting option to
    the add_column() statement. You can control width and alignment of
    both header and columns. Specify alignment with a single character:
      * left '<', center '=', and right '>'
      * header_alignment defaults to left '<'
      * column_alignment defaults to right '>'

    In addition you can specify the formatting of the actual column value
    using the value_format parameter which is applied before the column
    alignment.

    Default separator is a line consisting of '-' matching column widths,
    and a column separator of '   ' (three spaces). These can however
    be changed through the use of column_separator and replace_space_char.
    Try calling with column_separator=' | ', and replace_space_char='-'.
    """
    HEADER_PART = '{0:{1}{2}}'
    ROW_PART = '{{:{0}{1}}}'

    def __init__(self,
                 column_separator = '   ',
                 line_char = '-',
                 replace_space_char=None):

        self._column_separator = column_separator

        if replace_space_char == None:
            self._replaced_column_separator = column_separator
        else:
            self._replaced_column_separator = column_separator.replace(' ', replace_space_char)
        self._line_char = line_char

        self._header_text = []
        self._header_line = []
        self._row_format = []
        self._column_formats = []
        self._joined_row_format = None


    def add_column(self, title, width,
                   header_alignment='<',
                   column_alignment='>',
                   value_format = '{}'):
        """ Adds a new column to the table

        Extends the current header_text, header_line and
        row_format with proper text and alignment. Pushes the
        format to used for values in this column to column_formats for
        later use in print_line()
        """

        self._header_text.append(self.HEADER_PART
                                     .format(title,
                                             header_alignment,
                                             width))

        self._header_line.append(self.HEADER_PART
                                     .format(self._line_char * width,
                                             header_alignment,
                                             width))

        self._row_format.append(self.ROW_PART
                                    .format(column_alignment,
                                            width))

        self._column_formats.append(value_format)

    def print_header(self):
        """ Prints a header line, generated in add_column()"""
        print(self._column_separator.join(self._header_text))


    def print_separator(self):
        """ Prints a separator line, generated in add_column()"""
        print(self._replaced_column_separator.join(self._header_line))


    def print_line(self, *columns):
        """ Print a line in the table

        First we build a list of the formatted version of all column values,
        before printing this list using the row format
        """
        if self._joined_row_format == None:
            self._joined_row_format = self._column_separator.join(self._row_format)

        formatted_values = [self._column_formats[i].format(value)
                                 for (i, value) in enumerate(columns)]
        print(self._joined_row_format.format(*formatted_values))


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

                # if args.filter_ids:
                #     l.filter(args.filter_ids)

                if l.uid in LineProcessor.aggs:
                    nearest = dict()
                    for i, x in enumerate(LineProcessor.aggs[l.uid]):
                        # less than two items = +- 10s, more than two items use the max took time * 2
                        threshold = 10000 if len(x['tooks_max']) <= 2 else int(x['tooks_max'])*2
                        if l.ts_millis in range(x['ts_min']-threshold, x['ts_max']+threshold):
                            if l.shard in x['shards']:
                                continue
                            # # loop through array and build nearest matching agg
                            nearest[i] = min(x['ts_s'], key=lambda x: abs(x-l.ts_millis))
                    # shard is already in agg item, create new
                    if not nearest:
                        l.create_agg()
                    else:
                        match = min(nearest, key=lambda x: abs(x-l.ts_millis))
                        # check if match is in time window
                        l.append_agg(match)
                else:
                    l.create_agg()
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


def printer():
    columns, rows = os.get_terminal_size(0)
    for key, value in LineProcessor.aggs.items():
        for i in value:
            if i['slowlog_type'] == 'query':
                LineProcessor.stats['query'] += 1
            else:
                LineProcessor.stats['fetch'] += 1
            span = (max(i['ts_s']) - min(i['ts_s']))/1000
            time = "%s   %s" % (min(i['datestamps'])[11:], max(i['datestamps'])[11:])
            i['shards'] = [int(x) for x in i['shards']]
            i['shards'].sort()

            print_items = (
                i['md5'][-5:],
                i['slowlog_type'],
                i['shards_n'],
                span,
                time
            )
            # print_line(print_items)
            part1 = '{:6s} {:6s} {:2} {} {:>7.2f} {:>30}'.format(
                i['md5'][-5:],
                i['slowlog_type'],
                i['tooks_max'],
                i['shards_n'],
                span,
                time
            )
            # part2 = i['query'] if (len(part1)+len(i['query'])) <= columns else i['query'][0:(columns-len(part1)-3)]+'..'
            part2 = i['query'] + i['extra_source']
            # part2 = i['tooks']
            logger.info('{} {}'.format(part1, part2))
    logger.debug(LineProcessor.stats)


def column_format(name):
    c = {
        # 'took': {'name': 'tooks_max', 'align': '>', 'format': '${:,.2f}'},
        # 'ts': {'name': 'ts', 'align': '<', 'format': '{}'},
        'ts_s': {}, # list of timestamps in ms
        'md5': {'name': 'md5', 'width': 5, 'align': '<', 'format': '{}'}, # sort
        # 'datestamps': {},
        'loglevel': {},
        'type': {'name': 'slowlog_type', 'width': 5, 'align': '<', 'format': '{}'},
        'hosts': {}, # list of hosts, filter maybe, no sort
        'index': {}, # hmm. could there be individual entries per index?
        'shards': {}, # list of shards, filter? flat print
        'tooks': {}, # flat print
        'tooks_millis': {}, # flat print
        'types': {}, # flat print
        'stats': {}, # ???? why is this here?
        'search_type': {}, # sort
        'total_shards': {}, # sort
        'query': {'name': 'query', 'width': 5, 'align': '<', 'format': '{}'}, # nope, don't do it
        'extra_source': {'name': 'extra_source', 'width': 15, 'align': '<', 'format': '{}'}, # sort
        'shards_n': {}, # sort
        'tooks_min': {}, # sort
        'took': {'name': 'took', 'width': 5, 'align': '>', 'format': '{}'},
        'ts_min': {'name': 'ts_min', 'width': 12, 'align': '>', 'format': '{}'}, # sort
        'ts_max': {} # sort
    }
    return c[name]


def print_table(columns, data, column_separator="   ", replace_space_char = None):
    # http://codereview.stackexchange.com/questions/106156/using-changeable-column-formatting-to-display-table
    table = PrettyTable(
        column_separator = column_separator,
        replace_space_char = replace_space_char
    )

    cn = []
    for item in columns:
        c = column_format(item)
        cn.append(c['name'])

        table.add_column(
            item,
            c['width'],
            column_alignment=c['align'],
            value_format=c['format']
        )

    table.print_header()
    table.print_separator()

    # columns, rows = os.get_terminal_size(0)

    # x = results[0].items()
    # print(x)
    data.results.sort(key=lambda x: int(x['ts_min']), reverse=True)
    # print(json.dumps(results))
    for i in data.results:
    #     print(json.dumps(i))
        # truncated_query = i['query'][0:200]+'..'+str(len(i['query']))
        #truncated_query = i['query'] if (len(part1)+len(i['query'])) <= columns else i['query'][0:(columns-len(part1)-3)]+'..'
        # part2 = i['query'] + i['extra_source']
        # part2 = i['tooks']

        x = i['query'].split('highlight')[0]
        if '*' in x:
        # if int(i['tooks_max']) > 30000:
        # if any(x in i['query'] for x in ('wildcard', 'regexp')):
            table.print_line(
                i['md5'][-5:],
                i['slowlog_type'],
                i['tooks_max'],
                datetime.utcfromtimestamp(float(i['ts_min']/1000)).strftime("%H:%M:%S.%f")[:-3],
                (i['extra_source']+str(i['tooks'])),
                # x
                i['query']
                # truncated_query
            )
            # print('')
        # print(', '.join(map(str, i['ts_s'])))
        # print(', '.join(map(str, i['datestamps'])))
        # print(', '.join(map(str, i['shards'])))
    # logger.info(stats.__dict__)
    logger.info(data.metadata)


def es_index():
    from elasticsearch import Elasticsearch, helpers
    es = Elasticsearch(
        ['http://elastic:changeme@localhost:9200/'],
    )

    k = ({'_type': 'my_type', '_index': 'slowlog', '_source': item}
         for item in r.results)
    # print(list(k))

    # mapping = '''
    # {
    #   "mappings": {
    #     "my_type": {
    #       "properties": {
    #         "ip_addr": {
    #           "type": "ip"
    #         }
    #       }
    #     }
    #   }
    # }'''
    #
    # es.indices.create('test', ignore=400, body=mapping)

    helpers.bulk(es, k)
    print(es.count(index='slowlog'))
