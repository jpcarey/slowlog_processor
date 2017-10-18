#!/usr/bin/env python
from datetime import datetime
from itertools import zip_longest
from contextlib import ExitStack
import logging
import sys
import json
import re
import hashlib
import os
import glob
import argparse

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
        '/Users/jared/Downloads/smartprocure_dv3_index_search_slowlog.log/**/*.log'
        # '/Users/jared/Downloads/diagnostics-20170131-161624/172.16.0.20-log and config/logs/elastic-data-7oy4',
        # '/Users/jared/Downloads/smartprocure_dv3_index_search_slowlog-8_elastic-data-nodes_20170131-1150/*',
        # '/Users/jared/Downloads/smartprocure_dv3_index_search_slowlog-8_elastic-data-nodes_20170209-2238/*'
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
    '-c', '--columns',
    default=['md5','type','took','query'],
    help=''
)
parser.add_argument(
    '-f', '--filter_ids',
    action='store',
    default=(),
    # default=(
    #     '25247852f8a774374834e896585b893e',
    #     '89452558d66b72a9c7a094f08da15956',
    #     '3f251c6f58bdf731a05aa0fdbdce4338',
    #     'd384be0ba510fadaa5a0d1a6fe129976',
    #     '01ac10b0f4289122707cdc59f2e05d33',
    #     '5d9b2ed4e397460282c841ebdfc6ecf8',
    #     '9c1c86ca54c4404d2a3bfbeb7ac15b93',
    #     '0348b408cbaec21e4c008f9147e4b670',
    #     'e496936dbb832a484bb69165f651d20d',
    #     'a9b48e3a45b6d7fa4a700d228873d0b1',
    #     '4b25a3d05ab1e9a40863403ec977db82',
    #     '8b770f5ead8640c0382541eb3d1ab900',
    #     'b62da08a6041d1b5111df597a4a38192',
    #     '42f6dce623d5e025e9bf44840f8c35cd',
    #     '4f82259ced39702776a414390c13c671',
    #     '6c2e3be95105bc127f9404768a9eaf68'
    # ),
    help='md5 hash of the query body to filter to'
)
args = parser.parse_args()


class Agg(object):
    stats = {
        'grok_success': 0,
        'grok_failed': 0,
        'filtered': 0,
        'offsets': 0,
        'query': 0,
        'fetch': 0
    }
    offsets = dict()
    aggs = dict()

    def __init__(self, line):
        self.line = line
        self.grok(line)


    def grok(self, line):
        try:
            regex = re.compile(
                r'^\[(?P<datestamp>.*?)\]'
                r'\[(?P<loglevel>.*?)\]'
                r'\[index.search.slowlog.(?P<slowlog_type>.*?)\]\s?'
                r'\[(?P<host>.*?)\]\s?'
                r'\[(?P<index>.*?)\]'
                r'\[(?P<shard>\d+)\]\s?took'
                r'\[(?P<took>.*?)\],\s?took_millis'
                r'\[(?P<took_millis>\d+)\],\s?types'
                r'\[(?P<types>.*?)\],\s?stats'
                r'\[(?P<stats>.*?)\], search_type'
                r'\[(?P<search_type>.*?)\],\s?total_shards'
                r'\[(?P<total_shards>\d+)\],\s?source'
                r'\[(?P<query>.*?)\],\s?extra_source'
                r'\[(?P<extra_source>.*?)\],?'
            )
            match = re.search(regex, line)
            grokked = match.groupdict()
            grokked['md5'] = hashlib.md5((grokked['query']).encode('utf-8')).hexdigest()
            grokked['uid'] = grokked['md5'] + grokked['slowlog_type']
            grokked['datetime_object'] = datetime.strptime(grokked['datestamp'], '%Y-%m-%d %H:%M:%S,%f')
            grokked['ts_millis'] = int(grokked['datetime_object'].timestamp() * 1000)
            if Agg.offsets:
                grokked['ts_millis'] = grokked['ts_millis'] + Agg.offsets[grokked['host']]
                Agg.stats['offsets'] += 1
            Agg.stats['grok_success'] += 1
            for name, value in grokked.items():
                setattr(self, name, value)
        except AttributeError as e:
            Agg.stats['grok_failed'] += 1
            logger.warn("regex did not match")


    def filter(self, filter_ids):
        # called as log lines are iterated.
        try:
            # filter for query md5 values
            if self.md5 not in (filter_ids):
                Agg.stats['filtered'] += 1
                raise ValueError('message did not match filter')
        except NameError:
            pass


    def create_agg(self):
        # creates the base aggregated event
        event = {
            'ts': self.ts_millis,
            'ts_s': [self.ts_millis],
            'md5': self.md5,
            'datestamps': [self.datestamp],
            'loglevel': self.loglevel,
            'slowlog_type': self.slowlog_type,
            'hosts': [self.host],
            'index': self.index,
            'shards': [self.shard],
            'tooks': [self.took],
            'tooks_millis': [self.took_millis],
            'types': self.types,
            'stats': self.stats,
            'search_type': self.search_type,
            'total_shards': self.total_shards,
            'query': self.query,
            'extra_source': self.extra_source,
            'shards_n': 1,
            'tooks_min': self.took_millis,
            'tooks_max': self.took_millis,
            'ts_min': self.ts_millis,
            'ts_max': self.ts_millis
        }
        Agg.aggs.setdefault(self.uid, []).append(event)


    def append_agg(self, i):
        # TODO: check if shard number is already in agg?
        # (currently does the check in read_logs() )
        # move all logic into append
        Agg.aggs[self.uid][i]['ts_s'].append(self.ts_millis)
        Agg.aggs[self.uid][i]['datestamps'].append(self.datestamp)
        Agg.aggs[self.uid][i]['hosts'].append(self.host)
        Agg.aggs[self.uid][i]['shards'].append(self.shard)
        Agg.aggs[self.uid][i]['tooks'].append(self.took)
        Agg.aggs[self.uid][i]['tooks_millis'].append(self.took_millis)
        Agg.aggs[self.uid][i]['shards_n'] = len(Agg.aggs[self.uid][i]['shards'])
        Agg.aggs[self.uid][i]['tooks_min'] = min(Agg.aggs[self.uid][i]['tooks_millis'])
        Agg.aggs[self.uid][i]['tooks_max'] = max(Agg.aggs[self.uid][i]['tooks_millis'])
        Agg.aggs[self.uid][i]['ts_min'] = min(Agg.aggs[self.uid][i]['ts_s'])
        Agg.aggs[self.uid][i]['ts_max'] = max(Agg.aggs[self.uid][i]['ts_s'])
        # check if max number of shards has been reached
        # if int(aggs[self.uid][i]['shards_n']) == int(aggs[self.uid][i]['total_shards']):
        #     fin = aggs[self.uid].pop(i)
        #     done.setdefault(self.uid, []).append(fin)


    @staticmethod
    def time_align(file):
        # calculates timeoffsets from master node, sourced from nodes_stats diag
        with open(file) as json_data:
            d = json.load(json_data)
            master = dict(((k, v) for k, v in d["nodes"].items()
                          if v["attributes"]["master"] == 'true'))
            master_t = next(iter(master.values()))['os']['timestamp']
            diffs = dict()
            for k, v in d["nodes"].items():
                diffs[v['name']] = master_t - v['os']['timestamp']
            logger.debug(diffs)
            Agg.offsets = diffs
            # master node: nodes[].attributes.master = true
            # "nodes[].timestamp": 1487258197396,
            # "nodes[].os.timestamp": 1487258197396,
            # "nodes[].process.timestamp": 1487258197396,
            # "nodes[].jvm.timestamp": 1487258197397,
            # "nodes[].fs.timestamp": 1487258197397,


    def parse_date(s, fmts):
        # DATE_FORMATS = ['%m/%d/%Y %I:%M:%S %p', '%Y/%m/%d %H:%M:%S', '%d/%m/%Y %H:%M', '%m/%d/%Y', '%Y/%m/%d']
        # skeleton code for now, need to decided how I want to filter dates
        test_date = '2012/1/1 12:32:11'
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except ValueError:
                pass


    def __del__(self):
        pass


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


def read_logs():
    with ExitStack() as stack:
        handles = [stack.enter_context(open(file)) for file in filenames]
        for line in (line for tuple in zip_longest(*handles) for line in tuple if line):
            try:
                l = Agg(line.strip())
                if args.filter_ids:
                    l.filter(args.filter_ids)
                if l.uid in Agg.aggs:
                    nearest = dict()
                    for i, x in enumerate(Agg.aggs[l.uid]):
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


def check_slowlog():
    pass


def check_nodestats(slowlog_files):
    if args.time_align and os.path.isfile(args.time_align):
        Agg.time_align(args.time_align)
    # automatic check based upon slowlog paths
    else:
        for item in slowlog_files:
            if 'diagnostics-' in item:
                regex = r'^.*diagnostics-.*?\/'
                m = re.search(regex, item)
                if m and os.path.isdir(m.group(0)):
                    nodes_stats = os.path.join(m.group(0), 'nodes_stats.json')
                    if os.path.isfile(nodes_stats):
                        Agg.time_align(nodes_stats)
                        logger.debug('hello')
                        break


# def print_line(items = (), *args):
#     widths = []
#     for x in items:
#         widths.append(len(str(x))+2)
#     # fmt = '| {{:^{}s}} | {{:>{},.2f}} | {{:>{},d}} |'.format(*widths)
#     fmt = '{{:^{}}} {{:>{}}} {{:<{}}} {{:<{},.2f}} {{:<{}}}'.format(*widths)
#
#     logger.info(fmt.format(*items))


def printer():
    columns, rows = os.get_terminal_size(0)
    for key, value in Agg.aggs.items():
        for i in value:
            if i['slowlog_type'] == 'query':
                Agg.stats['query'] += 1
            else:
                Agg.stats['fetch'] += 1
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
    logger.debug(Agg.stats)


def column_format(name):
    c = {
        'md5': {'name': 'md5', 'align': '<', 'format': '{}'},
        'type': {'name': 'slowlog_type', 'align': '<', 'format': '{}'},
        # 'took': {'name': 'tooks_max', 'align': '>', 'format': '${:,.2f}'},
        'took': {'name': 'tooks_max', 'align': '>', 'format': '{}'},
        'query': {'name': 'query', 'align': '<', 'format': '{}'}
    }
    return c[name]



def print_table(columns, column_separator="   ", replace_space_char = None):
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
            5,
            column_alignment=c['align'],
            value_format=c['format']
        )

    # table.add_column('md5', 5, column_alignment='<')
    # table.add_column('type', 5, header_alignment='<', column_alignment='<')
    # table.add_column('took', 5, header_alignment='<', column_alignment='>')
    # table.add_column('query', 5, header_alignment='<')

    table.print_header()
    table.print_separator()

    # columns, rows = os.get_terminal_size(0)
    for md5 in Agg.aggs.values():
        for i in md5:
            if i['slowlog_type'] == 'query':
                Agg.stats['query'] += 1
            else:
                Agg.stats['fetch'] += 1

            # span = (max(i['ts_s']) - min(i['ts_s']))/1000
            # time = "%s   %s" % (min(i['datestamps'])[11:], max(i['datestamps'])[11:])
            # i['shards'] = [int(x) for x in i['shards']]
            # i['shards'].sort()

            # part1 = '{:6s} {:6s} {:2} {} {:>7.2f} {:>30}'.format(
            #                                             i['md5'][-5:],
            #                                             i['slowlog_type'],
            #                                             i['tooks_max'],
            #                                             i['shards_n'],
            #                                             span,
            #                                             time
            #                                             )
            truncated_query = i['query'][0:80]+'..'
            # truncated_query = i['query'] if (len(part1)+len(i['query'])) <= columns else i['query'][0:(columns-len(part1)-3)]+'..'
            # part2 = i['query'] + i['extra_source']
            # part2 = i['tooks']
            table.print_line(i['md5'][-5:], i['slowlog_type'], i['tooks_max'], truncated_query )
    logger.debug(Agg.stats)



def es_index():
    from elasticsearch import Elasticsearch, helpers
    es = Elasticsearch(
        ['http://elastic:changeme@localhost:9200/'],
    )

    k = ({'_type': 'my_type', '_index': 'slowlog', '_source': agg}
         for item in Agg.aggs.values() for agg in item)
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


if __name__ == "__main__":
    filenames = [logfile for path in args.paths for logfile in glob.glob(path, recursive=True) if not os.path.isdir(logfile)]
    logger.debug(filenames)
    check_nodestats(filenames)
    read_logs()
    # es_index()
    # printer()
    print_table(args.columns)
