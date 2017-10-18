# -*- coding: utf-8 -*-
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
import re


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
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
parser.add_argument(
    '-tz', '--timezone',
    default='UTC',
    help='timezone used for all date parsing and math'
    )

args = parser.parse_args()

TIMEZONE = pytz.timezone(args.timezone)

stats = Counter({
    'query': 0,
    'fetch': 0
})

class LineProcessor():

    offsets = dict()
    aggs = dict()
    errors = []

    stats = Counter({
        'grok_success': 0,
        'grok_failed': 0,
        'offsets': 0
    })

    def __init__(self, line, lineno, file, timezone):
        self.line = line
        self.lineno = lineno
        self.file = file
        self.timezone = timezone
        self.grok(line)


    def grok(self, line):
        try:
            regex = re.compile(
                r'^\[(?P<datestamp>.*?)\]'
                r'\[(?P<loglevel>.*?)\]'
                r'\[index.search.slowlog.(?P<slowlog_type>.*?)\]\s?'
                r'\[(?P<index>.*?)\]'
                r'took\[(?P<took>.*?)\],\s?'
                r'took_millis\[(?P<took_millis>\d+)\],\s?'
                r'types\[(?P<types>.*?)\],\s?'
                r'stats\[(?P<stats>.*?)\], '
                r'search_type\[(?P<search_type>.*?)\],\s?'
                r'total_shards\[(?P<total_shards>\d+)\],\s?'
                r'source\[(?P<query>.*?)\],\s?'
                r'extra_source\[(?P<extra_source>.*?)\],?'
            )

            match = re.search(regex, line)
            grokked = match.groupdict()

            grokked['md5'] = hashlib.md5(
                (grokked['query']).encode('utf-8')
            ).hexdigest()

            grokked['uid'] = grokked['md5'] + grokked['slowlog_type']

            # timezone = pytz.timezone(args.timezone)

            grokked['datetime_object'] = self.timezone.localize(
                datetime.strptime(
                    grokked['datestamp'], '%Y-%m-%d %H:%M:%S,%f'
                    )
                )

            grokked['ts_millis'] = int(
                grokked['datetime_object'].timestamp() * 1000
                )

            LineProcessor.stats['grok_success'] += 1

            # expand named capture
            for name, value in grokked.items():
                setattr(self, name, value)

        except AttributeError as e:
            LineProcessor.stats['grok_failed'] += 1
            LineProcessor.errors.append({
                'error': 'regex did not match',
                'file': '{}:{}'.format(self.file, self.lineno),
                'line': self.line
                })
            logger.warn(
                "regex did not match. \n{}:{}\n{}".format(
                    self.file,
                    self.lineno,
                    self.line
                    ))


    def create_agg(self):
        # creates the base aggregated event
        LineProcessor.aggs.setdefault(self.uid, []).append({
            'ts': self.ts_millis,
            'ts_s': [ self.ts_millis ],
            'md5': self.md5,

            'datestamps': [ self.datestamp ],
            'loglevel': self.loglevel,
            'slowlog_type': self.slowlog_type,
            'index': self.index,
            'tooks': [ self.took ],
            'tooks_millis': [ self.took_millis ],
            'types': self.types,
            'stats': self.stats,
            'search_type': self.search_type,
            'total_shards': self.total_shards,

            'query': self.query,
            'extra_source': self.extra_source,
            # 'shards_n': 1,
            'tooks_min': self.took_millis,
            'tooks_max': self.took_millis,
            'ts_min': self.ts_millis,
            'ts_max': self.ts_millis
            })


    def append_agg(self, i):
        # TODO: check if shard number is already in agg?
        # (currently does the check in read_logs() )
        # move all logic into append
        LineProcessor.aggs[self.uid][i]['ts_s'].append(self.ts_millis)
        LineProcessor.aggs[self.uid][i]['datestamps'].append(self.datestamp)

        # LineProcessor.aggs[self.uid][i]['shards'].append(self.shard)
        LineProcessor.aggs[self.uid][i]['tooks'].append(self.took)
        LineProcessor.aggs[self.uid][i]['tooks_millis'].append(self.took_millis)
        # LineProcessor.aggs[self.uid][i]['shards_n'] = len(LineProcessor.aggs[self.uid][i]['shards'])
        LineProcessor.aggs[self.uid][i]['tooks_min'] = min(LineProcessor.aggs[self.uid][i]['tooks_millis'])
        LineProcessor.aggs[self.uid][i]['tooks_max'] = max(LineProcessor.aggs[self.uid][i]['tooks_millis'])
        LineProcessor.aggs[self.uid][i]['ts_min'] = min(LineProcessor.aggs[self.uid][i]['ts_s'])
        LineProcessor.aggs[self.uid][i]['ts_max'] = max(LineProcessor.aggs[self.uid][i]['ts_s'])



    def __del__(self):
        pass


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
                            # if l.shard in x['shards']:
                                # continue
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


def printer(data):

    data.results.sort(key=lambda x: int(x['ts_min']), reverse=True)
    for i in data.results:
    #     print(json.dumps(i))
        # truncated_query = i['query'][0:200]+'..'+str(len(i['query']))
        #truncated_query = i['query'] if (len(part1)+len(i['query'])) <= columns else i['query'][0:(columns-len(part1)-3)]+'..'
        # part2 = i['query'] + i['extra_source']
        # part2 = i['tooks']

        # x = i['query'].split('highlight')[0]
        # if '*' in x:
        if int(i['tooks_max']) > 9000:
        # if any(x in i['query'] for x in ('wildcard', 'regexp')):
            print(
                i['md5'][-5:],
                i['slowlog_type'],
                # i['tooks_max'],
                # datetime.utcfromtimestamp(float(i['ts_min']/1000)).strftime("%H:%M:%S.%f")[:-3],
                # (i['extra_source']+str(i['tooks'])),
                # x
                str(i['tooks']),
                i['query']
                # truncated_query
            )
            print()
    logger.info(data.metadata)

    # columns, rows = os.get_terminal_size(0)
    # for key, value in LineProcessor.aggs.items():
    #     for i in value:
    #         if i['slowlog_type'] == 'query':
    #             LineProcessor.stats['query'] += 1
    #         else:
    #             LineProcessor.stats['fetch'] += 1
    #         span = (max(i['ts_s']) - min(i['ts_s']))/1000
    #         time = "%s   %s" % (min(i['datestamps'])[11:], max(i['datestamps'])[11:])
    #         i['shards'] = [int(x) for x in i['shards']]
    #         i['shards'].sort()
    #
    #         print_items = (
    #             i['md5'][-5:],
    #             i['slowlog_type'],
    #             i['shards_n'],
    #             span,
    #             time
    #         )
    #         # print_line(print_items)
    #         part1 = '{:6s} {:6s} {:2} {} {:>7.2f} {:>30}'.format(
    #             i['md5'][-5:],
    #             i['slowlog_type'],
    #             i['tooks_max'],
    #             i['shards_n'],
    #             span,
    #             time
    #         )
    #         # part2 = i['query'] if (len(part1)+len(i['query'])) <= columns else i['query'][0:(columns-len(part1)-3)]+'..'
    #         part2 = i['query'] + i['extra_source']
    #         # part2 = i['tooks']
    #         logger.info('{} {}'.format(part1, part2))
    # logger.debug(LineProcessor.stats)

def Main():
    logger.info(args.paths)
    filenames = [logfile for path in args.paths
        for logfile in glob(path, recursive=True) if not os.path.isdir(logfile)]

    logger.debug(filenames)

    R = namedtuple('R', 'failures results metadata')

    r = R(
        failures=LineProcessor.errors,
        results=read_logs(filenames),
        metadata=stats + LineProcessor.stats
    )

    printer(r)


if __name__ == '__main__':
    Main()
