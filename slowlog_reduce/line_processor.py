# -*- coding: utf-8 -*-

import re
import hashlib
import pytz
import logging
from datetime import datetime
from collections import Counter

logger = logging.getLogger(__name__)

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
            # =< elasticsearch 2.3
            # regex = re.compile(
            #     r'^\[(?P<datestamp>.*?)\]'
            #     r'\[(?P<loglevel>.*?)\]'
            #     r'\[index.search.slowlog.(?P<slowlog_type>.*?)\]\s?'
            #     r'\[(?P<index>.*?)\]'
            #     r'took\[(?P<took>.*?)\],\s?'
            #     r'took_millis\[(?P<took_millis>\d+)\],\s?'
            #     r'types\[(?P<types>.*?)\],\s?'
            #     r'stats\[(?P<stats>.*?)\], '
            #     r'search_type\[(?P<search_type>.*?)\],\s?'
            #     r'total_shards\[(?P<total_shards>\d+)\],\s?'
            #     r'source\[(?P<query>.*?)\],\s?'
            #     r'extra_source\[(?P<extra_source>.*?)\],?'
            #     )
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

            if LineProcessor.offsets:
                grokked['ts_millis'] = grokked['ts_millis'] #+ LineProcessor.offsets[grokked['host']]
                LineProcessor.stats['offsets'] += 1
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


    # def filter(self, filter_ids):
    #     # called as log lines are iterated.
    #     try:
    #         # filter for query md5 values
    #         if self.md5 not in (filter_ids):
    #             stats.filtered += 1
    #             raise ValueError('message did not match filter')
    #     except NameError:
    #         pass


    def create_agg(self):
        # creates the base aggregated event
        LineProcessor.aggs.setdefault(self.uid, []).append({
            'ts': self.ts_millis,
            'ts_s': [ self.ts_millis ],
            'md5': self.md5,
            'datestamps': [ self.datestamp ],
            'loglevel': self.loglevel,
            'slowlog_type': self.slowlog_type,
            # 'hosts': [ self.host ],
            'index': self.index,
            'shards': [ self.shard ],
            'tooks': [ self.took ],
            'tooks_millis': [ self.took_millis ],
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
            })


    def append_agg(self, i):
        # TODO: check if shard number is already in agg?
        # (currently does the check in read_logs() )
        # move all logic into append
        LineProcessor.aggs[self.uid][i]['ts_s'].append(self.ts_millis)
        LineProcessor.aggs[self.uid][i]['datestamps'].append(self.datestamp)
        # LineProcessor.aggs[self.uid][i]['hosts'].append(self.host)
        LineProcessor.aggs[self.uid][i]['shards'].append(self.shard)
        LineProcessor.aggs[self.uid][i]['tooks'].append(self.took)
        LineProcessor.aggs[self.uid][i]['tooks_millis'].append(self.took_millis)
        LineProcessor.aggs[self.uid][i]['shards_n'] = len(LineProcessor.aggs[self.uid][i]['shards'])
        LineProcessor.aggs[self.uid][i]['tooks_min'] = min(LineProcessor.aggs[self.uid][i]['tooks_millis'])
        LineProcessor.aggs[self.uid][i]['tooks_max'] = max(LineProcessor.aggs[self.uid][i]['tooks_millis'])
        LineProcessor.aggs[self.uid][i]['ts_min'] = min(LineProcessor.aggs[self.uid][i]['ts_s'])
        LineProcessor.aggs[self.uid][i]['ts_max'] = max(LineProcessor.aggs[self.uid][i]['ts_s'])
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
            LineProcessor.offsets = diffs
            # master node: nodes[].attributes.master = true
            # "nodes[].timestamp": 1487258197396,
            # "nodes[].os.timestamp": 1487258197396,
            # "nodes[].process.timestamp": 1487258197396,
            # "nodes[].jvm.timestamp": 1487258197397,
            # "nodes[].fs.timestamp": 1487258197397,

    # By adding caching, decided that all filters should be post processing.
    # def parse_date(s, fmts):
    #     # DATE_FORMATS = ['%m/%d/%Y %I:%M:%S %p', '%Y/%m/%d %H:%M:%S', '%d/%m/%Y %H:%M', '%m/%d/%Y', '%Y/%m/%d']
    #     # skeleton code for now, need to decided how I want to filter dates
    #     test_date = '2012/1/1 12:32:11'
    #     for f in fmts:
    #         try:
    #             return datetime.strptime(s, f)
    #         except ValueError:
    #             pass


    def __del__(self):
        pass
