# -*- coding: utf-8 -*-

import re
import hashlib
import pytz
import logging
from datetime import datetime
from collections import Counter

logger = logging.getLogger(__name__)

class LineProcessor():
    """
    Parse single log line and collate together
    """
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
        """
        Parse slowlog line
            md5 hash query body
            create unique id (uid)
            creates usable timestamp
        """
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
                grokked['ts_millis'] = grokked['ts_millis'] + LineProcessor.offsets[grokked['host']]
                LineProcessor.stats['offsets'] += 1
            LineProcessor.stats['grok_success'] += 1
            # expand named capture
            for name, value in grokked.items():
                setattr(self, name, value)

            self.collate_line()

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


    def collate_line(self):
        """
        Compare each line to try to collate matching queries together
        """
        if self.uid in LineProcessor.aggs:
            nearest = dict()
            for i, x in enumerate(LineProcessor.aggs[self.uid]):
                # less than two items = +- 10s, more than two items use the max took time * 2
                threshold = 10000 if len(x['tooks_max']) <= 2 else int(x['tooks_max'])*2
                if self.ts_millis in range(x['ts_min']-threshold, x['ts_max']+threshold):
                    if self.shard in x['shards']:
                        continue
                    # # loop through array and build nearest matching agg
                    nearest[i] = min(x['ts_s'], key=lambda x: abs(x - self.ts_millis))
            # shard is already in agg item, create new
            if not nearest:
                self.create_agg()
            else:
                match = min(nearest, key=lambda x: abs(x - self.ts_millis))
                # check if match is in time window
                self.append_agg(match)
        else:
            self.create_agg()


    def create_agg(self):
        """
        Create new aggregated slowlog item
        """
        LineProcessor.aggs.setdefault(self.uid,[]).append({
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
        """
        Add current log line to existing aggregated slowlog item
        """
        item = LineProcessor.aggs[self.uid][i]

        item['ts_s'].append(self.ts_millis)
        item['datestamps'].append(self.datestamp)
        # item['hosts'].append(self.host)
        item['shards'].append(self.shard)
        item['tooks'].append(self.took)
        item['tooks_millis'].append(self.took_millis)
        item['shards_n'] = len(item['shards'])
        item['tooks_min'] = min(item['tooks_millis'])
        item['tooks_max'] = max(item['tooks_millis'])
        item['ts_min'] = min(item['ts_s'])
        item['ts_max'] = max(item['ts_s'])


    @staticmethod
    def time_align(file):
        """
        calculates timeoffsets from master node, sourced from nodes_stats diag
        """
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
        """
        Remove each line instance when finished
        """
        # TODO: check if this is necessary. Trying to keep memory footprint down.
        pass
