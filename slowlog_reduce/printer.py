# -*- coding: utf-8 -*-

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PrettyTable():
    """ Helper class to print pretty tables with proper alignment


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
        if ( '*' in x
                or any(x in i['query'] for x in ('wildcard', 'regexp')
                or int(i['tooks_max']) > 30000) ):
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
    logger.info(data.metadata)



# def printer():
#     columns, rows = os.get_terminal_size(0)
#     for key, value in LineProcessor.aggs.items():
#         for i in value:
#             if i['slowlog_type'] == 'query':
#                 LineProcessor.stats['query'] += 1
#             else:
#                 LineProcessor.stats['fetch'] += 1
#             span = (max(i['ts_s']) - min(i['ts_s']))/1000
#             time = "%s   %s" % (min(i['datestamps'])[11:], max(i['datestamps'])[11:])
#             i['shards'] = [int(x) for x in i['shards']]
#             i['shards'].sort()
#
#             print_items = (
#                 i['md5'][-5:],
#                 i['slowlog_type'],
#                 i['shards_n'],
#                 span,
#                 time
#             )
#             # print_line(print_items)
#             part1 = '{:6s} {:6s} {:2} {} {:>7.2f} {:>30}'.format(
#                 i['md5'][-5:],
#                 i['slowlog_type'],
#                 i['tooks_max'],
#                 i['shards_n'],
#                 span,
#                 time
#             )
#             # part2 = i['query'] if (len(part1)+len(i['query'])) <= columns else i['query'][0:(columns-len(part1)-3)]+'..'
#             part2 = i['query'] + i['extra_source']
#             # part2 = i['tooks']
#             logger.info('{} {}'.format(part1, part2))
#     logger.debug(LineProcessor.stats)
