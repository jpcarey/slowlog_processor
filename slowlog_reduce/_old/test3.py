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
import fileinput
from pprint import pprint

parser = argparse.ArgumentParser(
    description='script to check the number of fields in elasticsearch'
)
parser.add_argument(
    '-p', '--paths',
    nargs='*',
    default=[
        '/Users/jared/tmp/file*.txt'
    ],
    help='paths / glob pattern to get slowlogs',
    required=False
)
args = parser.parse_args()

# class FileLineWrapper(object):
#     def __init__(self, f):
#         self.f = f
#         self.line = 0
#     def close(self):
#         return self.f.close()
#     def readline(self):
#         self.line += 1
#         return self.f.readline()
#     # to allow using in 'with' statements
#     def __enter__(self):
#         return self
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.close()


filenames = [logfile for path in args.paths for logfile in glob.glob(path, recursive=True) if not os.path.isdir(logfile)]
# print(filenames)
with ExitStack() as stack:
    handles = [stack.enter_context(fileinput.input(files=file)) for file in filenames]
    # print(handles)
    for i in handles:
        pass
        # pprint(i.__dict__)

    # for line in (line for tuple in zip_longest(*handles) for line in tuple if line):
    for f,line in ((f,line) for lines in zip_longest(*handles) for f,line in enumerate(lines) if line):
        print(handles[f].filelineno(), handles[f].filename())
        print(line.strip())
    # for lines in zip_longest(*handles):
    #     # print(handles[0].filelineno(), handles[0].filename())
    #     # for line in lines:
    #     for f,line in enumerate(lines):
    #         if line:
    #             print(line.strip())
