# -*- coding: utf-8 -*-

import os
import json
import hashlib
import gzip
from glob import glob

import logging
logger = logging.getLogger(__name__)

MAX_CACHE_FILES = 3

def read(enabled,files):
    """
    Read cache / processed slowlog from disk
    cache key is a md5 of dict>json of filenames: modified time
    """
    file_mods = dict()
    # for file in sorted(filenames):
    for file in files:
        file_mods[file] = os.stat(file).st_mtime

    cache_md5 = hashlib.md5(
                    json.dumps(file_mods, sort_keys=True).encode('utf-8')
                ).hexdigest()

    logger.debug('md5: {}, {}'.format(
        cache_md5,
        json.dumps(file_mods, sort_keys=True)
        ))

    py_name = os.path.basename(__file__)
    cache_pattern = '.{}_{}.cache'.format(py_name, '*')
    cache_filename = '.{}_{}.cache'.format(
        py_name,
        cache_md5
        )

    if enabled and os.path.isfile(cache_filename):
        with gzip.open(cache_filename, "rb") as f:
            data = json.loads(f.read().decode("ascii"))
        return (True, cache_filename, data)
    else:
        if enabled: cleanup_cache(cache_pattern)
        return (False, cache_filename, None)


def write(filename, data):
    with gzip.open(filename, "wt") as outfile:
        json.dump(data, outfile)


def cleanup_cache(glob_pattern):
    """Keep only X number of cache files, deleting the oldest accessed cache file"""
    cache_files = sorted(glob(glob_pattern), key=os.path.getatime, reverse=True)
    old_files = cache_files[(MAX_CACHE_FILES-1):]
    delete = lambda x: os.remove(x)
    if old_files:
        [delete(x) for x in old_files]
        logger.debug('cache files deleted: {}'.format(old_files))
