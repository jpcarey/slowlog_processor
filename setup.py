#!/usr/bin/env python


import re
from setuptools import setup


version = re.search(
    '^__version__\s*=\s*"(.*)"',
    open('slowlog_reduce/slowlog.py').read(),
    re.M
    ).group(1)


with open("README.rst", "rb") as f:
    long_descr = f.read().decode("utf-8")


setup(
    name = 'slowlog_reduce',
    packages = ['slowlog_reduce'],
    entry_points = {
        "console_scripts": ['slowlog_reduce = slowlog_reduce.slowlog:main']
        },
    version = version,
    description = '',
    long_description = long_descr,
    author = 'Jared Carey',
    author_email ='jared.carey@elastic.co',
    # license='MIT',
    python_requires='>=3.5',
    install_requires=[
        'lxml>=3.8.0',
        ],
    url = 'https://github.com/elastic/support',
    zip_safe=False
    )
