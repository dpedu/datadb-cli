#!/usr/bin/env python3
from setuptools import setup

__version__ = "0.0.0"

setup(name='datadb',
    version=__version__,
    description='datadb cli module',
    url='http://gitlab.xmopx.net/dave/datadb-cli',
    author='dpedu',
    author_email='dave@davepedu.com',
    packages=['datadb'],
    scripts=['bin/datadb']
    )
