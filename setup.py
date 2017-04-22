#!/usr/bin/env python3
from setuptools import setup

from datadb import __version__

setup(name='datadb',
      version=__version__,
      description='datadb cli module',
      url='http://gitlab.xmopx.net/dave/datadb-cli',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['datadb'],
      scripts=['bin/datadb'])
