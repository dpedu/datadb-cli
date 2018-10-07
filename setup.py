#!/usr/bin/env python3
from setuptools import setup
import os
from datadb import __version__

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
    requirements = f.read().splitlines()


setup(name='datadb',
      version=__version__,
      description='datadb cli module',
      url='http://gitlab.xmopx.net/dave/datadb-cli',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['datadb'],
      scripts=['bin/datadb'],
      install_requires=requirements)
