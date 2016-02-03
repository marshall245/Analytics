#!/usr/bin/env python

try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup


setup(name='mkbase',
      version='0.0.1',
      description='package providing base analytic functionality',
      author='marshall markham',
      author_email='jmmarkha@ncsu.edu',
      packages=['mkbase'],
     )
