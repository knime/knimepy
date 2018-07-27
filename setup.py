#!/usr/bin/env python

import sys
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.version_info < (3, 6):
    raise NotImplementedError("Sorry, you need at least Python 3.6+ to use this.")

import knime

setup(name='knime',
      version=knime.__version__,
      description='Fast and simple WSGI-framework for small web-applications.',
      long_description=knime.__doc__,
      author=knime.__author__,
      author_email='davin+knimepy@appliomics.com',
      url='https://www.knime.com/',
      py_modules=['knime'],
      #scripts=['knime.py'],
      license='???',
      platforms='any',
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Developers',
                   'Intended Audience :: End Users/Desktop',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: Python :: 3.7',
                   ],
)
