#!/usr/bin/env python

import sys
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.version_info < (3, 6):
    raise NotImplementedError("Sorry, you need at least Python 3.6+ to use this.")

import knime


def test_discovery():
    import unittest
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("tests", pattern="test_*.py")
    return test_suite


if __name__ == "__main__":
    setup(
        name="knime",
        version=knime.__version__,
        description="Tools for reading and executing KNIME workflows.",
        long_description=knime.__doc__,
        author=knime.__author__,
        author_email="davin+knimepy@appliomics.com",
        url="https://www.knime.com/",
        py_modules=["knime"],
        #scripts=["knime.py"],
        test_suite="setup.test_discovery",
        license="GPLv3",
        platforms="any",
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "Intended Audience :: End Users/Desktop",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        ],
        project_urls={
            "Source": "https://github.com/KNIME/knimepy",
        },
    )
