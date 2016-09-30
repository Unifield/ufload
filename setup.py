#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import codecs
import os
import re
import sys

from setuptools import find_packages
from setuptools import setup


def read(*parts):
    path = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(path, encoding='utf-8') as fobj:
        return fobj.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


install_requires = [
    'pyOpenSSL',
    'ndg-httpsclient',
    'pyasn1',
    'easywebdav',
]

tests_require = [
    'pytest',
]

v=find_version("ufload", "__init__.py"),

setup(
    name='ufload',
    version=v[0],
    description='Unifield loader',
    url='http://www.msf.org/',
    download_url = 'https://github.com/Unifield/ufload/tarball/%s' % v[0],
    author='MSF',
    license='MIT License',
    packages=find_packages(exclude=['tests.*', 'tests']),
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
    entry_points="""
    [console_scripts]
    ufload=ufload.cli.main:main
    """,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Customer Service',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
