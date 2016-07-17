#!/usr/bin/env python

import os
import os.path

from setuptools import setup

base_path = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(base_path, 'README.rst')) as f, \
     open(os.path.join(base_path, 'CHANGES.rst')) as g:
    long_description = '{0}\n{1}'.format(f.read(), g.read())


setup(name='mirte',
      version='0.1.7',
      description='Runtime module framework',
      long_description=long_description,
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/mirte/',
      packages=['mirte'],
      zip_safe=True,
      package_dir={'mirte': 'src'},
      install_requires = ['docutils>=0.3',
                          'pyyaml>=3.00',
                          'msgpack-python>=0.1.0',
                          'sarah>=0.1.2'],
      entry_points = {
              'console_scripts': [
                      'mirte = mirte.main:main',
              ]
      }
      )

# vim: et:sta:bs=2:sw=4:
