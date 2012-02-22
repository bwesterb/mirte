#!/usr/bin/env python

from setuptools import setup
from get_git_version import get_git_version

setup(name='mirte',
      version=get_git_version(),
      description='Runtime module framework',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/mirte/',
      packages=['mirte'],
      zip_safe=True,
      package_dir={'mirte': 'src'},
      install_requires = ['docutils>=0.3',
                          'pyyaml>=3.00',
                          'sarah>=0.1.1'],
      entry_points = {
              'console_scripts': [
                      'mirte = mirte.main:main',
              ]
      }
      )
