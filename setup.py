#!/usr/bin/env python

from setuptools import setup

setup(name='mirte',
      version='0.1.0a1',
      description='Runtime module framework',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/mirte/',
      packages=['mirte'],
      zip_safe=True,
      package_dir={'mirte': 'src'},
      install_requires = ['docutils>=0.3',
	      		  'pyyaml>=3.00',
			  'sarah>=0.1.0a1'],
      entry_points = {
	      'console_scripts': [
		      'mirte = mirte.main:main',
	      ]
      }
      )
