#!/usr/bin/env python

from setuptools import setup

setup(
    name='TracGit',
    install_requires=[],#'Trac ==0.12',
    description='GIT version control plugin for Trac 0.12',
    author='Herbert Valerio Riedel',
    author_email='hvr@gnu.org',
    keywords='trac scm plugin git',
    url="http://trac-hacks.org/wiki/GitPlugin",
    version='0.12.0.2',
    license="GPL",
    long_description="""
    This Trac 0.12 plugin provides support for the GIT SCM.

    See http://trac-hacks.org/wiki/GitPlugin for more details.
    """,
    packages=['tracext', 'tracext.git'],
    namespace_packages=['tracext'],
    entry_points = {'trac.plugins': 'git = tracext.git.git_fs'},
    data_files=['COPYING','README'])
