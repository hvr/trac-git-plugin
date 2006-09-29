from setuptools import setup

setup(
    name='TracGit',
    description='GIT version control plugin for Trac',
    author='Herbert Valerio Riedel',
    author_email='hvr@gnu.org',
    
    keywords='trac scm plugin git',
    url='http://trac-hacks.org/wiki/GitPlugin',
    version='0.0.1',
    license="GPL",
    long_description="""
    This Trac 0.10+ plugin provides support for the GIT SCM.
    """,
    zip_safe=True,
    packages=['gitplugin'],
    entry_points = {'trac.plugins':
                    ['git = gitplugin.git_fs'],
                    },
    data_files=['COPYING','README'],
    install_requires=[],
    )
