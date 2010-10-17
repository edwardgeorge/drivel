import sys
from setuptools import setup, find_packages

requires=['eventlet', 'WebOb', 'python-daemon']
if sys.version_info < (2, 6):
    requires.append('simplejson')

setup(
    name='drivel',
    version='0.1',
    packages=['drivel'],  # find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=requires,
    test_requires=['nose', 'mock', 'WebTest'],
)
