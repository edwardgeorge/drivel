from setuptools import setup, find_packages

setup(
    name='drivel',
    version='0.1',
    packages=find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=['eventlet'],
    test_requires=['nose', 'mock'],
)
