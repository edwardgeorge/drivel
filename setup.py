from setuptools import setup, find_packages

setup(
    name='drivel',
    version='0.1',
    packages=['drivel'],  # find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=['eventlet', 'WebOb', 'python-daemon'],
    test_requires=['nose', 'mock'],
)
