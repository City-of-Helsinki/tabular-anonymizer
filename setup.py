from setuptools import setup

setup(
   name='tabular_anonymizer',
   version='1.0',
   description='Utility for anonymizing tabular data',
   author='DataHel',
   author_email='foomail@foo.com',
   packages=[
    'tabular_anonymizer'
   ],
   install_requires=[
      'numpy==1.21.4',
      'pandas==1.3.4',
      'python-dateutil==2.8.2',
      'pytz==2021.3',
      'six==1.16.0',
      'anonypy~=0.1.7',
   ],
)