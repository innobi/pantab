from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pantab',
    version='0.0.1.a2',
    description='Converts pandas DataFrames into Tableau Hyper Extracts',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/WillAyd/pantab',
    author='Will Ayd',
    author_email='william.ayd@icloud.com',
    license='BSD',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Office/Business',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='tableau visualization pandas dataframe',
    packages=find_packages(exclude=['samples', 'tests']),
    python_requires='>=3.6',
    install_requires=['pandas'],
    extras_require={
        'dev': ['pytest'],
    },
)
