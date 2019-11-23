from os import path
from setuptools import find_packages, setup, Extension

import numpy as np

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

writer_module = Extension("libwriter", sources=["pantab/_writermodule.c"],
                          include_dirs=[np.get_include()])

setup(
    name="pantab",
    version="TODO: REPLACE THIS",
    description="Converts pandas DataFrames into Tableau Hyper Extracts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WillAyd/pantab",
    author="Will Ayd",
    author_email="william.ayd@icloud.com",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Office/Business",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="tableau visualization pandas dataframe",
    packages=find_packages(exclude=["samples", "tests"]),
    python_requires=">=3.6",
    install_requires=["pandas"],
    extras_require={"dev": ["pytest"]},
    ext_modules=[writer_module],
)
