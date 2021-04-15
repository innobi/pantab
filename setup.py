import os
import sys
from glob import glob

from setuptools import Extension, find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


# MSVC compiler has different flags; assume that's what we are using on Windows
if os.name == "nt":
    extra_compile_args = ["/WX"]
else:
    extra_compile_args = ["-Wextra", "-Werror"]


pantab_module = Extension(
    "libpantab",
    sources=list(glob("pantab/src/*.c")),
    depends=list(glob("pantab/src/*.h")),
    extra_compile_args=extra_compile_args,
)


setup(
    name="pantab",
    version="2.0.0",
    description="Converts pandas DataFrames into Tableau Hyper Extracts and back",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WillAyd/pantab",
    author="Will Ayd",
    author_email="william.ayd@icloud.com",
    license="BSD",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Office/Business",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="tableau visualization pandas dataframe",
    packages=find_packages(),
    package_data={"": ["*.h"], "pantab.tests": ["data/*"]},
    data_files=[("", ["LICENSE.txt", "README.md"])],
    python_requires=">=3.7",
    install_requires=["pandas", "tableauhyperapi"],
    extras_require={"dev": ["pytest"]},
    ext_modules=[pantab_module],
)
