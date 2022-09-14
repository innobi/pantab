import os
import sys
from glob import glob

import numpy as np
from setuptools import Extension, find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


# MSVC compiler has different flags; assume that's what we are using on Windows
if os.name == "nt":
    # Enable extra warnings except implicit cast, which throws a few
    # see https://bugzilla.mozilla.org/show_bug.cgi?id=857863 for justification
    extra_compile_args = ["/WX", "/wd4244"]
else:
    extra_compile_args = ["-Wextra", "-Werror"]
    if "--debug" in sys.argv:
        extra_compile_args.extend(["-g", "-UNDEBUG", "-O0"])


pantab_module = Extension(
    "libpantab",
    include_dirs=[np.get_include()],
    define_macros=[("NPY_NO_DEPRECATED_API", "0")],
    sources=list(glob("pantab/src/*.c")),
    depends=list(glob("pantab/src/*.h")),
    extra_compile_args=extra_compile_args,
)


setup(
    name="pantab",
    version="3.0.0",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="tableau visualization pandas dataframe",
    packages=find_packages(),
    package_data={"": ["*.h"], "pantab.tests": ["data/*"]},
    data_files=[("", ["LICENSE.txt", "README.md"])],
    python_requires=">=3.8",
    install_requires=["pandas>=1.0.0", "tableauhyperapi>=0.0.14567", "numpy"],
    extras_require={"dev": ["pytest"]},
    ext_modules=[pantab_module],
)
