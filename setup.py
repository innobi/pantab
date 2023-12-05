import os
import sys
from glob import glob

import numpy as np
from setuptools import Extension, find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


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
    packages=find_packages(),
    ext_modules=[pantab_module],
    package_data={"": ["*.h"], "pantab.tests": ["data/*"]},
    data_files=[("", ["LICENSE.txt", "README.md"])],
)
