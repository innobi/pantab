from os import path
import sys

from setuptools import Extension, find_packages, setup
from tableauhyperapi.impl.util import find_hyper_api_dll

here = path.abspath(path.dirname(__file__))
dll_path = find_hyper_api_dll()

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

if sys.platform.startswith("win32"):
    # Looks like the Tableau Python source doesn't have the needed lib file
    # so extract from C++ distributions
    import io
    import zipfile
    from urllib.request import urlopen
    data = urlopen("http://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-windows-x86_64-release-hyperapi_release_2.0.0.8953.r50e2ce3a.zip")
    target_name = str(dll_path.parent / "tableauhyperapi.lib")
    print("extract lib to {target_name}")    
    with zipfile.ZipFile(io.BytesIO(data.read())) as archive:
        archive.extract("tableauhyperapi-cxx-windows-x86_64-release-hyperapi_release_2.0.0.8953.r50e2ce3a/lib/tableauhyperapi.lib",
                        path=target_name)

writer_module = Extension(
    "libwriter",
    sources=["pantab/_writermodule.c"],
    library_dirs=[str(dll_path.parent.resolve())],
    libraries=[dll_path.stem.replace("lib", "")],
)

setup(
    name="pantab",
    version="0.0.1.b5",
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
