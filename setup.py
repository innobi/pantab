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

    data = urlopen(
        "http://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-windows-x86_64-release-hyperapi_release_3.0.0.9273.r6111d374.zip"
    )
    target = dll_path.parent / "tableauhyperapi.lib"
    print(f"extract lib to {target}")
    with zipfile.ZipFile(io.BytesIO(data.read())) as archive:
        target.write_bytes(
            archive.open(
                "tableauhyperapi-cxx-windows-x86_64-release-hyperapi_release_3.0.0.9273.r6111d374/lib/tableauhyperapi.lib"
            ).read()
        )

writer_module = Extension(
    "libwriter",
    sources=["pantab/pantab.c", "pantab/_writermodule.c"],
    library_dirs=[str(dll_path.parent.resolve())],
    libraries=[dll_path.stem.replace("lib", "")],
)

reader_module = Extension(
    "libreader",
    sources=["pantab/pantab.c", "pantab/_readermodule.c"],
    library_dirs=[str(dll_path.parent.resolve())],
    libraries=[dll_path.stem.replace("lib", "")],
)


setup(
    name="pantab",
    version="0.2.2",
    description="Converts pandas DataFrames into Tableau Hyper Extracts",
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="tableau visualization pandas dataframe",
    packages=find_packages(exclude=["samples", "tests"]),
    package_data={"": ["*.h"]},
    data_files=[("", ["LICENSE.txt", "README.md"])],
    python_requires=">=3.6",
    install_requires=["pandas"],
    extras_require={"dev": ["pytest"]},
    ext_modules=[writer_module, reader_module],
)
