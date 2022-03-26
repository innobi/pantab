from distutils.version import LooseVersion

import pandas as pd
from tableauhyperapi import __version__ as hyperapi_version

PANDAS_120 = LooseVersion(pd.__version__) >= LooseVersion("1.2.0")
PANDAS_130 = LooseVersion(pd.__version__) >= LooseVersion("1.3.0")

HYPER_0_0_14567 = LooseVersion(hyperapi_version) >= LooseVersion("0.0.14567")

__all__ = ["PANDAS_100", "PANDAS_120"]
