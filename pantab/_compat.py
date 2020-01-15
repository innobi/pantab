from distutils.version import LooseVersion

import pandas as pd

PANDAS_100 = LooseVersion(pd.__version__) >= LooseVersion("1.0.0")

__all__ = ["PANDAS_100"]
