from distutils.version import LooseVersion

import pandas as pd

PANDAS_100 = LooseVersion(pd.__version__) >= LooseVersion("1.0.0")
PANDAS_120 = LooseVersion(pd.__version__) >= LooseVersion("1.2.0")
PANDAS_130 = LooseVersion(pd.__version__) >= LooseVersion("1.3.0")

__all__ = ["PANDAS_100", "PANDAS_120"]
