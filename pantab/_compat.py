import pandas as pd
from pandas.util.version import parse

PANDAS_120 = parse(pd.__version__) >= parse("1.2.0")
PANDAS_130 = parse(pd.__version__) >= parse("1.3.0")

__all__ = ["PANDAS_120", "PANDAS_130"]
