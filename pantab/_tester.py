import pathlib
import sys

PKG = pathlib.Path(__file__).resolve().parent


def test():
    import pytest

    sys.exit(pytest.main([str(PKG)]))
