#!/bin/sh -l

/opt/python/${PYVER}/bin/pip install --upgrade pip
/opt/python/${PYVER}/bin/pip install install setuptools wheel pandas tableauhyperapi
/opt/python/${PYVER}/bin/pip wheel /io/ -w wheelhouse/
