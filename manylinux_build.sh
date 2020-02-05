#!/bin/sh -l

/opt/python/cp${PYVER}-cp${PYVER}m/bin/pip install --upgrade pip
/opt/python/cp${PYVER}-cp${PYVER}m/bin/pip install --upgrade setuptools
/opt/python/cp${PYVER}-cp${PYVER}m/bin/pip install wheel pandas tableauhyperapi

cd /io
/opt/python/cp${PYVER}-cp${PYVER}m/bin/python setup.py bdist_wheel
for whl in dist/*.whl; do
    /opt/python/cp${PYVER}-cp${PYVER}m/bin/python -m auditwheel repair "$whl" --plat $PLAT -w /io/wheelhouse/
done



