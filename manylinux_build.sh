#!/bin/sh -l

# PYVER2 removes the period from the version
PYVER2="${PYVER//.}"

# Starting in Python38 the ABI version is no longer required
if [ "$PYVER2" = "37" ] || [ "$PYVER2" = "36" ]
then
    ABIVER="m"
else
    ABIVER=""
fi

PYLOC=/opt/python/cp${PYVER2}-cp${PYVER2}${ABIVER}

${PYLOC}/bin/python -m pip install --upgrade pip
${PYLOC}/bin/python -m pip install --upgrade setuptools
${PYLOC}/bin/python -m pip install wheel pandas tableauhyperapi pytest

# Hack so auditwheel can find libtableauhyperapi
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${PYLOC}/lib/python${PYVER}/site-packages/tableauhyperapi/bin

cd /io
${PYLOC}/bin/python setup.py bdist_wheel
# TODO: we probably only need to repair one wheel file
for whl in dist/pantab*.whl; do
    ${PYLOC}/bin/python -m auditwheel repair "$whl" --plat $PLAT -w /io/wheelhouse/
done

# Run the tests using the installed wheel
${PYLOC}/bin/python -m pip install pantab --no-index -f /io/wheelhouse
${PYLOC}/bin/python -m pytest
