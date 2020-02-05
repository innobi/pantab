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

# Clear any existing pip caches
which ${PYLOC}/bin/python
${PYLOC}/bin/python -m pip install --upgrade pip
${PYLOC}/bin/python -m pip install --upgrade setuptools wheel
${PYLOC}/bin/python -m pip install auditwheel pandas tableauhyperapi

# Hack so auditwheel can find libtableauhyperapi
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${PYLOC}/lib/python${PYVER}/site-packages/tableauhyperapi/bin

cd /io
${PYLOC}/bin/python setup.py bdist_wheel
# TODO: we probably only need to repair one wheel file
for whl in dist/pantab*.whl; do
    ${PYLOC}/bin/python -m auditwheel repair "$whl" --plat $PLAT -w /io/wheelhouse/
done

# Install packages and test
cd $HOME
${PYLOC}/bin/python -m pip install pantab --no-index -f /io/wheelhouse
${PYLOC}/bin/python -c "import pantab; pantab.test()"
