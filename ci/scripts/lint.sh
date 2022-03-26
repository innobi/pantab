#!/bin/bash

set -e

black /pantab
isort /pantab/**/*.py
mypy /pantab
clang-format-11 /pantab/pantab/src/* -i --style=LLVM

