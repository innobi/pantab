#!/bin/bash

set -e

black /pantab
mypy /pantab
clang-format-11 /pantab/pantab/src/* -i --style=LLVM

