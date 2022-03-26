#!/bin/bash

set -e

black --check /pantab 
isort /pantab/**/*.py -c
mypy /pantab
clang-format-11 -i --style=LLVM --dry-run -Werror /pantab/pantab/src/*
