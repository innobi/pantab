#!/bin/bash

set -e

black --check /pantab 
mypy /pantab
clang-format -i --style=LLVM --dry-run -Werror /pantab/pantab/src/*
