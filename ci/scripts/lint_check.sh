#!/bin/bash

set -e

black --check /pantab 
isort /pantab/**/*.py -c
mypy /pantab

