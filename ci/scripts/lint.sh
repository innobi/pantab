#!/bin/bash

set -e

black /pantab
isort /pantab/**/*.py
mypy /pantab

