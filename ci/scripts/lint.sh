#!/bin/bash

set -e

black /pantab
isort /pantab
mypy /pantab

