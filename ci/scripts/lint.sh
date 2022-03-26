set -e

black --check /pantab
isort /pantab -c
mypy /pantab
