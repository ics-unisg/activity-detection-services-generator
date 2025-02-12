pylint cepact --disable=too-few-public-methods
mypy cepact --strict
# Test and coverage
export PYTHONPATH=.
coverage run --source=cepact --branch -m pytest
coverage report -m
