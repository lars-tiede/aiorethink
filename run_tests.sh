#!/bin/bash

if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "error: no virtualenv is active. activate one first."
    exit 1
fi

arg="tests/"
if [[ -n "$@" ]]; then
    arg="$@"
fi

py.test -x --cov=aiorethink --cov-report html --cov-report term --durations=10 --ignore=tests/a_unit/test_db.py --ignore=tests/a_unit/test_document.py --ignore=tests/a_unit/test_document_db.py --ignore=tests/a_unit/test_fields_base.py --ignore=tests/a_unit/test_fields_simple.py --ignore=tests/a_unit/test_fields_lazy.py --ignore=tests/a_unit/test_registry.py --ignore=tests/a_unit/test_validatable.py $arg
